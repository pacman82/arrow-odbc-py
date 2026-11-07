use arrow_odbc::odbc_api::{
    self, ConnectionOptions, ConnectionTransitions, ConnectionWithState, CursorImpl,
    MicrosoftConnectionOptions, ParameterCollectionRef, Shared, environment,
    handles::StatementConnection, sys::SsAccessToken,
};
use log::debug;

use crate::ArrowOdbcError;

pub type ConnectionType = Shared<ConnectionWithState<'static, Attributes>>;

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct ArrowOdbcConnection(ConnectionType);

impl ArrowOdbcConnection {
    /// # Safety
    ///
    /// If `entra_id` is set, the connection must be opened using the Microsoft ODBC Driver for SQL
    /// Server.
    pub unsafe fn new(
        connection_string: &str,
        options: ConnectionOptions,
        autocommit: bool,
        entra_id: Option<&str>,
    ) -> Result<Self, odbc_api::Error> {
        let env = environment()?;
        let entra_id = entra_id.map(SsAccessToken::from_text);
        // Safety: The token is bound to the connection using `with_state` right after connecting.
        // Moving the box does not move the token itself.
        let access_token = entra_id
            .as_deref()
            // Safety: We bind the access token in Attributes via .with_state to the connection. So
            // the pointer will be valid for as long as the connection lives.
            .map(|token| unsafe { &*(token as *const SsAccessToken) });
        let microsoft_options = MicrosoftConnectionOptions { access_token };
        // Safety: The caller guarantees the Microsoft driver is used, if an access token is set.
        let connection = unsafe {
            env.connect_with_connection_string_and_driver_specific_options(
                connection_string,
                options,
                microsoft_options,
            )
        }?;
        // Binding the attributes is what makes extending the lifetime of the entra id token safe.
        let connection = connection.with_state(Attributes { _entra_id: entra_id });

        // Default is autocommit=true. We only need the function call in cas it is false.
        if !autocommit {
            connection.set_autocommit(autocommit)?;
        }

        // Log dbms name to ease debugging of issues. Since ODBC calls are expensive, only query
        // name if we are actually logging the message.
        if log::max_level() <= log::LevelFilter::Debug {
            let dbms_name = connection.database_management_system_name()?;
            debug!("Database managment system name as reported by ODBC: {dbms_name}");
        }

        Ok(ArrowOdbcConnection(Shared::new(connection)))
    }

    /// Take the inner connection out of its wrapper
    pub fn inner(&self) -> ConnectionType {
        self.0.clone()
    }

    pub fn commit(&self) -> Result<(), odbc_api::Error> {
        self.0.lock().connection().commit()
    }

    pub fn rollback(&self) -> Result<(), odbc_api::Error> {
        self.0.lock().connection().rollback()
    }

    /// Execute the SQL statement and return the cursor (if the statement produced a result set).
    /// Cursor creation is owned here so the reader does not need to know about connections.
    pub fn execute(
        &self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<CursorImpl<StatementConnection<ConnectionType>>>, ArrowOdbcError> {
        match self.0.lock().into_cursor(query, params, query_timeout_sec) {
            Ok(cursor) => Ok(cursor),
            Err(failed) => Err(failed.error.into()),
        }
    }

    pub fn database_management_system_name(&self) -> Result<String, odbc_api::Error> {
        self.0.lock().connection().database_management_system_name()
    }
}

/// Additional connection attributes passed to the ODBC driver by pointer, but owned by the
/// [`ArrowOdbcConnection`].
pub struct Attributes {
    /// Exists only to keep the access token alive. The token itself is used via a pointer bound to
    /// the ODBC connection handle.
    _entra_id: Option<Box<SsAccessToken>>,
}
