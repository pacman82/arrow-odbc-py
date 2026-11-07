use arrow_odbc::odbc_api::{
    self, ConnectionOptions, ConnectionTransitions, ConnectionWithState, CursorImpl,
    ParameterCollectionRef, Shared, environment, handles::StatementConnection, sys::SsAccessToken,
};
use log::debug;

use crate::ArrowOdbcError;

pub type ConnectionType = Shared<ConnectionWithState<'static, Attributes>>;

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct ArrowOdbcConnection(ConnectionType);

impl ArrowOdbcConnection {
    pub fn new(
        connection_string: &str,
        options: ConnectionOptions,
        autocommit: bool,
    ) -> Result<Self, odbc_api::Error> {
        let env = environment()?;
        let connection = env.connect_with_connection_string(connection_string, options)?;

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

        let attributes = Attributes { entra_id: None };
        Ok(ArrowOdbcConnection(Shared::new(
            connection.with_state(attributes),
        )))
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
    entra_id: Option<Box<SsAccessToken>>,
}
