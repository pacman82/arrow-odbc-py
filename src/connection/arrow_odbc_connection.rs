use arrow_odbc::odbc_api::{
    self, Connection, ConnectionTransitions, ConnectionWithState, CursorImpl,
    ParameterCollectionRef, Shared, handles::StatementConnection, sys::SsAccessToken,
};

use crate::ArrowOdbcError;

pub type ConnectionType = Shared<ConnectionWithState<'static, Attributes>>;

/// Opaque type to transport connection to an ODBC Datasource over language boundry
pub struct ArrowOdbcConnection(ConnectionType);

impl ArrowOdbcConnection {
    pub fn new(connection: Connection<'static>) -> Self {
        let attributes = Attributes { entra_id: None };
        ArrowOdbcConnection(Shared::new(connection.with_state(attributes)))
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
