use arrow::{error::ArrowError, record_batch::{RecordBatch, RecordBatchReader}, datatypes::SchemaRef};
use arrow_odbc::{
    odbc_api::{CursorImpl, StatementConnection, self},
    BufferAllocationOptions, OdbcReader,
};

/// Opaque type holding all the state associated with an ODBC reader implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowOdbcReader(OdbcReader<CursorImpl<StatementConnection<'static>>>);

impl ArrowOdbcReader {
    pub fn new(
        cursor: CursorImpl<StatementConnection<'static>>,
        batch_size: usize,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Self, arrow_odbc::Error> {
        let schema = None; // Autodiscover schema information
        let reader = OdbcReader::with(cursor, batch_size, schema, buffer_allocation_options)?;
        Ok(Self(reader))
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.0.next().transpose()
    }

    pub fn into_cursor(
        self,
    ) -> Result<CursorImpl<StatementConnection<'static>>, odbc_api::Error> {
        self.0.into_cursor()
    }

    pub fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}
