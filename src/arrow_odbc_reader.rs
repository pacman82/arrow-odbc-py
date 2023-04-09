use crate::error::ArrowOdbcError;
use arrow::{
    record_batch::{RecordBatch, RecordBatchReader},
    ffi::FFI_ArrowSchema,
};
use arrow_odbc::{
    odbc_api::{Cursor, CursorImpl, StatementConnection},
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

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowOdbcError> {
        let next = self.0.next().transpose()?;
        Ok(next)
    }

    pub fn more_results(
        self,
        batch_size: usize,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Option<Self>, ArrowOdbcError> {
        let cursor = self.0.into_cursor()?;
        let next = if let Some(cursor) = cursor.more_results()? {
            // There is another result set. Let us create a new reader
            Some(ArrowOdbcReader::new(
                cursor,
                batch_size,
                buffer_allocation_options,
            )?)
        } else {
            None
        };
        Ok(next)
    }

    pub fn schema(&self) -> Result<FFI_ArrowSchema, ArrowOdbcError> {
        let schema_ref = self.0.schema();
        let schema = &*schema_ref;
        let schema_ffi = schema.try_into()?;
        Ok(schema_ffi)
    }
}