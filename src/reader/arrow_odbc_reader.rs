use crate::error::ArrowOdbcError;
use arrow::{
    array::{Array, StructArray},
    ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatchReader, datatypes::Schema,
};
use arrow_odbc::{
    odbc_api::{Cursor, CursorImpl, StatementConnection},
    BufferAllocationOptions, OdbcReader,
};

/// Opaque type holding all the state associated with an ODBC reader implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowOdbcReader(Option<OdbcReader<CursorImpl<StatementConnection<'static>>>>);

impl ArrowOdbcReader {
    pub fn new(
        cursor: CursorImpl<StatementConnection<'static>>,
        batch_size: usize,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Self, arrow_odbc::Error> {
        let schema = None; // Autodiscover schema information
        let reader = OdbcReader::with(cursor, batch_size, schema, buffer_allocation_options)?;
        Ok(Self(Some(reader)))
    }

    pub fn next_batch(
        &mut self,
    ) -> Result<Option<(FFI_ArrowArray, FFI_ArrowSchema)>, ArrowOdbcError> {
        let next = self.0.as_mut().and_then(OdbcReader::next).transpose()?;
        let next = if let Some(batch) = next {
            let struct_array: StructArray = batch.into();
            let arrow_array = ArrowArray::try_new(struct_array.data().clone())?;
            let array_data = arrow_array.to_data().unwrap();
            let ffi_array = FFI_ArrowArray::new(&array_data);
            let ffi_schema = FFI_ArrowSchema::try_from(array_data.data_type()).unwrap();
            Some((ffi_array, ffi_schema))
        } else {
            None
        };

        Ok(next)
    }

    pub fn more_results(
        self,
        batch_size: usize,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Option<Self>, ArrowOdbcError> {
        // None in case this reader has already moved past the last result set.
        if self.0.is_none() {
            return Ok(None);
        }
        let inner = self.0.unwrap();
        let cursor = inner.into_cursor()?;
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
        let schema_ffi = if let Some(inner) = self.0.as_ref() {
            let schema_ref = inner.schema();
            let schema = &*schema_ref;
            schema.try_into()?
        } else {
            // A schema with no columns. Different from FFI_ArrowSchema empty, which can not be
            // imported into pyarrow
            let schema = Schema::empty();
            schema.try_into()?
        };
        Ok(schema_ffi)
    }
}
