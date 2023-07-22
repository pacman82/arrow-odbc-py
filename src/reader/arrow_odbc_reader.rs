use crate::error::ArrowOdbcError;
use arrow::{
    array::{Array, StructArray},
    datatypes::Schema,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatchReader,
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

    pub fn empty() -> Self {
        Self(None)
    }

    pub fn next_batch(
        &mut self,
    ) -> Result<Option<(FFI_ArrowArray, FFI_ArrowSchema)>, ArrowOdbcError> {
        let next = self.0.as_mut().and_then(OdbcReader::next).transpose()?;
        let next = if let Some(batch) = next {
            let struct_array: StructArray = batch.into();
            let array_data = struct_array.to_data();
            let ffi_array = FFI_ArrowArray::new(&array_data);
            let ffi_schema = FFI_ArrowSchema::try_from(array_data.data_type()).unwrap();
            Some((ffi_array, ffi_schema))
        } else {
            None
        };

        Ok(next)
    }

    pub fn more_results(
        &mut self,
        batch_size: usize,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<bool, ArrowOdbcError> {
        // None in case this reader has already moved past the last result set.
        if self.0.is_none() {
            return Ok(false);
        }
        let inner = self.0.take().unwrap();
        let cursor = inner.into_cursor()?;
        if let Some(cursor) = cursor.more_results()? {
            // There is another result set. Let us create a new reader
            self.0 = Some(OdbcReader::with(
                cursor,
                batch_size,
                None,
                buffer_allocation_options,
            )?);
        };
        Ok(self.0.is_some())
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
