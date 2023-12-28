use std::mem::swap;

use crate::error::ArrowOdbcError;
use arrow::{
    array::{Array, StructArray},
    datatypes::Schema,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatchReader,
};
use arrow_odbc::{
    odbc_api::{Cursor, CursorImpl, StatementConnection},
    ConcurrentOdbcReader, OdbcReader, OdbcReaderBuilder,
};

/// Opaque type holding all the state associated with an ODBC reader implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
/// 
/// Originally this type had been intended soley as an opaque handle to the reader. However the
/// design that emerged is that this holds the statement and connection handle in various states.
/// This has several benefits:
/// 
/// * It allows statetransitions to happen in the Rust part, which is good, because they actually
///   can be triggered by calls to ODBC.
/// * We can keep the C-Interface lean, we just hold a handle to this instance, rather than
///   modelling a lot of different Python wrappers for various states.
/// * Since Python does not posses destructive move semantics, we would not be able to represent
///   the transitions in the Python part well anyway.
pub enum ArrowOdbcReader {
    /// The last result set has been extracted from the cursor. There is nothing more to fetch and
    /// all associated resources have been deallocated
    NoMoreResultSets,
    Reader(OdbcReader<CursorImpl<StatementConnection<'static>>>),
    ConcurrentReader(ConcurrentOdbcReader<CursorImpl<StatementConnection<'static>>>),
}

impl ArrowOdbcReader {
    pub fn new(
        reader: OdbcReader<CursorImpl<StatementConnection<'static>>>,
    ) -> Self {
        Self::Reader(reader)
    }

    pub fn empty() -> Self {
        Self::NoMoreResultSets
    }

    pub fn next_batch(
        &mut self,
    ) -> Result<Option<(FFI_ArrowArray, FFI_ArrowSchema)>, ArrowOdbcError> {
        let next = match self {
            ArrowOdbcReader::NoMoreResultSets => None,
            ArrowOdbcReader::Reader(reader) => reader.next().transpose()?,
            ArrowOdbcReader::ConcurrentReader(reader) => reader.next().transpose()?,
        };
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

    pub fn more_results(&mut self, builder: OdbcReaderBuilder) -> Result<bool, ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different typestate.
        let mut tmp_self = ArrowOdbcReader::NoMoreResultSets;
        swap(self, &mut tmp_self);
        let cursor = match tmp_self {
            ArrowOdbcReader::NoMoreResultSets => return Ok(false),
            ArrowOdbcReader::Reader(inner) => inner.into_cursor()?,
            ArrowOdbcReader::ConcurrentReader(inner) => inner.into_cursor()?,
        };
        if let Some(cursor) = cursor.more_results()? {
            // There is another result set. Let us create a new reader
            let reader = builder.build(cursor)?;
            *self = ArrowOdbcReader::Reader(reader);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn schema(&self) -> Result<FFI_ArrowSchema, ArrowOdbcError> {
        let schema_ffi = match self {
            ArrowOdbcReader::NoMoreResultSets => {
                // A schema with no columns. Different from FFI_ArrowSchema empty, which can not be
                // imported into pyarrow
                let schema = Schema::empty();
                schema.try_into()?
            }
            ArrowOdbcReader::Reader(inner) => {
                let schema_ref = inner.schema();
                let schema = &*schema_ref;
                schema.try_into()?
            }
            // This is actually dead code. Python part caches schema information as a member of the
            // reader. Every state change that would change it is performed on a sequential reader.
            // Yet the operation can be defined nicely, so we will do it despite this being
            // unreachable for now.
            ArrowOdbcReader::ConcurrentReader(inner) => {
                let schema_ref = inner.schema();
                let schema = &*schema_ref;
                schema.try_into()?
            }
        };
        Ok(schema_ffi)
    }

    pub fn into_concurrent(&mut self) -> Result<(), ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different typestate.
        let mut tmp_self = ArrowOdbcReader::NoMoreResultSets;
        swap(self, &mut tmp_self);

        *self = match tmp_self {
            // Nothing to do. There is nothing left to fetch.
            ArrowOdbcReader::NoMoreResultSets => ArrowOdbcReader::NoMoreResultSets,
            // Nothing to do. Reader is already concurrent,
            ArrowOdbcReader::ConcurrentReader(inner) => ArrowOdbcReader::ConcurrentReader(inner),
            ArrowOdbcReader::Reader(inner) => {
                let reader = inner.into_concurrent()?;
                ArrowOdbcReader::ConcurrentReader(reader)
            }
        };
        Ok(())
    }
}
