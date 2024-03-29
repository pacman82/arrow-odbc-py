use std::mem::swap;

use crate::error::ArrowOdbcError;
use arrow::{
    array::{Array, StructArray},
    datatypes::Schema,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatchReader,
};
use arrow_odbc::{
    arrow_schema_from,
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
/// * It allows state-transitions to happen in the Rust part, which is good, because they actually
///   can be triggered by calls to ODBC.
/// * We can keep the C-Interface lean, we just hold a handle to this instance, rather than
///   modelling a lot of different Python wrappers for various states.
/// * Since Python does not posses destructive move semantics, we would not be able to represent
///   the state transitions in the Python type system anyway.
pub enum ArrowOdbcReader {
    /// The last result set has been extracted from the cursor. There is nothing more to fetch and
    /// all associated resources have been deallocated
    NoMoreResultSets,
    /// We can not read batches in cursor state yet. We still would need to figure out the schema
    /// of the source data and (usually) infer the arrow schema from it. So in earlier versions
    /// we created everything directly in `Reader` state. However, if we want the user to be able
    /// to create a custom schema which is based on the source schema, then we need this
    /// intermediate step. Since the cursor state is not able to perform the most import operations
    /// like e.g. read batch we do not want our end users directly interacting with this state, yet
    /// the Python layer above should be able to make use of this in order to implement the schema
    /// mapping functionality.
    Cursor(CursorImpl<StatementConnection<'static>>),
    Reader(OdbcReader<CursorImpl<StatementConnection<'static>>>),
    ConcurrentReader(ConcurrentOdbcReader<CursorImpl<StatementConnection<'static>>>),
}

impl ArrowOdbcReader {
    /// Creates a new reader in Cursor state.
    pub fn new(cursor: CursorImpl<StatementConnection<'static>>) -> Self {
        Self::Cursor(cursor)
    }

    pub fn empty() -> Self {
        Self::NoMoreResultSets
    }

    pub fn next_batch(
        &mut self,
    ) -> Result<Option<(FFI_ArrowArray, FFI_ArrowSchema)>, ArrowOdbcError> {
        let next = match self {
            ArrowOdbcReader::NoMoreResultSets => None,
            ArrowOdbcReader::Cursor(_) => {
                unreachable!("Python code must not allow to call next_batch from cursor state")
            }
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

    /// Promotes `Cursor` to `Reader` state. I.e. we take the raw cursor which represents the
    /// result set, bind buffers to it so we can fetch in bulk and provide all information needed
    /// to convert the row groups into Arrow record batches.
    pub fn promote_to_reader(&mut self, builder: OdbcReaderBuilder) -> Result<(), ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different typestate.
        let mut tmp_self = ArrowOdbcReader::NoMoreResultSets;
        swap(self, &mut tmp_self);
        let cursor = match tmp_self {
            // In case there has been a query without a result set, we could be in an empty state.
            // Let's just keep it, there is simply nothing to bind a buffer to.
            ArrowOdbcReader::NoMoreResultSets => return Ok(()),
            ArrowOdbcReader::Cursor(cursor) => cursor,
            ArrowOdbcReader::Reader(_) | ArrowOdbcReader::ConcurrentReader(_) => {
                unreachable!("Python part must ensure to only promote cursors to readers.")
            }
        };
        // There is another result set. Let us create a new reader
        let reader = builder.build(cursor)?;
        *self = ArrowOdbcReader::Reader(reader);
        Ok(())
    }

    pub fn promote_to_cursor(&mut self, cursor: CursorImpl<StatementConnection<'static>>) {
        match self {
            ArrowOdbcReader::NoMoreResultSets => (),
            ArrowOdbcReader::Cursor(_)
            | ArrowOdbcReader::Reader(_)
            | ArrowOdbcReader::ConcurrentReader(_) => {
                unreachable!("Python part must ensure to only promote empty readers to cursors.")
            }
        }
        *self = ArrowOdbcReader::Cursor(cursor);
    }

    /// After this method call we will be in the `Cursor` state or `NoMoreResultSets`, in case we
    /// already consumed the last result set. In this case this method returns `false`.
    pub fn more_results(&mut self) -> Result<bool, ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different typestate.
        let mut tmp_self = ArrowOdbcReader::NoMoreResultSets;
        swap(self, &mut tmp_self);
        let cursor = match tmp_self {
            ArrowOdbcReader::NoMoreResultSets => return Ok(false),
            ArrowOdbcReader::Cursor(cursor) => cursor,
            ArrowOdbcReader::Reader(inner) => inner.into_cursor()?,
            ArrowOdbcReader::ConcurrentReader(inner) => inner.into_cursor()?,
        };
        // We need to call ODBCs `more_results` in order to get the next one.
        if let Some(cursor) = cursor.more_results()? {
            *self = ArrowOdbcReader::Cursor(cursor);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn schema(&mut self) -> Result<FFI_ArrowSchema, ArrowOdbcError> {
        let schema_ffi = match self {
            ArrowOdbcReader::NoMoreResultSets => {
                // A schema with no columns. Different from FFI_ArrowSchema empty, which can not be
                // imported into pyarrow
                let schema = Schema::empty();
                schema.try_into()?
            }
            ArrowOdbcReader::Cursor(inner) => {
                let schema = arrow_schema_from(inner)?;
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
            ArrowOdbcReader::Cursor(_) => {
                unreachable!("Python code must not allow to call into_concurrent from cursor state")
            }
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
