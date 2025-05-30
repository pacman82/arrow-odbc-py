use std::mem::swap;

use crate::error::ArrowOdbcError;
use arrow::{
    array::{Array, StructArray},
    datatypes::Schema,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatchReader,
};
use arrow_odbc::{
    ConcurrentOdbcReader, OdbcReader, OdbcReaderBuilder, arrow_schema_from,
    odbc_api::{Connection, Cursor, CursorImpl, ParameterCollectionRef, StatementConnection},
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
    /// Either a freshly created instance, or all our resources have been moved to another instance.
    Empty,
    /// We can not read batches in cursor state yet. We still would need to figure out the schema
    /// of the source data and (usually) infer the arrow schema from it. So in earlier versions
    /// we created everything directly in `Reader` state. However, if we want the user to be able
    /// to create a custom schema which is based on the source schema, then we need this
    /// intermediate step. Since the cursor state is not able to perform the most import operations
    /// like e.g. read batch we do not want our end users directly interacting with this state, yet
    /// the Python layer above should be able to make use of this in order to implement the schema
    /// mapping functionality.
    Cursor {
        /// Required to account for Database specific behavor then determining the arrow schema.
        dbms_name: String,
        cursor: CursorImpl<StatementConnection<'static>>,
    },
    Reader {
        /// We want to support state transitions from `Reader` back to `Cursor` so we keep the name
        /// around. Another way to look at this, is that we want to determine a new schema again in
        /// case we process multiple result sets.
        dbms_name: String,
        reader: OdbcReader<CursorImpl<StatementConnection<'static>>>,
    },
    ConcurrentReader {
        /// We want to support state transitions from `Reader` back to `Cursor` so we keep the name
        /// around. Another way to look at this, is that we want to determine a new schema again in
        /// case we process multiple result sets.
        dbms_name: String,
        reader: ConcurrentOdbcReader<CursorImpl<StatementConnection<'static>>>,
    },
}

impl ArrowOdbcReader {
    pub fn empty() -> Self {
        Self::Empty
    }

    pub fn next_batch(
        &mut self,
    ) -> Result<Option<(FFI_ArrowArray, FFI_ArrowSchema)>, ArrowOdbcError> {
        let next = match self {
            ArrowOdbcReader::Empty => None,
            ArrowOdbcReader::Cursor { .. } => {
                unreachable!("Python code must not allow to call next_batch from cursor state")
            }
            ArrowOdbcReader::Reader {
                reader,
                dbms_name: _,
            } => reader.next().transpose()?,
            ArrowOdbcReader::ConcurrentReader {
                reader,
                dbms_name: _,
            } => reader.next().transpose()?,
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
    pub fn promote_to_reader(
        &mut self,
        mut builder: OdbcReaderBuilder,
    ) -> Result<(), ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different state.
        let mut tmp_self = ArrowOdbcReader::Empty;
        swap(self, &mut tmp_self);
        let (cursor, dbms_name) = match tmp_self {
            // In case there has been a query without a result set, we could be in an empty state.
            // Let's just keep it, there is simply nothing to bind a buffer to.
            ArrowOdbcReader::Empty => return Ok(()),
            ArrowOdbcReader::Cursor { cursor, dbms_name } => (cursor, dbms_name),
            ArrowOdbcReader::Reader { .. } | ArrowOdbcReader::ConcurrentReader { .. } => {
                unreachable!("Python part must ensure to only promote cursors to readers.")
            }
        };
        // There is another result set. Let us create a new reader
        let reader = builder
            // This clone would not be necessary if builder would not need to take ownership of the
            // name.
            .with_dbms_name(dbms_name.clone())
            .build(cursor)?;
        *self = ArrowOdbcReader::Reader { reader, dbms_name };
        Ok(())
    }

    /// Promote Connection to cursor state. If this operation fails, the reader will stay in
    /// connection state.
    pub fn promote_to_cursor(
        &mut self,
        conn: Connection<'static>,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<(), ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different state.
        let mut tmp_self = ArrowOdbcReader::Empty;
        swap(self, &mut tmp_self);

        let dbms_name = conn.database_management_system_name()?;

        match conn.into_cursor(query, params, query_timeout_sec) {
            Ok(None) => (),
            Ok(Some(cursor)) => {
                *self = ArrowOdbcReader::Cursor { cursor, dbms_name };
            }
            Err(error) => {
                return Err(error.error.into());
            }
        }
        Ok(())
    }

    /// After this method call we will be in the `Cursor` state or `NoMoreResultSets`, in case we
    /// already consumed the last result set. In this case this method returns `false`.
    pub fn more_results(&mut self) -> Result<bool, ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different typestate.
        let mut tmp_self = ArrowOdbcReader::Empty;
        swap(self, &mut tmp_self);
        let (cursor, dbms_name) = match tmp_self {
            ArrowOdbcReader::Empty => return Ok(false),
            ArrowOdbcReader::Cursor { cursor, dbms_name } => (cursor, dbms_name),
            ArrowOdbcReader::Reader { reader, dbms_name } => (reader.into_cursor()?, dbms_name),
            ArrowOdbcReader::ConcurrentReader { reader, dbms_name } => {
                (reader.into_cursor()?, dbms_name)
            }
        };
        // We need to call ODBCs `more_results` in order to get the next one.
        if let Some(cursor) = cursor.more_results()? {
            *self = ArrowOdbcReader::Cursor { cursor, dbms_name };
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn schema(&mut self) -> Result<FFI_ArrowSchema, ArrowOdbcError> {
        let schema_ffi = match self {
            ArrowOdbcReader::Empty => {
                // A schema with no columns. Different from FFI_ArrowSchema empty, which can not be
                // imported into pyarrow
                let schema = Schema::empty();
                schema.try_into()?
            }
            ArrowOdbcReader::Cursor { cursor, dbms_name } => {
                let schema = arrow_schema_from(cursor, Some(&dbms_name), false)?;
                schema.try_into()?
            }
            ArrowOdbcReader::Reader {
                reader,
                dbms_name: _,
            } => {
                let schema_ref = reader.schema();
                let schema = &*schema_ref;
                schema.try_into()?
            }
            // This is actually dead code. Python part caches schema information as a member of the
            // reader. Every state change that would change it is performed on a sequential reader.
            // Yet the operation can be defined nicely, so we will do it despite this being
            // unreachable for now.
            ArrowOdbcReader::ConcurrentReader {
                reader,
                dbms_name: _,
            } => {
                let schema_ref = reader.schema();
                let schema = &*schema_ref;
                schema.try_into()?
            }
        };
        Ok(schema_ffi)
    }

    pub fn into_concurrent(&mut self) -> Result<(), ArrowOdbcError> {
        // Move self into a temporary instance we own, in order to take ownership of the inner
        // reader and move it to a different typestate.
        let mut tmp_self = ArrowOdbcReader::Empty;
        swap(self, &mut tmp_self);

        *self = match tmp_self {
            // Nothing to do. There is nothing left to fetch.
            ArrowOdbcReader::Empty => ArrowOdbcReader::Empty,
            ArrowOdbcReader::Cursor { .. } => {
                unreachable!("Python code must not allow to call into_concurrent from cursor state")
            }
            // Nothing to do. Reader is already concurrent,
            ArrowOdbcReader::ConcurrentReader { reader, dbms_name } => {
                ArrowOdbcReader::ConcurrentReader { reader, dbms_name }
            }
            ArrowOdbcReader::Reader { reader, dbms_name } => {
                let reader = reader.into_concurrent()?;
                ArrowOdbcReader::ConcurrentReader { reader, dbms_name }
            }
        };
        Ok(())
    }
}
