use arrow_odbc::{
    OdbcWriter, WriterError,
    arrow::{array::RecordBatch, datatypes::Schema},
    odbc_api::{SharedConnection, handles::StatementConnection},
};

/// Opaque type holding all the state associated with an ODBC writer implementation in Rust. This
/// type also has ownership of the ODBC Connection handle.
pub struct ArrowOdbcWriter(OdbcWriter<StatementConnection<SharedConnection<'static>>>);

impl ArrowOdbcWriter {
    pub fn new(
        connection: SharedConnection<'static>,
        schema: &Schema,
        table_name: &str,
        row_capacity: usize,
    ) -> Result<Self, WriterError> {
        let odbc_writer =
            OdbcWriter::from_connection(connection, schema, table_name, row_capacity)?;
        Ok(ArrowOdbcWriter(odbc_writer))
    }

    pub fn write_batch(&mut self, record_batch: &RecordBatch) -> Result<(), WriterError> {
        self.0.write_batch(record_batch)
    }

    pub fn flush(&mut self) -> Result<(), WriterError> {
        self.0.flush()
    }
}
