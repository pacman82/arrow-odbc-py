use arrow_odbc::{
    OdbcWriter, QuoteDefensively, QuoteOffensively,
    WriterError::{self},
    arrow::{array::RecordBatch, datatypes::Schema},
    insert_statement_from_schema,
    odbc_api::{ConnectionTransitions, SharedConnection, handles::StatementConnection},
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
        let quote_char = connection
            .lock()
            .unwrap()
            .identifier_quote_char()
            .map_err(WriterError::QueryQuotingCharacter)?;
        let sql = if let Some(quote_char) = quote_char {
            insert_statement_from_schema(schema, table_name, &QuoteOffensively::new(quote_char))
        } else {
            insert_statement_from_schema(schema, table_name, &QuoteDefensively)
        };

        let statement = connection
            .into_prepared(&sql)
            .map_err(|source| WriterError::PreparingInsertStatement { source, sql })?;
        let odbc_writer = OdbcWriter::new(row_capacity, schema, statement)?;
        Ok(ArrowOdbcWriter(odbc_writer))
    }

    pub fn write_batch(&mut self, record_batch: &RecordBatch) -> Result<(), WriterError> {
        self.0.write_batch(record_batch)
    }

    pub fn flush(&mut self) -> Result<(), WriterError> {
        self.0.flush()
    }
}
