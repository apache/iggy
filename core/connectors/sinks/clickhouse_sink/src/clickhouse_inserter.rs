use std::mem;

use tokio::time::Duration;

use clickhouse::{
    Client,
    error::{Error, Result},
    insert_formatted::{self, BufInsertFormatted},
};

use crate::ClickHouseInsertFormat;

#[must_use]
pub struct ClickHouseInserter {
    client: Client,
    table: String,
    format: ClickHouseInsertFormat,
    chunk_capacity: usize,
    send_timeout: Option<Duration>,
    end_timeout: Option<Duration>,
    insert: Option<insert_formatted::BufInsertFormatted>,
    pending: Quantities,
    in_transaction: bool,
}

/// Statistics about pending or inserted data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quantities {
    pub bytes: u64,
    pub rows: u64,
    pub transactions: u64,
}

impl Quantities {
    pub const ZERO: Quantities = Quantities {
        bytes: 0,
        rows: 0,
        transactions: 0,
    };
}

impl ClickHouseInserter {
    pub(crate) fn new(
        client: &Client,
        table: &str,
        format: ClickHouseInsertFormat,
        chunk_capacity: usize,
    ) -> Self {
        Self {
            client: client.clone(),
            table: table.into(),
            format,
            chunk_capacity,
            send_timeout: None,
            end_timeout: None,
            insert: None,
            pending: Quantities::ZERO,
            in_transaction: false,
        }
    }

    /// Serializes the provided row into an internal buffer.
    /// # Panics
    /// If called after the previous call that returned an error.
    #[inline]
    pub async fn write(&mut self, row: Vec<u8>) -> Result<()> {
        if self.insert.is_none() {
            self.init_insert().await?;
        }

        let insert = self.insert.as_mut().unwrap();

        let newline = b"\n";
        let bytes_json = row.len();
        let bytes_newline = newline.len();

        if bytes_json
            .checked_add(bytes_newline)
            .and_then(|sum| sum.checked_add(insert.buf_len()))
            .is_none()
        {
            return Err(Error::Custom("Buffer overflow".to_owned()));
        }
        //JSON row
        insert.write_buffered(&row);

        //newline separator
        insert.write_buffered(newline);

        let total_bytes = bytes_json + bytes_newline;
        self.pending.bytes += total_bytes as u64;
        self.pending.rows += 1;

        if !self.in_transaction {
            self.pending.transactions += 1;
            self.in_transaction = true;
        }
        Ok(())
    }

    /// Ends the current `INSERT` unconditionally.
    pub async fn force_commit(&mut self) -> Result<Quantities> {
        let quantities = self.insert().await?;
        Ok(quantities)
    }

    async fn insert(&mut self) -> Result<Quantities> {
        self.in_transaction = false;
        let quantities = mem::replace(&mut self.pending, Quantities::ZERO);

        if let Some(mut insert) = self.insert.take() {
            insert.end().await?;
        }

        // TODO: Reinstate the callback
        // if let Some(cb) = &mut self.on_commit
        //     && quantities.transactions > 0
        // {
        //     (cb)(&quantities);
        // }

        Ok(quantities)
    }

    #[cold]
    #[inline(never)]
    async fn init_insert(&mut self) -> Result<()> {
        debug_assert!(self.insert.is_none());
        debug_assert_eq!(self.pending, Quantities::ZERO);
        let sql = format!("INSERT INTO {} FORMAT {}", self.table, self.format.as_str());
        let new_insert: BufInsertFormatted = self
            .client
            .insert_formatted_with(sql)
            .with_timeouts(self.send_timeout, self.end_timeout)
            .buffered_with_capacity(self.chunk_capacity);
        self.insert = Some(new_insert);
        Ok(())
    }
}
