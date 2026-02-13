use crate::clickhouse_inserter::Quantities;
use clickhouse::Row;
use iggy_connector_sdk::ConsumedMessage;

/// Common trait for all inserter types (Inserter, ClickHouseInserter, etc.)
#[allow(async_fn_in_trait)]
pub trait GenericInserter: Send {
    /// The type of data this inserter accepts
    type WriteData;

    /// Write a single item
    async fn write(&mut self, data: Self::WriteData) -> clickhouse::error::Result<()>;

    /// Force commit regardless of limits
    async fn force_commit(&mut self) -> clickhouse::error::Result<Quantities>;
}

// Implement the trait for the standard Inserter<T>
impl<T> GenericInserter for clickhouse::inserter::Inserter<T>
where
    T: Row + clickhouse::RowWrite,
{
    type WriteData = T::Value<'static>;

    async fn write(&mut self, data: Self::WriteData) -> clickhouse::error::Result<()> {
        self.write(&data).await
    }

    async fn force_commit(&mut self) -> clickhouse::error::Result<Quantities> {
        let q = self.force_commit().await?;
        Ok(Quantities {
            bytes: q.bytes,
            rows: q.rows,
            transactions: q.transactions,
        })
    }
}

// Implement the trait for JsonInserter
impl GenericInserter for crate::ClickHouseInserter {
    type WriteData = Vec<u8>;

    async fn write(&mut self, data: Self::WriteData) -> clickhouse::error::Result<()> {
        self.write(data).await
    }

    async fn force_commit(&mut self) -> clickhouse::error::Result<Quantities> {
        let q = self.force_commit().await?;
        Ok(Quantities {
            bytes: q.bytes,
            rows: q.rows,
            transactions: q.transactions,
        })
    }
}

/// Write and force-commit
///
/// # Type Parameters
/// - `I`: The inserter type (must implement GenericInserter)
/// - `F`: Function to prepare WriteData from a ConsumedMessage
pub async fn run_inserter<I, F>(
    mut inserter: I,
    messages: &[ConsumedMessage],
    mut prepare_data: F,
) -> clickhouse::error::Result<()>
where
    I: GenericInserter,
    F: FnMut(&ConsumedMessage) -> clickhouse::error::Result<I::WriteData>,
{
    for message in messages {
        // Prepare the data in the format expected by this inserter
        let data = prepare_data(message)?;
        inserter.write(data).await?;
    }

    // force_commit to flush
    inserter.force_commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Payload;
    use std::sync::{Arc, Mutex};

    // ── Mock inserter ────────────────────────────────────────────────────────

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MockCall {
        Write(String),
        ForceCommit,
    }

    struct MockInserter {
        calls: Arc<Mutex<Vec<MockCall>>>,
        write_error_on_call: Option<usize>,
        write_count: usize,
    }

    impl MockInserter {
        fn new(calls: Arc<Mutex<Vec<MockCall>>>) -> Self {
            Self {
                calls,
                write_error_on_call: None,
                write_count: 0,
            }
        }

        fn failing_on_write(calls: Arc<Mutex<Vec<MockCall>>>, fail_on_call: usize) -> Self {
            Self {
                calls,
                write_error_on_call: Some(fail_on_call),
                write_count: 0,
            }
        }
    }

    impl GenericInserter for MockInserter {
        type WriteData = String;

        async fn write(&mut self, data: Self::WriteData) -> clickhouse::error::Result<()> {
            self.write_count += 1;
            if self.write_error_on_call == Some(self.write_count) {
                return Err(clickhouse::error::Error::Custom("mock write error".into()));
            }
            self.calls.lock().unwrap().push(MockCall::Write(data));
            Ok(())
        }

        async fn force_commit(&mut self) -> clickhouse::error::Result<Quantities> {
            self.calls.lock().unwrap().push(MockCall::ForceCommit);
            Ok(Quantities {
                bytes: 0,
                rows: 0,
                transactions: 0,
            })
        }
    }

    // ── Test fixtures ────────────────────────────────────────────────────────

    fn test_message(id: u128) -> ConsumedMessage {
        ConsumedMessage {
            id,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Json(simd_json::json!({"msg_id": id as u64})),
        }
    }

    // ── run_inserter tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn given_messages_should_write_each_and_force_commit() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let inserter = MockInserter::new(calls.clone());
        let messages = vec![test_message(1), test_message(2), test_message(3)];

        let result = run_inserter(inserter, &messages, |msg| Ok(format!("msg-{}", msg.id))).await;

        assert!(result.is_ok());
        let recorded = calls.lock().unwrap();
        assert_eq!(recorded.len(), 4); // 3 writes + 1 force_commit
        assert_eq!(recorded[0], MockCall::Write("msg-1".into()));
        assert_eq!(recorded[1], MockCall::Write("msg-2".into()));
        assert_eq!(recorded[2], MockCall::Write("msg-3".into()));
        assert_eq!(recorded[3], MockCall::ForceCommit);
    }

    #[tokio::test]
    async fn given_prepare_data_error_should_propagate() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let inserter = MockInserter::new(calls.clone());
        let messages = vec![test_message(1)];

        let result = run_inserter(inserter, &messages, |_msg| {
            Err(clickhouse::error::Error::Custom("prepare failed".into()))
        })
        .await;

        assert!(result.is_err());
        let recorded = calls.lock().unwrap();
        // No write, no force_commit — error propagated immediately
        assert!(recorded.is_empty());
    }

    #[tokio::test]
    async fn given_write_error_should_propagate() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let inserter = MockInserter::failing_on_write(calls.clone(), 1);
        let messages = vec![test_message(1), test_message(2)];

        let result = run_inserter(inserter, &messages, |msg| Ok(format!("msg-{}", msg.id))).await;

        assert!(result.is_err());
        let recorded = calls.lock().unwrap();
        // No successful write recorded, no force_commit
        assert!(recorded.is_empty());
    }
}
