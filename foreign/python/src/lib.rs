pub mod client;
mod receive_message;
mod send_message;
mod stream;
mod topic;

use client::IggyClient;
use pyo3::prelude::*;
use receive_message::{MessageState, PollingStrategy, ReceiveMessage};
use send_message::SendMessage;
use stream::StreamDetails;
use topic::TopicDetails;

/// A Python module implemented in Rust.
#[pymodule]
fn iggy_py(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SendMessage>()?;
    m.add_class::<ReceiveMessage>()?;
    m.add_class::<IggyClient>()?;
    m.add_class::<StreamDetails>()?;
    m.add_class::<TopicDetails>()?;
    m.add_class::<PollingStrategy>()?;
    m.add_class::<MessageState>()?;
    Ok(())
}
