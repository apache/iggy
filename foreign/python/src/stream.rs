use iggy::models::stream::StreamDetails as RustStreamDetails;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[pyclass]
#[gen_stub_pyclass]
pub struct StreamDetails {
    pub(crate) inner: RustStreamDetails,
}

impl From<RustStreamDetails> for StreamDetails {
    fn from(stream_details: RustStreamDetails) -> Self {
        Self {
            inner: stream_details,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl StreamDetails {
    #[getter]
    pub fn id(&self) -> u32 {
        self.inner.id
    }

    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.to_string()
    }

    #[getter]
    pub fn messages_count(&self) -> u64 {
        self.inner.messages_count
    }

    #[getter]
    pub fn topics_count(&self) -> u32 {
        self.inner.topics_count
    }
}
