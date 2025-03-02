use bytes::Bytes;
use iggy::models::batch::IggyHeader;
use std::ops::Range;

#[derive(Default)]
pub struct IggyBatchSlice {
    pub range: Range<usize>,
    pub header: IggyHeader,
    pub bytes: Bytes,
}

impl IggyBatchSlice {
    pub fn new(range: Range<usize>, header: IggyHeader, bytes: Bytes) -> Self {
        Self {
            range,
            header,
            bytes,
        }
    }
}

#[derive(Default)]
pub struct IggyBatchFetchResult {
    pub header: IggyHeader,
    pub slices: Vec<IggyBatchSlice>,
}

impl From<Vec<IggyBatchSlice>> for IggyBatchFetchResult {
    fn from(slices: Vec<IggyBatchSlice>) -> Self {
        if slices.is_empty() {
            return Default::default();
        }
        let header = slices[0].header;
        IggyBatchFetchResult::new(header, slices)
    }
}

impl IggyBatchFetchResult {
    pub fn new(header: IggyHeader, slices: Vec<IggyBatchSlice>) -> Self {
        Self { header, slices }
    }
}
