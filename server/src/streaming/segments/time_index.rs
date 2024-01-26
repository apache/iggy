#[derive(Debug, Clone, Copy, Default)]
pub struct TimeIndex {
    pub relative_offset: u32,
    pub timestamp: u64,
}
