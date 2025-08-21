#[derive(Debug, Clone)]
pub struct Segment2 {
    pub id: usize,
    // Parent id is the id of the partiton this segment belongs to.
    pub parent_id: usize,
}
