pub struct SegmentId(pub u64);

impl SegmentId {
    pub fn filename(&self) -> String {
        format!("{:020}.log", self.0)
        // zero-padded supaya lexicographic sort = chronological sort
    }
}
