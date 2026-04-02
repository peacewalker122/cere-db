use std::sync::atomic::AtomicU64;

pub struct SegmentId(pub AtomicU64);

impl SegmentId {
    pub fn filename(&self) -> String {
        format!(
            "{:020}.log",
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        )
        // zero-padded supaya lexicographic sort = chronological sort
    }
}
