use std::collections::HashMap;

use crate::types::{share, SequenceNumber, Shared, MAX_SEQUENCE_NUMBER};

use std::rc::Rc;

/// Opaque snapshot handle; Represents index to SnapshotList.map
type SnapshotHandle = u64;

/// An InnerSnapshot is shared by several Snapshots. This enables cloning snapshots, and a snapshot
/// is released once the last instance is dropped.
#[derive(Clone)]
struct InnerSnapshot {
    id: SnapshotHandle,
    seq: SequenceNumber,
    sl: Shared<InnerSnapshotList>,
}

impl Drop for InnerSnapshot {
    fn drop(&mut self) {
        self.sl.borrow_mut().delete(self.id);
    }
}

#[derive(Clone)]
pub struct Snapshot {
    inner: Rc<InnerSnapshot>,
}

impl Snapshot {
    pub fn sequence(&self) -> SequenceNumber {
        self.inner.seq
    }
}

/// A list of all snapshots is kept in the DB.
struct InnerSnapshotList {
    map: HashMap<SnapshotHandle, SequenceNumber>,
    newest: SnapshotHandle,
    oldest: SnapshotHandle,
}

pub struct SnapshotList {
    inner: Shared<InnerSnapshotList>,
}

impl SnapshotList {
    pub fn new() -> SnapshotList {
        SnapshotList {
            inner: share(InnerSnapshotList {
                map: HashMap::new(),
                newest: 0,
                oldest: 0,
            }),
        }
    }

    pub fn new_snapshot(&mut self, seq: SequenceNumber) -> Snapshot {
        let inner = self.inner.clone();
        let mut sl = self.inner.borrow_mut();

        sl.newest += 1;
        let newest = sl.newest;
        sl.map.insert(newest, seq);

        if sl.oldest == 0 {
            sl.oldest = sl.newest;
        }

        Snapshot {
            inner: Rc::new(InnerSnapshot {
                id: sl.newest,
                seq,
                sl: inner,
            }),
        }
    }

    /// oldest returns the lowest sequence number of all snapshots. It returns 0 if no snapshots
    /// are present.
    pub fn oldest(&self) -> SequenceNumber {
        let oldest = self
            .inner
            .borrow()
            .map
            .iter()
            .fold(
                MAX_SEQUENCE_NUMBER,
                |s, (_, seq)| if *seq < s { *seq } else { s },
            );
        if oldest == MAX_SEQUENCE_NUMBER {
            0
        } else {
            oldest
        }
    }

    /// newest returns the newest sequence number of all snapshots. If no snapshots are present, it
    /// returns 0.
    pub fn newest(&self) -> SequenceNumber {
        self.inner
            .borrow()
            .map
            .iter()
            .fold(0, |s, (_, seq)| if *seq > s { *seq } else { s })
    }

    pub fn empty(&self) -> bool {
        self.inner.borrow().map.is_empty()
    }
}

impl InnerSnapshotList {
    fn delete(&mut self, id: SnapshotHandle) {
        self.map.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(unused_variables)]
    #[test]
    fn test_snapshot_list() {
        let mut l = SnapshotList::new();

        {
            assert!(l.empty());
            let a = l.new_snapshot(1);

            {
                let b = l.new_snapshot(2);

                {
                    let c = l.new_snapshot(3);

                    assert!(!l.empty());
                    assert_eq!(l.oldest(), 1);
                    assert_eq!(l.newest(), 3);
                }

                assert_eq!(l.newest(), 2);
                assert_eq!(l.oldest(), 1);
            }

            assert_eq!(l.oldest(), 1);
        }
        assert_eq!(l.oldest(), 0);
    }

    // The original test above accidentally passes even with the bugs because seq==handle
    // (seq=1 for handle=1, seq=2 for handle=2, etc.). These tests use non-sequential
    // sequence numbers to catch the real bugs.

    #[test]
    fn test_oldest_returns_minimum_sequence_not_handle() {
        // Bug: oldest() iterated over map keys (SnapshotHandle) instead of values
        // (SequenceNumber). With seq != handle, it returned the wrong value.
        let mut l = SnapshotList::new();
        let _a = l.new_snapshot(1000); // handle=1, seq=1000
        let _b = l.new_snapshot(500);  // handle=2, seq=500  ← lowest seq, higher handle
        let _c = l.new_snapshot(2000); // handle=3, seq=2000 ← highest seq
        assert_eq!(l.oldest(), 500,  "oldest() must return minimum sequence, not minimum handle");
        assert_eq!(l.newest(), 2000, "newest() must return maximum sequence, not maximum handle");
    }

    #[test]
    fn test_empty_returns_true_after_all_snapshots_dropped() {
        // Bug: empty() checked the `oldest` struct field, which is set on first
        // snapshot creation and never reset to 0 when snapshots are deleted.
        // After dropping all snapshots, empty() incorrectly returned false.
        let mut l = SnapshotList::new();
        assert!(l.empty());
        {
            let _snap = l.new_snapshot(999);
            assert!(!l.empty());
        } // _snap dropped here
        assert!(l.empty(), "empty() must return true after all snapshots are released");
        // oldest()/newest() must also behave sensibly on an empty list.
        assert_eq!(l.oldest(), 0, "oldest() on empty list must return 0");
        assert_eq!(l.newest(), 0, "newest() on empty list must return 0");
    }
}
