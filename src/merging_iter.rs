use crate::cmp::Cmp;
use crate::types::{current_key_val, Direction, LdbIterator};

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::rc::Rc;

// Warning: This module is kinda messy. The original implementation is
// not that much better though :-)
//
// Issues: 1) prev() may not work correctly at the beginning of a merging
// iterator.

#[derive(PartialEq)]
enum SL {
    Smallest,
    Largest,
}

pub struct MergingIter {
    iters: Vec<Box<dyn LdbIterator>>,
    current: Option<usize>,
    direction: Direction,
    cmp: Rc<dyn Cmp>,
}

impl MergingIter {
    /// Construct a new merging iterator.
    pub fn new(cmp: Rc<dyn Cmp>, iters: Vec<Box<dyn LdbIterator>>) -> MergingIter {
        MergingIter {
            iters,
            current: None,
            direction: Direction::Forward,
            cmp,
        }
    }

    fn init(&mut self) {
        for i in 0..self.iters.len() {
            self.iters[i].reset();
            self.iters[i].advance();
            if !self.iters[i].valid() {
                self.iters[i].reset()
            }
        }
        self.find_smallest();
    }

    /// Adjusts the direction of the iterator depending on whether the last
    /// call was next() or prev(). This basically sets all iterators to one
    /// entry after (Forward) or one entry before (Reverse) the current() entry.
    fn update_direction(&mut self, d: Direction) {
        if self.direction == d {
            return;
        }

        if let Some((key, _)) = current_key_val(self) {
            if let Some(current) = self.current {
                match d {
                    Direction::Forward if self.direction == Direction::Reverse => {
                        self.direction = Direction::Forward;
                        for i in 0..self.iters.len() {
                            if i != current {
                                self.iters[i].seek(&key);
                                // This doesn't work if two iterators are returning the exact same
                                // keys. However, in reality, two entries will always have differing
                                // sequence numbers.
                                if let Some((current_key, _)) = self.iters[i].current() {
                                    if self.cmp.cmp(&current_key, &key) == Ordering::Equal {
                                        self.iters[i].advance();
                                    }
                                }
                            }
                        }
                    }
                    Direction::Reverse if self.direction == Direction::Forward => {
                        self.direction = Direction::Reverse;
                        for i in 0..self.iters.len() {
                            if i != current {
                                self.iters[i].seek(&key);
                                if self.iters[i].valid() {
                                    self.iters[i].prev();
                                } else {
                                    // seek to last.
                                    while self.iters[i].advance() {}
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn find_smallest(&mut self) {
        self.find(SL::Smallest)
    }
    fn find_largest(&mut self) {
        self.find(SL::Largest)
    }

    fn find(&mut self, direction: SL) {
        if self.iters.is_empty() {
            // Iterator stays invalid.
            return;
        }

        let ord = if direction == SL::Smallest {
            Ordering::Less
        } else {
            Ordering::Greater
        };

        let mut next_ix = 0;

        for i in 1..self.iters.len() {
            if let Some((current_key, _)) = self.iters[i].current() {
                if let Some((smallest_key, _)) = self.iters[next_ix].current() {
                    if self.cmp.cmp(&current_key, &smallest_key) == ord {
                        next_ix = i;
                    }
                } else {
                    next_ix = i;
                }
            }
        }

        self.current = Some(next_ix);
    }
}

impl LdbIterator for MergingIter {
    fn advance(&mut self) -> bool {
        if let Some(current) = self.current {
            self.update_direction(Direction::Forward);
            if !self.iters[current].advance() {
                // Take this iterator out of rotation; this will return false
                // for every call to current() and thus it will be ignored
                // from here on.
                self.iters[current].reset();
            }
            self.find_smallest();
        } else {
            self.init();
        }
        self.valid()
    }
    fn valid(&self) -> bool {
        if let Some(ix) = self.current {
            self.iters[ix].valid()
        } else {
            false
        }
    }
    fn seek(&mut self, key: &[u8]) {
        for i in 0..self.iters.len() {
            self.iters[i].seek(key);
        }
        self.find_smallest();
    }
    fn reset(&mut self) {
        for i in 0..self.iters.len() {
            self.iters[i].reset();
        }
        self.current = None;
    }
    fn current(&self) -> Option<(Bytes, Bytes)> {
        if let Some(ix) = self.current {
            self.iters[ix].current()
        } else {
            None
        }
    }
    fn prev(&mut self) -> bool {
        if let Some(current) = self.current {
            if self.iters[current].valid() {
                self.update_direction(Direction::Reverse);
                self.iters[current].prev();
                self.find_largest();
                self.valid()
            } else {
                false
            }
        } else {
            false
        }
    }
}

/// One child's current key in the merge heap. Ordered as a *min-heap* under the
/// comparator, so the heap root is the smallest key across all children (i.e.
/// the newest version first, matching `MergingIter::find_smallest`).
struct HeapEntry {
    key: Bytes,
    idx: usize,
    cmp: Rc<dyn Cmp>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp.cmp(&self.key, &other.key) == Ordering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reversing the arguments turns it into a
        // min-heap under `cmp` (smaller key => higher priority => popped first).
        self.cmp.cmp(&other.key, &self.key)
    }
}

/// Forward-only merging iterator backed by a binary heap.
///
/// `MergingIter::find_smallest` is O(children) on every `advance()`, which makes
/// a wide merge (e.g. a compaction over hundreds/thousands of overlapping L0
/// tables) O(children) per emitted key - pathologically slow. This iterator is
/// O(log children) per key instead, at the cost of supporting forward iteration
/// only. It is used solely by `VersionSet::make_input_iterator` (compaction
/// input), which drives it strictly forward (`seek_to_first` then `advance`);
/// the read path keeps the bidirectional `MergingIter`. `prev()` panics.
pub struct HeapMergeIter {
    iters: Vec<Box<dyn LdbIterator>>,
    cmp: Rc<dyn Cmp>,
    heap: BinaryHeap<HeapEntry>,
    // Index of the child holding the current entry (the heap root), or None.
    current: Option<usize>,
    initialized: bool,
}

impl HeapMergeIter {
    pub fn new(cmp: Rc<dyn Cmp>, iters: Vec<Box<dyn LdbIterator>>) -> HeapMergeIter {
        HeapMergeIter {
            iters,
            cmp,
            heap: BinaryHeap::new(),
            current: None,
            initialized: false,
        }
    }

    /// Push child `idx`'s current key onto the heap if it is still valid.
    fn push_idx(&mut self, idx: usize) {
        if self.iters[idx].valid() {
            if let Some((k, _)) = self.iters[idx].current() {
                self.heap.push(HeapEntry { key: k, idx, cmp: self.cmp.clone() });
            }
        }
    }

    /// Position every child at its first entry and (re)build the heap.
    fn init(&mut self) {
        self.heap.clear();
        for i in 0..self.iters.len() {
            self.iters[i].reset();
            self.iters[i].advance();
            if self.iters[i].valid() {
                self.push_idx(i);
            } else {
                self.iters[i].reset();
            }
        }
        self.initialized = true;
        self.current = self.heap.peek().map(|e| e.idx);
    }
}

impl LdbIterator for HeapMergeIter {
    fn advance(&mut self) -> bool {
        if !self.initialized {
            self.init();
            return self.valid();
        }
        // Emit the next key: pop the current smallest, advance that child, and
        // re-insert its new position. The new root is the next smallest key.
        if let Some(top) = self.heap.pop() {
            let idx = top.idx;
            self.iters[idx].advance();
            self.push_idx(idx);
        }
        self.current = self.heap.peek().map(|e| e.idx);
        self.valid()
    }
    fn valid(&self) -> bool {
        match self.current {
            Some(ix) => self.iters[ix].valid(),
            None => false,
        }
    }
    fn current(&self) -> Option<(Bytes, Bytes)> {
        match self.current {
            Some(ix) => self.iters[ix].current(),
            None => None,
        }
    }
    fn seek(&mut self, key: &[u8]) {
        self.heap.clear();
        for i in 0..self.iters.len() {
            self.iters[i].seek(key);
            if self.iters[i].valid() {
                self.push_idx(i);
            }
        }
        self.initialized = true;
        self.current = self.heap.peek().map(|e| e.idx);
    }
    fn reset(&mut self) {
        self.heap.clear();
        for i in 0..self.iters.len() {
            self.iters[i].reset();
        }
        self.current = None;
        self.initialized = false;
    }
    fn prev(&mut self) -> bool {
        // Forward-only: only used for compaction input, which never goes back.
        panic!("HeapMergeIter is forward-only and does not support prev()");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cmp::DefaultCmp;
    use crate::skipmap::tests;
    use crate::test_util::{test_iterator_properties, LdbIteratorIter, TestLdbIter};
    use crate::types::{current_key_val, LdbIterator};

    #[test]
    fn test_merging_one() {
        let skm = tests::make_skipmap();
        let iter = skm.iter();
        let mut iter2 = skm.iter();

        let mut miter = MergingIter::new(Rc::new(DefaultCmp),vec![Box::new(iter)]);

        while let Some((k, v)) = miter.next() {
            if let Some((k2, v2)) = iter2.next() {
                assert_eq!(k, k2);
                assert_eq!(v, v2);
            } else {
                panic!("Expected element from iter2");
            }
        }
    }

    #[test]
    fn test_merging_two() {
        let skm = tests::make_skipmap();
        let iter = skm.iter();
        let iter2 = skm.iter();

        let mut miter = MergingIter::new(
            Rc::new(DefaultCmp),
            vec![Box::new(iter), Box::new(iter2)],
        );

        while let Some((k, v)) = miter.next() {
            if let Some((k2, v2)) = miter.next() {
                assert_eq!(k, k2);
                assert_eq!(v, v2);
            } else {
                panic!("Odd number of elements");
            }
        }
    }

    #[test]
    fn test_merging_zero() {
        let mut miter = MergingIter::new(Rc::new(DefaultCmp),vec![]);
        assert_eq!(0, LdbIteratorIter::wrap(&mut miter).count());
    }

    #[test]
    fn test_merging_behavior() {
        let val = b"def";
        let iter = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val)]);
        let iter2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);
        let miter = MergingIter::new(
            Rc::new(DefaultCmp),
            vec![Box::new(iter), Box::new(iter2)],
        );
        test_iterator_properties(miter);
    }

    #[test]
    fn test_merging_forward_backward() {
        let val = b"def";
        let iter = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
        let iter2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);

        let mut miter = MergingIter::new(
            Rc::new(DefaultCmp),
            vec![Box::new(iter), Box::new(iter2)],
        );

        // miter should return the following sequence: [aba, abb, abc, abd, abe]

        // -> aba
        let first = miter.next();
        // -> abb
        let second = miter.next();
        // -> abc
        let third = miter.next();
        eprintln!("{:?} {:?} {:?}", first, second, third);

        assert!(first != third);
        // abb <-
        assert!(miter.prev());
        assert_eq!(second, current_key_val(&miter));
        // aba <-
        assert!(miter.prev());
        assert_eq!(first, current_key_val(&miter));
        // -> abb
        assert!(miter.advance());
        assert_eq!(second, current_key_val(&miter));
        // -> abc
        assert!(miter.advance());
        assert_eq!(third, current_key_val(&miter));
        // -> abd
        assert!(miter.advance());
        assert_eq!(
            Some((b("abd").to_vec(), val.to_vec())),
            current_key_val(&miter)
        );
    }

    fn b(s: &'static str) -> &'static [u8] {
        s.as_bytes()
    }

    #[test]
    fn test_merging_real() {
        let val = b"def";

        let it1 = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
        let it2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);
        let expected = [b("aba"), b("abb"), b("abc"), b("abd"), b("abe")];

        let mut iter = MergingIter::new(
            Rc::new(DefaultCmp),
            vec![Box::new(it1), Box::new(it2)],
        );

        for (i, (k, _)) in LdbIteratorIter::wrap(&mut iter).enumerate() {
            assert_eq!(k, expected[i]);
        }
    }

    #[test]
    fn test_merging_seek_reset() {
        let val = b"def";

        let it1 = TestLdbIter::new(vec![(b("aba"), val), (b("abc"), val), (b("abe"), val)]);
        let it2 = TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)]);

        let mut iter = MergingIter::new(
            Rc::new(DefaultCmp),
            vec![Box::new(it1), Box::new(it2)],
        );

        assert!(!iter.valid());
        iter.advance();
        assert!(iter.valid());
        assert!(current_key_val(&iter).is_some());

        iter.seek(b"abc");
        assert_eq!(
            current_key_val(&iter),
            Some((b("abc").to_vec(), val.to_vec()))
        );
        iter.seek(b"ab0");
        assert_eq!(
            current_key_val(&iter),
            Some((b("aba").to_vec(), val.to_vec()))
        );
        iter.seek(b"abx");
        assert_eq!(current_key_val(&iter), None);

        iter.reset();
        assert!(!iter.valid());
        iter.next();
        assert_eq!(
            current_key_val(&iter),
            Some((b("aba").to_vec(), val.to_vec()))
        );
    }

    #[test]
    fn test_heap_merge_forward_matches_merging_iter() {
        // HeapMergeIter must emit the exact same forward sequence as the linear
        // MergingIter for the same inputs - it changes only the per-step cost
        // (O(log n) vs O(n)), not the order.
        let val = b"def";
        let make = || -> Vec<Box<dyn LdbIterator>> {
            vec![
                Box::new(TestLdbIter::new(vec![
                    (b("aba"), val),
                    (b("abc"), val),
                    (b("abe"), val),
                ])),
                Box::new(TestLdbIter::new(vec![(b("abb"), val), (b("abd"), val)])),
                Box::new(TestLdbIter::new(vec![
                    (b("aa0"), val),
                    (b("abf"), val),
                    (b("abz"), val),
                ])),
            ]
        };
        let cmp: Rc<dyn Cmp> = Rc::new(DefaultCmp);

        let mut linear = MergingIter::new(cmp.clone(), make());
        let mut heaped = HeapMergeIter::new(cmp.clone(), make());

        let linear_seq: Vec<_> = LdbIteratorIter::wrap(&mut linear).collect();
        let heaped_seq: Vec<_> = LdbIteratorIter::wrap(&mut heaped).collect();

        assert_eq!(linear_seq, heaped_seq, "heap merge order must match linear merge");

        let expected = ["aa0", "aba", "abb", "abc", "abd", "abe", "abf", "abz"];
        assert_eq!(heaped_seq.len(), expected.len());
        for (i, (k, _)) in heaped_seq.iter().enumerate() {
            assert_eq!(k.as_slice(), expected[i].as_bytes());
        }
    }

    #[test]
    fn test_heap_merge_empty() {
        let cmp: Rc<dyn Cmp> = Rc::new(DefaultCmp);
        let mut it = HeapMergeIter::new(cmp, vec![]);
        assert_eq!(0, LdbIteratorIter::wrap(&mut it).count());
        assert!(!it.valid());
    }

    #[test]
    fn test_heap_merge_seek_to_first() {
        let val = b"def";
        let cmp: Rc<dyn Cmp> = Rc::new(DefaultCmp);
        let mut it = HeapMergeIter::new(
            cmp,
            vec![
                Box::new(TestLdbIter::new(vec![(b("abc"), val), (b("abe"), val)])),
                Box::new(TestLdbIter::new(vec![(b("aba"), val), (b("abd"), val)])),
            ],
        );
        it.seek_to_first();
        // Lands on the smallest key across both children, including the first entry.
        assert_eq!(current_key_val(&it), Some((b("aba").to_vec(), val.to_vec())));
        let mut got = vec![current_key_val(&it).unwrap().0];
        while it.advance() {
            got.push(current_key_val(&it).unwrap().0);
        }
        assert_eq!(
            got,
            vec![
                b("aba").to_vec(),
                b("abc").to_vec(),
                b("abd").to_vec(),
                b("abe").to_vec(),
            ]
        );
    }
}
