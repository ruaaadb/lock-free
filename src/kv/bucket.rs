use std::borrow::Borrow;
use std::sync::atomic::Ordering;

use crossbeam_epoch::{self as epoch, Guard};

use crate::collections::LinkedMap;
use crate::epoch::{Atomic, Shared};

const OCCUPIED_BIT: u16 = 1 << 15;
const TAG_MASK: u16 = (1 << 15) - 1;

type Entry<K, V> = LinkedMap<K, V>;

#[repr(align(64))]
pub struct Bucket<K, V>
where
    K: Eq,
{
    entries: [Entry<K, V>; 7],
    overflow: Atomic<Bucket<K, V>>,
}

impl<K, V> Bucket<K, V>
where
    K: Eq,
{
    pub fn new() -> Self {
        Self {
            entries: Default::default(),
            overflow: Default::default(),
        }
    }
}

impl<K, V> Default for Bucket<K, V>
where
    K: Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

#[inline(always)]
fn match_tag<E>(ptr: &Shared<E>, tag: u16, occupied: bool) -> bool {
    match occupied {
        true => ptr.tag_equals(OCCUPIED_BIT | tag),
        false => ptr.tag_equals(tag),
    }
}

impl<K, V> Bucket<K, V>
where
    K: Eq,
{
    #[inline]
    fn find_entry_loop<'f>(
        &'f self,
        tag: u16,
        g: &'f Guard,
    ) -> (Option<&'f Entry<K, V>>, &'f Self) {
        for entry in &self.entries {
            let s = entry.head().load(Ordering::Acquire, g);
            if match_tag(&s, tag, true) {
                return (Some(entry), self);
            }
        }

        let overflow_ptr = self.overflow.load(Ordering::Acquire, g);
        if overflow_ptr.is_null() {
            return (None, self);
        }

        let mut bucket = unsafe { overflow_ptr.deref() };
        loop {
            for entry in &bucket.entries {
                let s = entry.head().load(Ordering::Acquire, g);
                if match_tag(&s, tag, true) {
                    return (Some(entry), bucket);
                }
            }

            let overflow_ptr = bucket.overflow.load(Ordering::Acquire, g);
            if overflow_ptr.is_null() {
                return (None, bucket);
            }
            bucket = unsafe { overflow_ptr.deref() };
        }
    }

    #[inline]
    fn find_entry<'f>(&'f self, tag: u16, g: &'f Guard) -> (Option<&'f Entry<K, V>>, &'f Self) {
        self.find_entry_loop(tag, g)
    }

    #[inline]
    fn find_entry_recursive<'f>(
        &'f self,
        tag: u16,
        g: &'f Guard,
    ) -> (Option<&'f Entry<K, V>>, &'f Self) {
        for entry in &self.entries {
            let s = entry.head().load(Ordering::Acquire, g);
            if match_tag(&s, tag, true) {
                return (Some(entry), self);
            }
        }

        let overflow_ptr = self.overflow.load(Ordering::Acquire, g);
        return match overflow_ptr.is_null() {
            true => (None, self),
            _ => {
                let overflow_bucket = unsafe { overflow_ptr.deref() };
                overflow_bucket.find_entry_recursive(tag, g)
            }
        };
    }

    fn try_enlist_new_overflow_bucket<'f>(
        tail: &Bucket<K, V>,
        g: &'f Guard,
    ) -> Shared<'f, Bucket<K, V>> {
        let bucket = Atomic::new(Bucket::new());
        let bucket = bucket.load(Ordering::Relaxed, g);
        match tail.overflow.compare_exchange(
            Shared::null(),
            bucket,
            Ordering::AcqRel,
            Ordering::Acquire,
            g,
        ) {
            Ok(_) => bucket,
            Err(err) => {
                drop(unsafe { bucket.into_owned() });
                err.current
            }
        }
    }

    fn install_entry<'f>(
        &'f self,
        new_entry: &Entry<K, V>,
        tag: u16,
        g: &'f Guard,
    ) -> (&'f Entry<K, V>, bool) {
        let new_entry_head_ptr = new_entry.head().load(Ordering::Relaxed, g);

        for entry in &self.entries {
            let s = entry.head().load(Ordering::Acquire, g);

            if match_tag(&s, tag, true) {
                return (entry, false);
            }

            if s.is_zero() {
                // Try CAS to install the new entry.
                match entry.head().compare_exchange(
                    s,
                    new_entry_head_ptr,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    g,
                ) {
                    Ok(_) => {
                        return (entry, true);
                    }
                    Err(_) => continue,
                }
            }
        }

        let mut overflow_ptr = self.overflow.load(Ordering::Acquire, g);
        if overflow_ptr.is_null() {
            overflow_ptr = Self::try_enlist_new_overflow_bucket(self, g);
        }
        let bucket = unsafe { overflow_ptr.deref() };
        bucket.install_entry(new_entry, tag, g)
    }

    fn try_install_new_entry<'f>(&'f self, tag: u16, g: &'f Guard) -> &'f Entry<K, V> {
        // New entry with tag and occupied bit set.
        let new_entry = Entry::new_with_tag(tag | OCCUPIED_BIT);

        // Install the entry or drop if fails.
        let (entry, new) = self.install_entry(&new_entry, tag, g);

        // If not installed, release the entry.
        if !new {
            drop(new_entry);
        }

        entry
    }

    fn find_or_new_entry<'f>(&'f self, tag: u16, g: &'f Guard) -> &'f Entry<K, V> {
        match self.find_entry(tag, g) {
            (Some(entry), _) => entry,
            (None, b) => {
                // Install into the last searched bucket (because the previous
                // is always full).
                b.try_install_new_entry(tag, g)
            }
        }
    }

    pub fn get<Q>(&self, tag: u16, key: Q, g: &Guard) -> Option<V>
    where
        Q: Borrow<K>,
        K: Clone,
        V: Clone,
    {
        match self.find_entry(tag, g) {
            (None, _) => None,
            (Some(entry), _) => entry.get(key, g),
        }
    }

    pub fn get_fn<Q, F, U>(&self, tag: u16, key: Q, f: F, g: &Guard) -> Option<U>
    where
        Q: Borrow<K>,
        F: Fn(&V) -> U,
    {
        match self.find_entry(tag, g) {
            (None, _) => None,
            (Some(entry), _) => entry.get_fn(key, f, g),
        }
    }

    pub fn insert(&self, tag: u16, key: K, value: V, g: &Guard) -> bool {
        let entry = self.find_or_new_entry(tag, g);
        entry.insert(key, value, g)
    }

    pub fn delete(&self, tag: u16, key: K, g: &Guard) -> bool {
        match self.find_entry(tag, g) {
            (None, _) => false,
            (Some(entry), _) => entry.remove(key, g),
        }
    }

    pub fn entries(&self) -> &[Entry<K, V>] {
        &self.entries
    }

    pub fn entry_size(&self, g: &Guard) -> usize {
        // Only for the current bucket, not the overflows.
        let mut len = 0;
        for entry in &self.entries {
            let s = entry.head().load(Ordering::Acquire, g);
            if (s.tag() & OCCUPIED_BIT) != 0 {
                len += 1;
            }
        }
        len
    }
}

pub struct BucketChain<K, V>
where
    K: Eq,
{
    head: Bucket<K, V>,
}

pub struct Iter<'f, K, V>
where
    K: Eq,
{
    next: Option<&'f Bucket<K, V>>,
}

impl<'f, K, V> Iterator for Iter<'f, K, V>
where
    K: Eq,
{
    type Item = &'f Bucket<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let g = unsafe { epoch::unprotected() };
        match self.next {
            None => None,
            Some(bucket) => {
                let overflow_ptr = bucket.overflow.load(Ordering::Acquire, g);
                if overflow_ptr.is_null() {
                    self.next = None;
                } else {
                    self.next = Some(unsafe { overflow_ptr.deref() });
                }
                Some(bucket)
            }
        }
    }
}

impl<K, V> BucketChain<K, V>
where
    K: Eq,
{
    pub fn new() -> Self {
        Self {
            head: Default::default(),
        }
    }

    #[inline]
    pub fn head(&self) -> &Bucket<K, V> {
        &self.head
    }

    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            next: Some(self.head()),
        }
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }
}

impl<K, V> Default for BucketChain<K, V>
where
    K: Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for BucketChain<K, V>
where
    K: Eq,
{
    fn drop(&mut self) {
        let g = unsafe { epoch::unprotected() };

        // Drop every overflow bucket.
        let mut overflow_ptr = self.head.overflow.load(Ordering::Acquire, g);
        while !overflow_ptr.is_null() {
            let bucket = unsafe { overflow_ptr.deref() };
            let next_ptr = bucket.overflow.load(Ordering::Acquire, g);
            drop(unsafe { overflow_ptr.into_owned() });
            overflow_ptr = next_ptr;
        }

        // Let the chain drop the head itself.
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use crossbeam_epoch as epoch;

    use crate::kv::bucket::BucketChain;

    #[test]
    fn test_bucket_chain() {
        assert_eq!(64, align_of::<BucketChain<i32, i32>>());
        assert_eq!(64, size_of::<BucketChain<i32, i32>>());

        let g = unsafe { epoch::unprotected() };

        let bc = BucketChain::new();
        let b = bc.head();

        assert!(b.insert(0, 1, 1, g));
        assert_eq!(Some(1), b.get(0, 1, g));
        assert_eq!(None, b.get(1, 1, g));
        assert!(b.insert(0, 2, 1, g));
        assert_eq!(Some(1), b.get(0, 2, g));
        assert!(b.delete(0, 1, g));
        assert_eq!(None, b.get(0, 1, g));

        for i in 0..14 {
            assert!(b.insert(1, i, -i, g));
            assert_eq!(Some(-i), b.get(1, i, g));
        }
    }
}
