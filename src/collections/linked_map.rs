use std::borrow::Borrow;
use std::fmt;
use std::fmt::Formatter;

use crossbeam_epoch::Guard;

use crate::collections::{AtomicNodePtr, IntoIter, Iter, LinkedList};
use crate::epoch::Atomic;

pub struct Entry<K, V> {
    key: K,
    value: Option<V>,
}

impl<K, V> fmt::Debug for Entry<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value)
            .finish()
    }
}

impl<K, V> PartialEq<Self> for Entry<K, V>
where
    K: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        // Entry equals when key equals.
        self.key.eq(&other.key)
    }
}

impl<K, V> Eq for Entry<K, V> where K: Eq {}

impl<K, V> AsRef<K> for Entry<K, V> {
    fn as_ref(&self) -> &K {
        &self.key
    }
}

impl<K, V> From<(K, V)> for Entry<K, V> {
    fn from(t: (K, V)) -> Self {
        Self::new(t.0, t.1)
    }
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value: Some(value),
        }
    }
}

impl<K, V> PartialEq<K> for Entry<K, V>
where
    K: Eq,
{
    fn eq(&self, other: &K) -> bool {
        self.key.eq(other)
    }
}

impl<K, V> From<K> for Entry<K, V> {
    fn from(key: K) -> Self {
        Self { key, value: None }
    }
}

pub struct EntryRef<'f, K, V> {
    key: &'f K,
    value: &'f V,
}

impl<'f, K, V> fmt::Debug for EntryRef<'f, K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntryRef")
            .field("key", self.key)
            .field("value", self.value)
            .finish()
    }
}

pub struct LinkedMap<K, V> {
    list: LinkedList<Entry<K, V>>,
}

impl<K, V> LinkedMap<K, V> {
    pub fn new() -> Self {
        Self {
            list: LinkedList::new(),
        }
    }

    pub fn new_with_tag(tag: u16) -> Self {
        Self {
            list: LinkedList::new_with_head(Atomic::null_with_tag(tag)),
        }
    }
}

impl<K, V> Default for LinkedMap<K, V>
where
    K: Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct MapIter<'f, K, V> {
    iter: Iter<'f, Entry<K, V>>,
}

impl<'f, K, V> Iterator for MapIter<'f, K, V> {
    type Item = EntryRef<'f, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|e| EntryRef {
            key: &e.key,
            value: e.value.as_ref().unwrap(),
        })
    }
}

impl<K, V> LinkedMap<K, V>
where
    K: Eq,
{
    pub fn head(&self) -> &AtomicNodePtr<Entry<K, V>> {
        self.list.head()
    }

    pub fn is_empty(&self, g: &Guard) -> bool {
        self.list.is_empty_key(g)
    }

    pub fn contains<Q>(&self, key: Q, g: &Guard) -> bool
    where
        Q: Borrow<K>,
    {
        self.list.contains_key(key.borrow(), g)
    }

    pub fn insert(&self, key: K, value: V, g: &Guard) -> bool {
        self.list.insert_key(Entry::new(key, value), g)
    }

    pub fn remove(&self, key: K, g: &Guard) -> bool {
        self.list.delete_key(key, g)
    }

    pub fn get_fn<F, T, Q>(&self, key: Q, f: F, g: &Guard) -> Option<T>
    where
        Q: Borrow<K>,
        F: Fn(&V) -> T,
    {
        self.list.find_fn(
            |e| {
                if key.borrow().eq(&e.key) {
                    return Some(f(e.value.as_ref().unwrap()));
                }
                None
            },
            g,
        )
    }

    pub fn get<Q>(&self, key: Q, g: &Guard) -> Option<V>
    where
        K: Clone,
        V: Clone,
        Q: Borrow<K>,
    {
        self.list.find_fn(
            |e| {
                if e.eq(key.borrow()) {
                    Some(e.value.as_ref().unwrap().clone())
                } else {
                    None
                }
            },
            g,
        )
    }

    pub fn find_fn<Q, F>(&self, key: Q, f: F, g: &Guard) -> bool
    where
        F: Fn(&V),
        Q: Borrow<K>,
    {
        self.list.find_fn(
            |e| {
                if key.borrow().eq(&e.key) {
                    f(e.value.as_ref().unwrap());
                    return Some(());
                }
                None
            },
            g,
        ) != None
    }

    pub fn list_len(&self, g: &Guard) -> usize {
        self.list.list_len(g)
    }

    pub fn len(&self, g: &Guard) -> usize {
        self.list.len_key(g)
    }

    pub fn clear(&self, g: &Guard) {
        self.list.clear(g)
    }

    pub fn retain<F>(&self, f: F, g: &Guard)
    where
        F: Fn(&K) -> bool,
        K: Clone,
    {
        for elem in self.list.iter(g) {
            if f(&elem.key) {
                self.remove(elem.key.clone(), g);
            }
        }
    }

    pub fn iter<'f>(&self, g: &'f Guard) -> MapIter<'f, K, V> {
        MapIter {
            iter: self.list.iter(g),
        }
    }
}

impl<K, V> IntoIterator for LinkedMap<K, V> {
    type Item = Entry<K, V>;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.into_iter()
    }
}

impl<K, V, T> From<T> for LinkedMap<K, V>
where
    T: Into<LinkedList<Entry<K, V>>>,
{
    fn from(x: T) -> Self {
        Self { list: x.into() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use crossbeam_epoch as epoch;
    use crossbeam_utils::atomic::AtomicConsume;

    use crate::collections::{Entry, LinkedMap};

    #[test]
    fn test_map_works() {
        let m = LinkedMap::from((0i64..10).map(|x| Entry::from((x, x))));
        let g = unsafe { epoch::unprotected() };

        assert_eq!(10, m.len(g));
        for i in 0i64..10 {
            assert!(m.contains(i, g));
            assert_eq!(Some(i), m.get(i, g));
        }

        m.iter(g).for_each(|e| {
            assert_eq!(e.key, e.value);
            dbg!(e);
        })
    }

    #[test]
    fn test_map_with_atomic_value_works() {
        let m = LinkedMap::from((0i64..10).map(|x| Entry::from((x, AtomicI64::new(x)))));
        let g = unsafe { epoch::unprotected() };

        assert_eq!(10, m.len(g));
        for i in 0i64..10 {
            assert!(m.contains(i, g));
            assert_eq!(Some(i), m.get_fn(i, |v| { v.load_consume() }, g));
        }

        // Invert the values.
        for i in 0i64..10 {
            m.find_fn(i, |v| v.store(-i, Ordering::Release), g);
        }

        m.iter(g).for_each(|e| {
            assert_eq!(-e.key, e.value.load_consume());
            dbg!(e);
        });

        // Delete 1 value.
        assert!(m.contains(0, g));
        assert!(m.remove(0, g));
        assert!(!m.remove(0, g));
        assert_eq!(9, m.len(g));

        m.clear(g);

        assert_eq!(0, m.len(g));
        assert!(m.is_empty(g));
    }
}
