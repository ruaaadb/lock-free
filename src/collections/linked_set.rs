use std::borrow::Borrow;

use crossbeam_epoch::Guard;

use crate::collections::{IntoIter, Iter, LinkedList};

pub struct LinkedSet<E> {
    list: LinkedList<E>,
}

impl<E> LinkedSet<E> {
    pub fn new() -> Self {
        Self {
            list: LinkedList::new(),
        }
    }
}

impl<E> Default for LinkedSet<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> LinkedSet<E>
where
    E: Eq,
{
    pub fn is_empty(&self, g: &Guard) -> bool {
        self.list.is_empty(g)
    }

    pub fn contains<Q>(&self, elem: Q, g: &Guard) -> bool
    where
        Q: Borrow<E>,
    {
        self.list.contains(elem.borrow(), g)
    }

    pub fn insert<Q>(&self, elem: Q, g: &Guard) -> bool
    where
        Q: Into<E>,
    {
        self.list.insert(elem.into(), g)
    }

    pub fn remove<Q>(&self, elem: Q, g: &Guard) -> bool
    where
        Q: Into<E>,
    {
        self.list.delete(elem.into(), g)
    }

    pub fn len(&self, g: &Guard) -> usize {
        self.list.len(g)
    }

    pub fn clear(&self, g: &Guard) {
        self.list.clear(g)
    }

    pub fn retain<F>(&self, f: F, g: &Guard)
    where
        F: Fn(&E) -> bool,
        E: Clone,
    {
        for elem in self.list.iter(g) {
            if f(elem) {
                self.remove(elem.clone(), g);
            }
        }
    }

    pub fn iter<'f>(&self, g: &'f Guard) -> Iter<'f, E> {
        self.list.iter(g)
    }
}

impl<E> IntoIterator for LinkedSet<E> {
    type Item = E;
    type IntoIter = IntoIter<E>;

    fn into_iter(self) -> Self::IntoIter {
        self.list.into_iter()
    }
}

impl<E, T> From<T> for LinkedSet<E>
where
    T: Into<LinkedList<E>>,
{
    fn from(x: T) -> Self {
        Self { list: x.into() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    use crossbeam_epoch as epoch;
    use crossbeam_utils::atomic::AtomicConsume;
    use rand::Rng;

    use crate::collections::LinkedSet;

    #[test]
    fn test_set_works() {
        let s = LinkedSet::from(0u64..10);
        let g = unsafe { epoch::unprotected() };

        assert_eq!(10, s.len(g));

        for i in 0u64..10 {
            assert!(s.contains(i, g));
        }

        s.clear(g);

        assert!(s.is_empty(g));
        assert_eq!(0, s.len(g));
    }

    #[test]
    fn test_set_insert_and_delete() {
        let s = LinkedSet::from(0..10);
        let g = unsafe { epoch::unprotected() };

        assert_eq!(10, s.len(g));

        assert!(!s.insert(0, g));
        assert!(s.remove(0, g));
        assert_eq!(9, s.len(g));

        assert!(s.insert(10, g));
        assert_eq!(10, s.len(g));

        assert!(s.contains(10, g));
        assert!(!s.contains(0, g));
    }

    #[test]
    pub fn test_sequential_read_writes() {
        const KEY_LEN: u64 = 1000;
        const KEY_RANGE: u64 = 2000;
        const ITERATIONS: u64 = 10000000;
        const POINT_LOOK_UP_RATIO: u64 = 80;

        let mut rng = rand::thread_rng();

        let s = LinkedSet::from(0u64..KEY_LEN);
        let mut size: usize = 1000;
        let g = unsafe { epoch::unprotected() };
        for _ in 0..ITERATIONS {
            let key: u64 = rng.gen::<u64>() % KEY_RANGE;
            match rng.gen::<u64>() % 100 {
                0..=POINT_LOOK_UP_RATIO => {
                    s.contains(key, g);
                }
                _ => {
                    let x = rng.gen::<u64>() % 100;
                    if x < 49 {
                        if s.insert(key, g) {
                            size += 1;
                        }
                    } else if x < 98 {
                        if s.remove(key, g) {
                            size -= 1;
                        }
                    } else {
                        // s.clear(g);
                    }
                }
            }
        }

        dbg!(s.len(unsafe { epoch::unprotected() }));
        dbg!(s.list.list_len(unsafe { epoch::unprotected() }));
        assert_eq!(size, s.len(unsafe { epoch::unprotected() }));
    }

    #[test]
    pub fn test_parallel_read_writes() {
        const KEY_LEN: u64 = 1000;
        const KEY_RANGE: u64 = 2000;

        let collector = epoch::Collector::new();
        let s = Arc::new(LinkedSet::from(0u64..KEY_LEN));

        const THREAD_NUM: u64 = 8;
        const TIMES_PER_THREAD: u64 = 10000000;
        const POINT_LOOK_UP_RATIO: u64 = 80;

        let cnt = Arc::new(AtomicUsize::new(KEY_LEN as usize));
        let threads = (0..THREAD_NUM)
            .map(|_| {
                let s = s.clone();
                let c = collector.clone();
                let ct = cnt.clone();
                thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    let h = c.register();

                    for _ in 0..TIMES_PER_THREAD {
                        let g = &h.pin();

                        let key: u64 = rng.gen::<u64>() % KEY_RANGE;
                        match rng.gen::<u64>() % 100 {
                            0..=POINT_LOOK_UP_RATIO => {
                                s.contains(key, g);
                            }
                            _ => {
                                let x = rng.gen::<u64>() % 100;
                                if x < 49 {
                                    if s.insert(key, g) {
                                        ct.fetch_add(1, Ordering::Relaxed);
                                    }
                                } else if x < 98 {
                                    if s.remove(key, g) {
                                        ct.fetch_sub(1, Ordering::Relaxed);
                                    }
                                } else {
                                    // s.clear(g);
                                }
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join().unwrap();
        }

        dbg!(s.len(unsafe { epoch::unprotected() }));
        dbg!(s.list.list_len(unsafe { epoch::unprotected() }));
        dbg!(cnt.load_consume());
        assert_eq!(cnt.load_consume(), s.len(unsafe { epoch::unprotected() }))
    }
}
