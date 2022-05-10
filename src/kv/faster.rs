use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_epoch::Guard;

use crate::kv::bucket::BucketChain;

pub struct FasterKV<K, V, S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    state: S,
    // Buckets.
    buckets: Vec<BucketChain<K, V>>,
    // Bucket size equals to (1 << bucket_k).
    bucket_k: usize,
    // Bucket size.
    bucket_size: usize,
    // Number of elements.
    size: AtomicU64,
}

impl<K, V, S> FasterKV<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    pub fn new(bucket_k: usize) -> Self {
        assert!(bucket_k <= 32, "bucket_k must be not larger than 32");

        let bucket_size = 1 << bucket_k;
        let mut buckets = Vec::with_capacity(bucket_size);
        buckets.resize_with(bucket_size, || BucketChain::new());

        Self {
            state: Default::default(),
            bucket_k,
            bucket_size,
            buckets,
            size: AtomicU64::new(0),
        }
    }
}

impl<K, V, S> Default for FasterKV<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn default() -> Self {
        Self::new(20)
    }
}

impl<K, V, S> FasterKV<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn tag(&self, hash: u64) -> u16 {
        ((hash >> (64 - 15 - self.bucket_k)) as u16) & 0x7f
    }

    fn bucket(&self, hash: u64) -> &BucketChain<K, V> {
        let index = (hash >> (64 - self.bucket_k)) as usize;
        &self.buckets[index]
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.state.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn insert(&self, key: K, value: V, g: &Guard) -> bool {
        let hash = self.hash(&key);
        let bucket = self.bucket(hash);
        match bucket.head().insert(self.tag(hash), key, value, g) {
            true => {
                self.size.fetch_add(1, Ordering::Relaxed);
                true
            }
            false => false,
        }
    }

    pub fn remove(&self, key: K, g: &Guard) -> bool {
        let hash = self.hash(&key);
        let bucket = self.bucket(hash);
        match bucket.head().delete(self.tag(hash), key, g) {
            true => {
                self.size.fetch_sub(1, Ordering::Relaxed);
                true
            }
            false => false,
        }
    }

    pub fn contains<Q>(&self, key: Q, g: &Guard) -> bool
    where
        Q: Borrow<K>,
    {
        self.get_fn(key, |_| (), g) != None
    }

    pub fn get_fn<Q, F, U>(&self, key: Q, f: F, g: &Guard) -> Option<U>
    where
        Q: Borrow<K>,
        F: Fn(&V) -> U,
    {
        let hash = self.hash(key.borrow());
        let bucket = self.bucket(hash);
        bucket.head().get_fn(self.tag(hash), key, f, g)
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    pub fn view<'g>(&self, g: &'g Guard) -> View<'_, 'g, K, V, S> {
        View { kv: self, g }
    }
}

pub struct View<'f, 'g, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    kv: &'f FasterKV<K, V, S>,
    g: &'g Guard,
}

impl<'f, 'g, K, V, S> View<'f, 'g, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    #[inline]
    pub fn insert(&self, key: K, value: V) -> bool {
        self.kv.insert(key, value, self.g)
    }

    #[inline]
    pub fn remove(&self, key: K) -> bool {
        self.kv.remove(key, self.g)
    }

    #[inline]
    pub fn contains<Q>(&self, key: Q) -> bool
    where
        Q: Borrow<K>,
    {
        self.kv.contains(key, self.g)
    }

    #[inline]
    pub fn get_fn<Q, F, U>(&self, key: Q, f: F) -> Option<U>
    where
        Q: Borrow<K>,
        F: Fn(&V) -> U,
    {
        self.kv.get_fn(key, f, self.g)
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.kv.size()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.kv.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::collections::HashMap;
    use std::fs::File;
    use std::hash::{BuildHasher, Hash};
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    use crossbeam_epoch::{self as epoch, Guard, LocalHandle};
    use crossbeam_utils::atomic::AtomicConsume;
    use rand::rngs::SmallRng;
    use rand::{RngCore, SeedableRng};

    use crate::kv::faster::FasterKV;

    const DATA_LEN: u64 = 4u64 << 20;
    const ITER_PER_THREAD: usize = 5000000;
    const BUCKET_K: usize = 20;
    const THREAD_NUM: u64 = 4;

    fn report_faster_kv_stats<K, V, S>(kv: &FasterKV<K, V, S>, g: &Guard)
    where
        K: Eq + Hash,
        S: BuildHasher + Default,
    {
        eprintln!("Bucket K: {}", kv.bucket_k);
        eprintln!("Buckets slots: {}", kv.bucket_size);
        eprintln!("Size: {}", kv.size());
        let total_buckets = kv.buckets.iter().map(|bc| bc.len()).sum::<usize>();
        eprintln!("Total buckets: {}", total_buckets);
        let total_entries: usize = kv
            .buckets
            .iter()
            .map(|bc| {
                let entry_size: usize = bc.iter().map(|b| b.entry_size(g)).sum();
                entry_size
            })
            .sum();
        eprintln!("Total entries: {}", total_entries);
        let empty_entries: usize = kv
            .buckets
            .iter()
            .map(|bc| {
                let empty_entries: usize = bc
                    .iter()
                    .map(|b| {
                        b.entries()
                            .iter()
                            .filter(|e| !e.head().load_consume(g).is_null() && e.len(g) == 0)
                            .count()
                    })
                    .sum();
                empty_entries
            })
            .sum();
        eprintln!(
            "Empty entries rate: {:.4}",
            empty_entries as f64 / total_entries as f64
        );
        let total_linked_list_length: usize = kv
            .buckets
            .iter()
            .map(|bc| {
                let entry_len: usize = bc
                    .iter()
                    .map(|b| b.entries().iter().map(|e| e.list_len(g)).sum::<usize>())
                    .sum();
                entry_len
            })
            .sum();
        eprintln!("Total linked list length: {}", total_linked_list_length);
        eprintln!(
            "Avg bucket overflow rate: {:.4}",
            (total_buckets as f64 / kv.bucket_size as f64) - 1.0
        );
        eprintln!(
            "Avg entry occupation rate: {:.4}",
            total_entries as f64 / (total_buckets * 7) as f64
        );
        eprintln!(
            "Avg linked list length: {:.4}",
            total_linked_list_length as f64 / (total_entries as f64)
        );
        let max_linked_list_length: usize = kv
            .buckets
            .iter()
            .map(|bc| {
                let max_entry_len: usize = bc
                    .iter()
                    .map(|b| b.entries().iter().map(|e| e.list_len(g)).max().unwrap())
                    .max()
                    .unwrap();
                max_entry_len
            })
            .max()
            .unwrap();
        eprintln!("Max linked list length: {}", max_linked_list_length);
        let max_linked_set_length: usize = kv
            .buckets
            .iter()
            .map(|bc| {
                let max_entry_len: usize = bc
                    .iter()
                    .map(|b| b.entries().iter().map(|e| e.len(g)).max().unwrap())
                    .max()
                    .unwrap();
                max_entry_len
            })
            .max()
            .unwrap();
        eprintln!("Max linked size length: {}", max_linked_set_length);
    }

    fn run_and_print_ops<F>(f: F, total_ops: u64)
    where
        F: Fn(),
    {
        // let guard = pprof::ProfilerGuardBuilder::default()
        //     .frequency(1000)
        //     .blocklist(&["libc", "libgcc", "pthread"])
        //     .build()
        //     .unwrap();

        let time_start = Instant::now();
        dbg!(time_start);
        f();
        let elapsed = time_start.elapsed();
        let ops_per_ms = total_ops as f64 / elapsed.as_secs_f64();
        eprintln!("{:?} op/s", ops_per_ms);

        // if let Ok(report) = guard.report().build() {
        //     let file = File::create("/tmp/flamegraph.svg").unwrap();
        //     report.flamegraph(file).unwrap();
        // };
    }

    fn random_read_write_default_hash() {
        let mut rng = SmallRng::from_entropy();

        let mut kv = HashMap::new();

        const ITER_LEN: usize = ITER_PER_THREAD;
        for i in 0..ITER_LEN {
            let x = rng.next_u64() & (DATA_LEN - 1);
            let dice = rng.next_u64() % 3;
            if dice < 1 {
                kv.insert(x, x);
            } else if dice < 2 {
                kv.remove(&x);
            } else if let Some(y) = kv.get(&x) {
                assert_eq!(x, *y)
            }
        }
    }

    fn random_read_write(kv: &FasterKV<u64, u64>, lh: Rc<LocalHandle>) {
        let mut rng = SmallRng::from_entropy();

        const ITER_LEN: usize = ITER_PER_THREAD;
        for i in 0..ITER_LEN {
            let x = rng.next_u64() & (DATA_LEN - 1);
            let dice = rng.next_u64() % 3;
            let g = &lh.pin();
            let view = kv.view(g);
            if dice < 1 {
                view.insert(x, x);
            } else if dice < 2 {
                view.remove(x);
            } else if let Some(y) = view.get_fn(x, |v| *v) {
                assert_eq!(x, y)
            }
        }
    }

    fn pure_rmw_init(kv: &FasterKV<u64, AtomicU64>) {
        let g = unsafe { epoch::unprotected() };
        let view = kv.view(g);
        for i in 0..DATA_LEN {
            view.insert(i, AtomicU64::new(0));
        }
        assert_eq!(DATA_LEN, kv.size());
    }

    fn pure_rmw(kv: &FasterKV<u64, AtomicU64>, lh: Rc<LocalHandle>) {
        let mut rng = SmallRng::from_entropy();

        const ITER_LEN: usize = ITER_PER_THREAD;
        for i in 0..ITER_LEN {
            let x = rng.next_u64() & (2 * DATA_LEN - 1);
            let g = &lh.pin();
            let view = kv.view(g);
            assert!(if x >= DATA_LEN {
                view.get_fn(x, |v| v.fetch_add(1, Ordering::Relaxed))
                    .is_none()
            } else {
                view.get_fn(x, |v| v.fetch_add(1, Ordering::Relaxed))
                    .is_some()
            });
        }
    }

    fn pure_read(kv: &FasterKV<u64, AtomicU64>, lh: Rc<LocalHandle>) {
        let mut rng = SmallRng::from_entropy();

        const ITER_LEN: usize = ITER_PER_THREAD;
        for i in 0..ITER_LEN {
            let x = rng.next_u64() & (2 * DATA_LEN - 1);
            let g = &lh.pin();
            let view = kv.view(g);
            assert!(if x >= DATA_LEN {
                view.get_fn(x, |v| v.load_consume()).is_none()
            } else {
                view.get_fn(x, |v| v.load_consume()).is_some()
            });
        }
    }

    #[test]
    fn test_default_hash_map_in_single_thread() {
        run_and_print_ops(random_read_write_default_hash, ITER_PER_THREAD as u64);
    }

    #[test]
    fn test_faster_kv_in_single_thread() {
        let collector = epoch::Collector::new();
        let kv = FasterKV::new(BUCKET_K);
        run_and_print_ops(
            || {
                random_read_write(&kv, Rc::new(collector.register()));
            },
            ITER_PER_THREAD as u64,
        );
        report_faster_kv_stats(&kv, unsafe { epoch::unprotected() })
    }

    #[test]
    fn test_faster_kv_in_parallel() {
        let collector = epoch::Collector::new();
        let kv = Arc::new(FasterKV::new(BUCKET_K));

        run_and_print_ops(
            || {
                let threads = (0..THREAD_NUM)
                    .map(|_| {
                        let kv = kv.clone();
                        let c = collector.clone();
                        thread::spawn(move || {
                            let lh = Rc::new(c.register());
                            random_read_write(kv.borrow(), lh.clone());
                            lh.pin().flush();
                        })
                    })
                    .collect::<Vec<_>>();

                for t in threads {
                    t.join().unwrap();
                }
            },
            ITER_PER_THREAD as u64 * THREAD_NUM,
        );

        report_faster_kv_stats(&kv, unsafe { epoch::unprotected() })
    }

    #[test]
    fn test_faster_kv_pure_rmw_single() {
        let collector = epoch::Collector::new();
        let kv = Arc::new(FasterKV::new(BUCKET_K));

        pure_rmw_init(kv.borrow());

        run_and_print_ops(
            || {
                pure_rmw(kv.borrow(), Rc::new(collector.register()));
            },
            ITER_PER_THREAD as u64,
        );

        report_faster_kv_stats(&kv, unsafe { epoch::unprotected() })
    }

    #[test]
    fn test_faster_kv_pure_rmw_in_parallel() {
        let collector = epoch::Collector::new();
        let kv = Arc::new(FasterKV::new(BUCKET_K));

        pure_rmw_init(kv.borrow());

        run_and_print_ops(
            || {
                let threads = (0..THREAD_NUM)
                    .map(|_| {
                        let kv = kv.clone();
                        let c = collector.clone();
                        thread::spawn(move || {
                            pure_rmw(kv.borrow(), Rc::new(c.register()));
                        })
                    })
                    .collect::<Vec<_>>();

                for t in threads {
                    t.join().unwrap();
                }
            },
            ITER_PER_THREAD as u64 * THREAD_NUM,
        );

        report_faster_kv_stats(&kv, unsafe { epoch::unprotected() })
    }

    #[test]
    fn test_faster_kv_pure_read_single() {
        let collector = epoch::Collector::new();
        let kv = Arc::new(FasterKV::new(BUCKET_K));

        pure_rmw_init(kv.borrow());

        run_and_print_ops(
            || {
                pure_read(kv.borrow(), Rc::new(collector.register()));
            },
            ITER_PER_THREAD as u64,
        );

        report_faster_kv_stats(&kv, unsafe { epoch::unprotected() })
    }

    #[test]
    fn test_faster_kv_pure_read_in_parallel() {
        let collector = epoch::Collector::new();
        let kv = Arc::new(FasterKV::new(BUCKET_K));

        pure_rmw_init(kv.borrow());

        run_and_print_ops(
            || {
                let threads = (0..THREAD_NUM)
                    .map(|_| {
                        let kv = kv.clone();
                        let c = collector.clone();
                        thread::spawn(move || {
                            pure_read(kv.borrow(), Rc::new(c.register()));
                        })
                    })
                    .collect::<Vec<_>>();

                for t in threads {
                    t.join().unwrap();
                }
            },
            ITER_PER_THREAD as u64 * THREAD_NUM,
        );

        report_faster_kv_stats(&kv, unsafe { epoch::unprotected() })
    }
}
