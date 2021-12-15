use std::borrow::{Borrow, BorrowMut};
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cmp, fmt, mem};

use crossbeam_epoch::Guard;
use crossbeam_utils::atomic::AtomicConsume;

const ADDRESS_BITS: usize = 48;
const ADDRESS_MASK: u64 = (1 << ADDRESS_BITS) - 1;
const TAG_MASK: u64 = u64::MAX - ADDRESS_MASK;

#[inline(always)]
fn high_bits_tag(tag: u16) -> u64 {
    (tag as u64) << ADDRESS_BITS
}

#[inline(always)]
fn compose_tag_to_ptr(tag: u16, ptr: usize) -> u64 {
    ((tag as u64) << ADDRESS_BITS) + (ptr as u64 & ADDRESS_MASK)
}

#[inline(always)]
fn decompose_tag_from_ptr(ptr: u64) -> (u16, usize) {
    ((ptr >> ADDRESS_BITS) as u16, (ptr & ADDRESS_MASK) as usize)
}

#[inline(always)]
fn ptr_equals(data: u64, ptr: usize) -> bool {
    ptr == (data & ADDRESS_MASK) as usize
}

#[inline(always)]
fn tag_equals(data: u64, tag: u16) -> bool {
    tag == (data >> ADDRESS_BITS) as u16
}

#[inline]
unsafe fn into_raw_ptr<T>(b: Box<T>) -> usize {
    // Takes the ownership of the key and convert it into a raw pointer.
    let key_ptr = Box::into_raw(b);

    debug_assert!((key_ptr as u64) < (1 << ADDRESS_BITS));

    // Convert into usize.
    key_ptr as usize
}

#[inline]
unsafe fn deref<'a, T>(ptr: usize) -> &'a T {
    &*(ptr as *const T)
}

#[inline]
unsafe fn deref_mut<'a, T>(ptr: usize) -> &'a mut T {
    &mut *(ptr as *mut T)
}

#[inline]
unsafe fn from_raw_ptr<T>(ptr: usize) -> Box<T> {
    Box::from_raw(ptr as *mut T)
}

pub trait Pointer<T> {
    fn into_u64(self) -> u64;

    unsafe fn from_u64(data: u64) -> Self;
}

pub struct Atomic<T> {
    data: AtomicU64,
    _marker: PhantomData<T>,
}

pub struct CompareExchangeError<'g, T, P: Pointer<T>> {
    /// The value in the atomic pointer at the time of the failed operation.
    pub current: Shared<'g, T>,

    /// The new value, which the operation failed to store.
    pub new: P,
}

impl<T> Atomic<T> {
    #[inline]
    fn from_u64(data: u64) -> Self {
        Self {
            data: AtomicU64::new(data),
            _marker: PhantomData::default(),
        }
    }

    #[inline(always)]
    fn new_with_raw_ptr_and_tag(raw_ptr: usize, tag: u16) -> Atomic<T> {
        Self::from_u64(compose_tag_to_ptr(tag, raw_ptr))
    }

    pub fn null() -> Atomic<T> {
        Self::from_u64(0)
    }

    pub fn null_with_tag(tag: u16) -> Atomic<T> {
        Self::from_u64(compose_tag_to_ptr(tag, 0))
    }

    fn from_box(b: Box<T>, tag: u16) -> Self {
        Self::new_with_raw_ptr_and_tag(unsafe { into_raw_ptr(b) }, tag)
    }

    pub fn new(x: T) -> Atomic<T> {
        Self::from_box(Box::new(x), 0)
    }

    pub fn new_with_tag(x: T, tag: u16) -> Atomic<T> {
        Self::from_box(Box::new(x), tag)
    }

    pub fn load<'g>(&self, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.load(ord)) }
    }

    pub fn load_consume<'g>(&self, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.load_consume()) }
    }

    pub fn store<P: Pointer<T>>(&self, new: P, ord: Ordering) {
        self.data.store(new.into_u64(), ord)
    }

    pub fn swap<'g, P: Pointer<T>>(&self, new: P, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.swap(new.into_u64(), ord)) }
    }

    pub fn compare_exchange<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: Pointer<T>,
    {
        let new = new.into_u64();
        self.data
            .compare_exchange(current.into_u64(), new, success, failure)
            .map(|_| unsafe { Shared::from_u64(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_u64(current),
                    new: P::from_u64(new),
                }
            })
    }

    pub fn compare_exchange_weak<'g, P>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, P>>
    where
        P: Pointer<T>,
    {
        let new = new.into_u64();
        self.data
            .compare_exchange_weak(current.into_u64(), new, success, failure)
            .map(|_| unsafe { Shared::from_u64(new) })
            .map_err(|current| unsafe {
                CompareExchangeError {
                    current: Shared::from_u64(current),
                    new: P::from_u64(new),
                }
            })
    }

    pub fn fetch_and<'g>(&self, tag: u16, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.fetch_and(high_bits_tag(tag), ord)) }
    }

    pub fn fetch_or<'g>(&self, tag: u16, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.fetch_or(high_bits_tag(tag), ord)) }
    }

    pub fn fetch_xor<'g>(&self, tag: u16, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.fetch_xor(high_bits_tag(tag), ord)) }
    }

    pub fn fetch_add<'g>(&self, tag_delta: u16, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.fetch_add(high_bits_tag(tag_delta), ord)) }
    }

    pub fn fetch_sub<'g>(&self, tag_delta: u16, ord: Ordering, _: &'g Guard) -> Shared<'g, T> {
        unsafe { Shared::from_u64(self.data.fetch_sub(high_bits_tag(tag_delta), ord)) }
    }

    pub unsafe fn into_owned(self) -> Owned<T> {
        Owned::from_u64(self.data.into_inner())
    }

    pub fn into_u64(self) -> u64 {
        self.data.into_inner()
    }
}

impl<T> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (tag, raw) = decompose_tag_from_ptr(data);

        f.debug_struct("Atomic")
            .field("tag", &tag)
            .field("raw", &raw)
            .finish()
    }
}

impl<T> fmt::Pointer for Atomic<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let data = self.data.load(Ordering::SeqCst);
        let (_, raw) = decompose_tag_from_ptr(data);
        fmt::Pointer::fmt(&(unsafe { deref::<T>(raw) as *const _ }), f)
    }
}

impl<T> Clone for Atomic<T> {
    fn clone(&self) -> Self {
        let data = self.data.load(Ordering::Relaxed);
        Atomic::from_u64(data)
    }
}

impl<T> Default for Atomic<T> {
    fn default() -> Self {
        Atomic::null()
    }
}

impl<T> From<Owned<T>> for Atomic<T> {
    fn from(owned: Owned<T>) -> Self {
        let data = owned.data;
        mem::forget(owned);
        Self::from_u64(data)
    }
}

impl<T> From<Box<T>> for Atomic<T> {
    fn from(b: Box<T>) -> Self {
        Self::from_box(b, 0)
    }
}

impl<T> From<T> for Atomic<T> {
    fn from(t: T) -> Self {
        Self::new(t)
    }
}

impl<'g, T> From<Shared<'g, T>> for Atomic<T> {
    fn from(ptr: Shared<'g, T>) -> Self {
        Self::from_u64(ptr.data)
    }
}

impl<T> From<*const T> for Atomic<T> {
    fn from(raw: *const T) -> Self {
        Self::from_u64(raw as u64)
    }
}

pub struct Shared<'g, T: 'g + ?Sized> {
    data: u64,
    _marker: PhantomData<(&'g (), T)>,
}

impl<T> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            _marker: PhantomData,
        }
    }
}

impl<T> Copy for Shared<'_, T> {}

impl<T> Pointer<T> for Shared<'_, T> {
    #[inline]
    fn into_u64(self) -> u64 {
        self.data
    }

    #[inline]
    unsafe fn from_u64(data: u64) -> Self {
        Self {
            data,
            _marker: Default::default(),
        }
    }
}

impl<'g, T> Shared<'g, T> {
    pub fn as_raw(&self) -> *const T {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        raw as *const _
    }

    pub fn null() -> Shared<'g, T> {
        Shared {
            data: 0,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        ptr_equals(self.data, 0)
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        self.data == 0
    }

    pub unsafe fn deref(&self) -> &'g T {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        deref(raw)
    }

    pub unsafe fn deref_mut(&mut self) -> &'g mut T {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        deref_mut(raw)
    }

    pub unsafe fn as_ref(&self) -> Option<&'g T> {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        if raw == 0 {
            None
        } else {
            Some(deref(raw))
        }
    }

    #[inline(always)]
    pub fn tag(&self) -> u16 {
        let (tag, _) = decompose_tag_from_ptr(self.data);
        tag
    }

    #[inline(always)]
    pub fn tag_equals(&self, tag: u16) -> bool {
        tag_equals(self.data, tag)
    }

    #[inline]
    pub fn with_tag(&self, tag: u16) -> Shared<'g, T> {
        let (_, ptr) = decompose_tag_from_ptr(self.data);
        unsafe { Self::from_u64(compose_tag_to_ptr(tag, ptr)) }
    }

    pub unsafe fn into_owned(self) -> Owned<T> {
        debug_assert!(!self.is_null(), "converting a null `Shared` into `Owned`");
        Owned::from_u64(self.data)
    }

    pub fn defer_destroy(self, g: &'g Guard) {
        unsafe { g.defer_unchecked(move || self.into_owned()) }
    }
}

impl<T> From<*const T> for Shared<'_, T> {
    fn from(raw: *const T) -> Self {
        let raw = raw as u64;
        unsafe { Self::from_u64(raw) }
    }
}

impl<'g, T> PartialEq<Shared<'g, T>> for Shared<'g, T> {
    fn eq(&self, other: &Shared<'g, T>) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for Shared<'_, T> {}

impl<'g, T> PartialOrd<Shared<'g, T>> for Shared<'g, T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.data.partial_cmp(&other.data)
    }
}

impl<T> Ord for Shared<'_, T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl<T> fmt::Debug for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (tag, raw) = decompose_tag_from_ptr(self.data);

        f.debug_struct("Shared")
            .field("tag", &tag)
            .field("raw", &raw)
            .finish()
    }
}

impl<T> fmt::Pointer for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Pointer::fmt(&(unsafe { self.deref() as *const _ }), f)
    }
}

impl<T> Default for Shared<'_, T> {
    fn default() -> Self {
        Shared::null()
    }
}

pub struct Owned<T> {
    data: u64,
    _marker: PhantomData<Box<T>>,
}

impl<T> Pointer<T> for Owned<T> {
    #[inline]
    fn into_u64(self) -> u64 {
        let data = self.data;
        mem::forget(self);
        data
    }

    #[inline]
    unsafe fn from_u64(data: u64) -> Self {
        debug_assert!(data != 0, "converting zero into `Owned`");
        Owned {
            data,
            _marker: PhantomData,
        }
    }
}

impl<T> Owned<T> {
    pub unsafe fn from_raw(raw: *mut T) -> Owned<T> {
        let raw = raw as u64;
        Self::from_u64(raw)
    }

    pub fn into_box(self) -> Box<T> {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        mem::forget(self);
        unsafe { from_raw_ptr(raw) }
    }

    pub fn new(x: T) -> Owned<T> {
        unsafe { Self::from_u64(into_raw_ptr(Box::new(x)) as u64) }
    }

    pub fn into_shared(self, _: &Guard) -> Shared<T> {
        unsafe { Shared::from_u64(self.into_u64()) }
    }

    pub fn tag(&self) -> u16 {
        let (tag, _) = decompose_tag_from_ptr(self.data);
        tag
    }

    pub fn with_tag(self, tag: u16) -> Owned<T> {
        let data = self.into_u64();
        unsafe { Self::from_u64(compose_tag_to_ptr(tag, data as usize)) }
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        if raw > 0 {
            unsafe { drop(from_raw_ptr::<T>(raw)) }
        }
    }
}

impl<T> fmt::Debug for Owned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (tag, raw) = decompose_tag_from_ptr(self.data);

        f.debug_struct("Owned")
            .field("tag", &tag)
            .field("raw", &raw)
            .finish()
    }
}

impl<T: Clone> Clone for Owned<T> {
    fn clone(&self) -> Self {
        Owned::new((**self).clone()).with_tag(self.tag())
    }
}

impl<T> Deref for Owned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        unsafe { deref(raw) }
    }
}

impl<T> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        let (_, raw) = decompose_tag_from_ptr(self.data);
        unsafe { deref_mut(raw) }
    }
}

impl<T> From<T> for Owned<T> {
    fn from(t: T) -> Self {
        Owned::new(t)
    }
}

impl<T> From<Box<T>> for Owned<T> {
    fn from(b: Box<T>) -> Self {
        unsafe { Self::from_raw(Box::into_raw(b)) }
    }
}

impl<T> Borrow<T> for Owned<T> {
    fn borrow(&self) -> &T {
        self.deref()
    }
}

impl<T> BorrowMut<T> for Owned<T> {
    fn borrow_mut(&mut self) -> &mut T {
        self.deref_mut()
    }
}

impl<T> AsRef<T> for Owned<T> {
    fn as_ref(&self) -> &T {
        self.deref()
    }
}

impl<T> AsMut<T> for Owned<T> {
    fn as_mut(&mut self) -> &mut T {
        self.deref_mut()
    }
}

#[cfg(all(test))]
mod tests {
    use std::borrow::Borrow;
    use std::ops::Deref;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use std::{mem, thread};

    use crossbeam_epoch as epoch;
    use crossbeam_epoch::{Collector, LocalHandle};
    use rand::Rng;

    use crate::epoch::{Atomic, Owned, Shared};

    #[test]
    fn valid_tag_i8() {
        let p = Shared::<i8>::null().with_tag(0);
        dbg!(p);
    }

    #[test]
    fn valid_tag_i64() {
        let p = Shared::<i64>::null().with_tag(7);
        dbg!(p);
    }

    #[test]
    fn drop() {
        let guard = epoch::pin();

        let p = Atomic::<u64>::new(100);
        dbg!(&p);

        let sp = p.load(Ordering::Relaxed, &guard);
        dbg!(&sp);

        let mut sp = sp.with_tag(10);
        dbg!(&sp);

        unsafe {
            *(sp.deref_mut()) += 10;
        }

        let op = unsafe {
            let op = sp.into_owned();
            dbg!(&op);
            op
        };
        let &x = op.deref();
        dbg!(x);

        mem::drop(op);

        dbg!(sp);
    }

    static mut DEFER_CNT: AtomicUsize = AtomicUsize::new(0);

    fn worker(a: &Atomic<AtomicUsize>, handle: LocalHandle) -> usize {
        let mut rng = rand::thread_rng();
        let mut sum = 0;

        if rng.gen() {
            thread::sleep(Duration::from_millis(1));
        }
        let timeout = Duration::from_millis(rng.gen_range(0..10));
        let now = Instant::now();

        while now.elapsed() < timeout {
            for _ in 0..100 {
                let guard = &handle.pin();
                guard.flush();
                guard.defer(|| unsafe {
                    DEFER_CNT.fetch_add(1, Ordering::AcqRel);
                });

                let val = if rng.gen() {
                    let p = a.swap(Owned::new(AtomicUsize::new(sum)), Ordering::AcqRel, guard);
                    unsafe {
                        guard.defer_unchecked(move || p.into_owned());
                        guard.flush();
                        p.deref().load(Ordering::Relaxed)
                    }
                } else {
                    let p = a.load(Ordering::Acquire, guard);
                    unsafe { p.deref().fetch_add(sum, Ordering::Relaxed) }
                };

                sum = sum.wrapping_add(val);
            }
        }

        sum
    }

    #[test]
    fn it_works() {
        for _ in 0..100 {
            let collector = Collector::new();
            let a = Arc::new(Atomic::new(AtomicUsize::new(777)));
            let h = collector.register();
            let g = h.pin();
            dbg!(a.deref());
            unsafe { dbg!(a.deref().load(Ordering::Relaxed, &g).deref()) };

            let threads = (0..16)
                .map(|_| {
                    let a = a.clone();
                    let c = collector.clone();
                    thread::spawn(move || worker(a.borrow(), c.register()))
                })
                .collect::<Vec<_>>();

            for t in threads {
                t.join().unwrap();
            }

            unsafe {
                dbg!(a.deref());
                dbg!(a.deref().load(Ordering::Relaxed, &g).deref());

                a.swap(Shared::null(), Ordering::AcqRel, epoch::unprotected())
                    .into_owned();
            }
        }
    }
}
