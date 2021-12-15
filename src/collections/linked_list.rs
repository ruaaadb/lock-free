use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicU16, Ordering};

use crossbeam_epoch::{self as epoch, Guard};
use crossbeam_utils::atomic::AtomicConsume;

use crate::epoch::{Atomic, Owned, Pointer, Shared};

type State = u16;
type AtomicState = AtomicU16;

pub const STATE_VALID: State = 0x0;
pub const STATE_INSERTING: State = 0x1;
pub const STATE_REMOVING: State = 0x2;
pub const STATE_INVALID: State = 0x4;

pub const MARK_DELETING: u16 = 0x1;

#[inline(always)]
fn is_ptr_marked<'f, T>(ptr: impl Borrow<Shared<'f, T>>) -> bool
where
    T: 'f,
{
    ptr.borrow().tag_equals(MARK_DELETING)
}

#[inline(always)]
fn mark_ptr<T>(ptr: Shared<T>) -> Shared<T> {
    ptr.with_tag(MARK_DELETING)
}

#[inline(always)]
fn unmark_ptr<T>(ptr: Shared<T>) -> Shared<T> {
    ptr.with_tag(0)
}

#[inline(always)]
pub fn is_invalid(state: State) -> bool {
    state & STATE_INVALID != 0
}

pub type AtomicNodePtr<E> = Atomic<Node<E>>;
pub type SharedNodePtr<'f, E> = Shared<'f, Node<E>>;

pub struct Node<E> {
    elem: E,
    state: AtomicState,
    next: AtomicNodePtr<E>,
}

impl<E> Node<E> {
    pub fn new(elem: E, state: State) -> Node<E> {
        Self {
            elem,
            state: AtomicState::from(state),
            next: Atomic::null(),
        }
    }
}

pub struct LinkedList<E> {
    head: AtomicNodePtr<E>,
}

impl<E> LinkedList<E> {
    pub fn new() -> Self {
        Self {
            head: Atomic::null(),
        }
    }

    pub fn new_with_head<T>(head: T) -> Self
    where
        T: Into<AtomicNodePtr<E>>,
    {
        Self { head: head.into() }
    }

    pub fn head(&self) -> &AtomicNodePtr<E> {
        &self.head
    }
}

impl<E> Default for LinkedList<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> LinkedList<E> {}

pub struct NodePtrIter<'f, E> {
    cur: SharedNodePtr<'f, E>,
    g: &'f Guard,
}

impl<'f, E> NodePtrIter<'f, E> {
    pub fn new(head: SharedNodePtr<'f, E>, g: &'f Guard) -> Self {
        Self { cur: head, g }
    }
}

impl<'f, E> Iterator for NodePtrIter<'f, E> {
    type Item = SharedNodePtr<'f, E>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.cur.is_null() {
            let cur = self.cur;
            let node = unsafe { cur.deref() };
            self.cur = node.next.load(Ordering::Acquire, self.g);
            Some(cur)
        } else {
            None
        }
    }
}

pub struct NodeIter<'f, E> {
    prev: SharedNodePtr<'f, E>,
    cur: SharedNodePtr<'f, E>,
    g: &'f Guard,
}

impl<'f, E> NodeIter<'f, E> {
    fn new(head: SharedNodePtr<'f, E>, g: &'f Guard) -> Self {
        Self {
            prev: Shared::null(),
            cur: head,
            g,
        }
    }

    fn from_next(prev: SharedNodePtr<'f, E>, g: &'f Guard) -> Self {
        debug_assert!(!prev.is_null());
        let prev_node = unsafe { prev.deref() };
        Self {
            prev,
            cur: prev_node.next.load(Ordering::Acquire, g),
            g,
        }
    }
}

impl<'f, E> Iterator for NodeIter<'f, E> {
    type Item = (&'f Node<E>, State);

    fn next(&mut self) -> Option<Self::Item> {
        while !self.cur.is_null() {
            let cur_node = unsafe { self.cur.deref() };
            let state = cur_node.state.load(Ordering::Acquire);
            if !is_invalid(state) {
                self.prev = self.cur;
                self.cur = cur_node.next.load(Ordering::Acquire, self.g);
                return Some((cur_node, state));
            } else if !self.prev.is_null() {
                let prev_node = unsafe { self.prev.deref() };
                let next =
                    LinkedList::delete_current_node(prev_node, cur_node, self.cur, state, self.g);
                self.prev = Shared::from(next.0 as *const Node<E>);
                self.cur = next.1;
            } else {
                self.prev = self.cur;
                self.cur = cur_node.next.load(Ordering::Acquire, self.g);
            }
        }

        None
    }
}

pub struct Iter<'f, E> {
    iter: NodeIter<'f, E>,
}

impl<'f, E> Iter<'f, E> {
    fn new(head: SharedNodePtr<'f, E>, g: &'f Guard) -> Self {
        Self {
            iter: NodeIter::new(head, g),
        }
    }

    fn from_next(prev: SharedNodePtr<'f, E>, g: &'f Guard) -> Self {
        Self {
            iter: NodeIter::from_next(prev, g),
        }
    }
}

impl<'f, E> Iterator for Iter<'f, E> {
    type Item = &'f E;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some((node, state)) => {
                    if state == STATE_VALID {
                        return Some(&node.elem);
                    }
                }
                None => return None,
            }
        }
    }
}

impl<E> LinkedList<E> {
    pub fn node_iter<'f>(&self, g: &'f Guard) -> NodeIter<'f, E> {
        NodeIter::new(self.head.load(Ordering::Acquire, g), g)
    }

    pub fn iter<'f>(&self, g: &'f Guard) -> Iter<'f, E> {
        Iter::new(self.head.load(Ordering::Acquire, g), g)
    }

    fn enlist(&self, node_ptr: SharedNodePtr<E>, g: &Guard) {
        debug_assert!(!node_ptr.is_null());
        let node = unsafe { node_ptr.deref() };
        let mut head_ptr = self.head.load(Ordering::Acquire, g);
        loop {
            // Store next as clean ptr.
            node.next.store(head_ptr.with_tag(0), Ordering::Release);
            match self.head.compare_exchange(
                head_ptr,
                // But keep head's tag.
                node_ptr.with_tag(head_ptr.tag()),
                Ordering::AcqRel,
                Ordering::Acquire,
                g,
            ) {
                Ok(_) => return,
                Err(err) => {
                    head_ptr = err.current;
                }
            }
        }
    }

    fn schedule_node_for_recycle(node_ptr: SharedNodePtr<E>, state: State, g: &Guard) {
        debug_assert!(is_invalid(state));
        unsafe {
            g.defer_unchecked(move || node_ptr.into_owned());
        }
    }

    fn mark_node_as_invalid(node: &Node<E>, state: State) -> bool {
        let mut state = state;
        while !is_invalid(state) {
            match node.state.compare_exchange(
                state,
                STATE_INVALID,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(s) => state = s,
            }
        }
        false
    }

    /// Try to remove the current node by setting the prev.next = cur.next via CAS.
    /// If CAS succeeds, it means that it's the first time current node is deleted
    /// from the list, so we just drop it. Otherwise, restart with the
    /// updated current node.
    ///
    /// Problems: concurrent deletions by threads on adjacent nodes?
    /// A -> B -> C -> D
    /// If thread 1 is trying to delete B and thread 2 is trying to delete
    /// C (with simple CAS), then it's possible that thread 1's operation discards
    /// what thread 2 does, i.e. make the deletion of C invalid.
    ///
    /// By here we use a two-phase protocol to avoid such problem:
    ///    If we want to delete a node, we must mark it as deleting on it's next pointer
    ///    by CAS(node.next, unmarked, marked).
    /// 1. First we check if A is marked as deleting, if true we skip the deletion of B.
    /// 2. Then we try to mark B as deleting, break if B is already marked.
    /// 2. Any thread observes a marked node (B) can try to delete it by
    ///    CAS(prev.next, unmarked(B), unmarked(B.next)), i.e. we disallow that deleting B
    ///    when A is also deleting.
    ///    2.1 If CAS succeeds, we have deleted B from the list. And it can be safely defer
    ///        collected by the epoch framework since it won't reappear in the list.
    ///    2.2 If CAS fails, either A is marked deleted or B is deleted by another thread,
    ///        we just abort the deletion and retry the scan.
    ///
    /// In such protocol, deleting B and C concurrently may leave C in the list
    /// (but marked deleting). It's ok since another process will try to delete it.
    fn delete_current_node<'f>(
        prev_node: &'f Node<E>,
        cur_node: &'f Node<E>,
        cur_ptr: SharedNodePtr<'f, E>,
        cur_state: State,
        g: &'f Guard,
    ) -> (&'f Node<E>, SharedNodePtr<'f, E>) {
        let mut succ_ptr = cur_node.next.load(Ordering::Acquire, g);

        // Abort if previous node is marked as deleting.
        if is_ptr_marked(cur_ptr) {
            return (cur_node, succ_ptr);
        }

        // Mark the current node as deleting.
        while !is_ptr_marked(succ_ptr) {
            match cur_node.next.compare_exchange(
                succ_ptr,
                mark_ptr(succ_ptr),
                Ordering::AcqRel,
                Ordering::Acquire,
                g,
            ) {
                Err(err) => {
                    succ_ptr = err.current;
                }
                Ok(_) => {
                    break;
                }
            }
        }

        // Already marked, by self or other threads.
        // Going to delete the current node by CAS(prev.next, unmasked(cur), unmasked(succ)).
        // If succeeds, then the current node is deleted.
        // If fails, either the prev.next is marked as deleting or prev.next is changed.
        // We just retry the loaded current node if fails.
        match prev_node.next.compare_exchange(
            unmark_ptr(cur_ptr),
            unmark_ptr(succ_ptr),
            Ordering::AcqRel,
            Ordering::Acquire,
            g,
        ) {
            Ok(_) => {
                Self::schedule_node_for_recycle(cur_ptr, cur_state, g);
                (prev_node, unmark_ptr(succ_ptr))
            }
            Err(err) => (prev_node, err.current),
        }
    }

    fn process_insert_forward_iter<F>(&self, node_ptr: SharedNodePtr<E>, pred: F, g: &Guard) -> bool
    where
        F: Fn(&E) -> bool,
    {
        let mut iter = NodeIter::from_next(node_ptr, g);
        match iter.find(|(node, _)| pred(&node.elem)) {
            Some((_, state)) => state == STATE_REMOVING,
            None => true,
        }
    }

    fn process_insert_forward<F>(&self, node_ptr: SharedNodePtr<E>, pred: F, g: &Guard) -> bool
    where
        F: Fn(&E) -> bool,
    {
        debug_assert!(!node_ptr.is_null());

        let mut prev_node = unsafe { node_ptr.deref() };
        let mut cur_ptr = prev_node.next.load(Ordering::Acquire, g);

        while !cur_ptr.is_null() {
            let cur_node = unsafe { cur_ptr.deref() };
            let cur_state = cur_node.state.load(Ordering::Acquire);

            if is_invalid(cur_state) {
                let next = Self::delete_current_node(prev_node, cur_node, cur_ptr, cur_state, g);
                prev_node = next.0;
                cur_ptr = next.1;
                continue;
            } else if !pred(&cur_node.elem) {
                // Current node does not match, we chase the list
                // by loading the current node's next.

                // Problems: what if we are loading a deleting node?
                // It's possible that the next node's already be marked
                // with INV and being removed by another thread.
                // We load a being deleted ptr. And that thread will
                // compete with this one in the above block.

                prev_node = cur_node;
                cur_ptr = cur_node.next.load(Ordering::Acquire, g);
            } else if cur_state == STATE_REMOVING {
                // We've seen a matched node that is removed.
                // Thus we know the insert succeeds. The removed node
                // is going to be reclaimed.
                return true;
            } else if cur_state == STATE_INSERTING || cur_state == STATE_VALID {
                // Found another matched node already be inserting or visible.
                return false;
            } else {
                // There are no other states. Should never reach here.
                panic!("never reach here, tag: {:?}", cur_state)
            }
        }

        true
    }

    fn process_delete_forward<F>(&self, node_ptr: SharedNodePtr<E>, pred: F, g: &Guard) -> bool
    where
        F: Fn(&E) -> bool,
    {
        debug_assert!(!node_ptr.is_null());

        let mut prev_node = unsafe { node_ptr.deref() };
        let mut cur_ptr = prev_node.next.load(Ordering::Acquire, g);

        while !cur_ptr.is_null() {
            let cur_node = unsafe { cur_ptr.deref() };
            let cur_state = cur_node.state.load(Ordering::Acquire);

            if is_invalid(cur_state) {
                let next = Self::delete_current_node(prev_node, cur_node, cur_ptr, cur_state, g);
                prev_node = next.0;
                cur_ptr = next.1;
                continue;
            } else if !pred(&cur_node.elem) {
                prev_node = cur_node;
                cur_ptr = cur_node.next.load(Ordering::Acquire, g);
            } else if cur_state == STATE_REMOVING {
                // We've seen a matched node that is removed.
                return false;
            } else if cur_state == STATE_INSERTING {
                // Try to CAS it to removing. If succeeds, we removed the
                // inserting node. Thus we removed successfully.
                // The thread that inserting this node will find it changed
                // and process the removing then.
                if cur_node
                    .state
                    .compare_exchange(
                        STATE_INSERTING,
                        STATE_REMOVING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return true;
                }
                // Otherwise, just try again.
            } else if cur_state == STATE_VALID {
                // Found a valid node, try to invalidate it. Threads can invalidate
                // a node at the same time, but only the one who does the CAS successfully
                // return true.
                return Self::mark_node_as_invalid(cur_node, cur_state);
            } else {
                // There are no other states. Should never reach here.
                panic!("never reach here, tag: {:?}", cur_state)
            }
        }

        false
    }

    fn insert_f<F>(&self, elem: E, pred: F, g: &Guard) -> bool
    where
        F: Fn(&E, &E) -> bool,
    {
        let node_ptr = Owned::new(Node::new(elem, STATE_INSERTING));
        let node_ptr = node_ptr.into_shared(g);
        self.enlist(node_ptr, g);

        let node = unsafe { node_ptr.deref() };
        let pred = |e: &E| pred(&node.elem, e);

        let target_state = match self.process_insert_forward_iter(node_ptr, pred, g) {
            true => STATE_VALID,
            false => STATE_INVALID,
        };

        // CAS the state to VIS or INV. If fails, it means that another
        // thread has removed this (mark as REM), and we are going remove it.
        if let Err(state) = node.state.compare_exchange(
            STATE_INSERTING,
            target_state,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            self.process_delete_forward(node_ptr, pred, g);
            Self::mark_node_as_invalid(node, state);
        }

        target_state == STATE_VALID
    }

    fn delete_f<F>(&self, elem: E, pred: F, g: &Guard) -> bool
    where
        F: Fn(&E, &E) -> bool,
    {
        let node_ptr = Owned::new(Node::new(elem, STATE_REMOVING));
        let node_ptr = node_ptr.into_shared(g);
        self.enlist(node_ptr, g);

        let node = unsafe { node_ptr.deref() };
        let pred = |e: &E| pred(&node.elem, e);

        let result = self.process_delete_forward(node_ptr, pred, g);
        Self::mark_node_as_invalid(node, STATE_REMOVING);
        result
    }

    pub fn find_fn<F, U>(&self, f: F, g: &Guard) -> Option<U>
    where
        F: Fn(&E) -> Option<U>,
    {
        match self
            .node_iter(g)
            .filter(|(_, state)| *state == STATE_VALID)
            .map(|(node, _)| f(&node.elem))
            .find(|x| !matches!(x, None))
        {
            None => None,
            Some(x) => x,
        }
    }

    pub fn clear(&self, g: &Guard) {
        let head = self.head.swap(Shared::null(), Ordering::AcqRel, g);
        let garbage = LinkedList::new_with_head(head);
        unsafe {
            g.defer_unchecked(move || drop(garbage));
        }
    }

    pub fn list_len(&self, g: &Guard) -> usize {
        NodePtrIter::new(self.head.load(Ordering::Acquire, g), g).count()
    }
}

impl<E> LinkedList<E>
where
    E: Eq,
{
    pub fn contains(&self, elem: &E, g: &Guard) -> bool {
        self.node_iter(g).any(|(node, state)| {
            // Short circuit.
            if state != STATE_VALID && state != STATE_INSERTING {
                return false;
            }
            elem.eq(&node.elem)
        })
    }

    pub fn is_empty(&self, g: &Guard) -> bool {
        // Be consistent with contains.
        !self
            .node_iter(g)
            .any(|(_, state)| state == STATE_VALID || state == STATE_INSERTING)
    }

    pub fn len(&self, g: &Guard) -> usize {
        self.node_iter(g)
            .filter(|(_, state)| *state == STATE_VALID || *state == STATE_INSERTING)
            .count()
    }

    pub fn insert(&self, elem: E, g: &Guard) -> bool {
        self.insert_f(elem, |elem_ins, elem| elem_ins.eq(elem), g)
    }

    pub fn delete(&self, elem: E, g: &Guard) -> bool {
        self.delete_f(elem, |e1, e2| e1.eq(e2), g)
    }
}

impl<E> LinkedList<E> {
    pub fn contains_key<K>(&self, key: &K, g: &Guard) -> bool
    where
        E: PartialEq<K>,
    {
        self.node_iter(g).any(|(node, state)| {
            if state != STATE_VALID {
                return false;
            }
            node.elem.eq(key)
        })
    }

    pub fn is_empty_key(&self, g: &Guard) -> bool {
        // Be consistent with contains.
        !self.node_iter(g).any(|(node, state)| state == STATE_VALID)
    }

    pub fn len_key(&self, g: &Guard) -> usize {
        self.node_iter(g)
            .filter(|(_, state)| *state == STATE_VALID)
            .count()
    }

    pub fn insert_key<K>(&self, elem: E, g: &Guard) -> bool
    where
        E: PartialEq<K> + AsRef<K>,
    {
        self.insert_f(elem, |elem_ins, elem| elem.eq(elem_ins.as_ref()), g)
    }

    pub fn delete_key<K>(&self, key: K, g: &Guard) -> bool
    where
        E: PartialEq<K> + From<K> + AsRef<K>,
    {
        self.delete_f(E::from(key), |elem_del, elem| elem.eq(elem_del.as_ref()), g)
    }

    pub fn get_key<K>(&self, key: &K, g: &Guard) -> Option<E>
    where
        E: PartialEq<K> + Clone,
    {
        self.find_fn(
            |elem| match elem.eq(key) {
                true => Some(elem.clone()),
                false => None,
            },
            g,
        )
    }
}

impl<E> Drop for LinkedList<E> {
    fn drop(&mut self) {
        let g = unsafe { epoch::unprotected() };
        let head = self.head.swap(Shared::null(), Ordering::AcqRel, g);
        let iter = NodePtrIter::new(head, g);
        for ptr in iter {
            drop(unsafe { ptr.into_owned() });
        }
    }
}

pub struct IntoIter<E> {
    prev: u64,
    cur: u64,
    _marker: PhantomData<E>,
}

impl<E> Iterator for IntoIter<E> {
    type Item = E;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let prev_ptr: SharedNodePtr<E> = unsafe { Shared::from_u64(self.prev) };
        if !prev_ptr.is_null() {
            drop(unsafe { prev_ptr.into_owned() });
        }

        let node_ptr: SharedNodePtr<E> = unsafe { Shared::from_u64(self.cur) };
        self.prev = self.cur;

        if node_ptr.is_null() {
            return None;
        }

        let node = unsafe { node_ptr.deref() };
        let elem = unsafe { ptr::read(&node.elem as *const E) };
        self.cur = node
            .next
            .load(Ordering::Acquire, unsafe { epoch::unprotected() })
            .into_u64();
        Some(elem)
    }
}

impl<E> Drop for IntoIter<E> {
    fn drop(&mut self) {
        for _ in self {}
    }
}

impl<E> IntoIterator for LinkedList<E> {
    type Item = E;
    type IntoIter = IntoIter<E>;

    fn into_iter(self) -> Self::IntoIter {
        let head = self.head.swap(Shared::null(), Ordering::Acquire, unsafe {
            epoch::unprotected()
        });
        IntoIter {
            prev: Shared::<()>::null().into_u64(),
            cur: head.into_u64(),
            _marker: PhantomData,
        }
    }
}

impl<E, Iter> From<Iter> for LinkedList<E>
where
    Iter: Iterator<Item = E>,
    E: Eq,
{
    fn from(iter: Iter) -> Self {
        let g = unsafe { epoch::unprotected() };
        let s = Self::new();
        for elem in iter {
            s.insert(elem, g);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_epoch as epoch;

    use crate::collections::linked_list::{LinkedList, Node, STATE_VALID};
    use crate::epoch::Owned;

    #[test]
    fn test_from() {
        let l1 = LinkedList::new_with_head(Box::new(Node::new(1, STATE_VALID)));
        let l2 = LinkedList::new_with_head(Owned::new(Node::new(1, STATE_VALID)));
    }

    #[test]
    fn test_iter() {
        let l = LinkedList::from((0..10).rev());
        let count = l
            .iter(unsafe { epoch::unprotected() })
            .map(|x| {
                dbg!(x);
            })
            .count();
        assert_eq!(10, count);
    }

    #[test]
    fn test_into_iter() {
        let l = LinkedList::from((0..10).rev());
        let count = l
            .into_iter()
            .map(|x| {
                dbg!(x);
            })
            .count();
        assert_eq!(10, count);
    }
}
