#[global_allocator]
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
// static GLOBAL: tcmalloc::TCMalloc = tcmalloc::TCMalloc;
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

pub mod collections;
pub mod epoch;
pub mod kv;
