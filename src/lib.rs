#[global_allocator]
// static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
static GLOBAL: tcmalloc::TCMalloc = tcmalloc::TCMalloc;

pub mod collections;
pub mod epoch;
pub mod kv;
