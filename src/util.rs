
use std::{
    alloc::{GlobalAlloc, Layout},
};

use pgx::*;

struct PallocAllocator;

/// There is an uncomfortable mismatch between rust's memory allocation and
/// postgres's; rust tries to clean memory by using stack-based destructors,
/// while postgres does so using arenas. The issue we encounter is that postgres
/// implements exception-handling using setjmp/longjmp, which will can jump over
/// stack frames containing rust destructors. To avoid needing to register a
/// setjmp handler at every call to a postgres function, we use postgres's
/// MemoryContexts to manage memory, even though this is not strictly speaking
/// safe. Though it is tempting to try to get more control over which
/// MemoryContext we allocate in, there doesn't seem to be way to do so that is
/// safe in the context of postgres exceptions and doesn't incur the cost of
/// setjmp
unsafe impl GlobalAlloc for PallocAllocator {
    //FIXME allow for switching the memory context allocated in
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        pg_sys::MemoryContextAlloc(pg_sys::CurrentMemoryContext, layout.size() as _)  as *mut _
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        pg_sys::pfree(ptr as *mut _)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        pg_sys::MemoryContextAllocZero(pg_sys::CurrentMemoryContext, layout.size() as _)  as *mut _
    }

    unsafe fn realloc(&self, ptr: *mut u8, _layout: Layout, new_size: usize) -> *mut u8 {
        pg_sys::repalloc(ptr as *mut _, new_size as _) as *mut _
    }
}
