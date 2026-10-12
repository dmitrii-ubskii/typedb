/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]
#![deny(elided_lifetimes_in_paths)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct TrackingAlloc {
    pub allocated_bytes: AtomicUsize,
}

impl TrackingAlloc {
    const fn new() -> Self {
        Self { allocated_bytes: AtomicUsize::new(0) }
    }
}

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocated_bytes.fetch_add(layout.size(), Ordering::AcqRel);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.allocated_bytes.fetch_sub(layout.size(), Ordering::AcqRel);
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
pub static ALLOC: TrackingAlloc = TrackingAlloc::new();
