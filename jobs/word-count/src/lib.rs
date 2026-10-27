//! Word-count WebAssembly job library targeted for `wasm32-wasi`.

use std::alloc::{alloc as std_alloc, dealloc as std_dealloc, Layout};
use std::slice;
use std::str;

/// Allocates `len` bytes in linear memory and returns a pointer to the allocated block.
#[no_mangle]
pub extern "C" fn alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return std::ptr::NonNull::<u8>::dangling().as_ptr();
    }
    let layout = match Layout::from_size_align(len, 1) {
        Ok(layout) => layout,
        Err(_) => return std::ptr::null_mut(),
    };
    unsafe { std_alloc(layout) }
}

/// Deallocates a block of `len` bytes previously allocated at `ptr`.
#[no_mangle]
pub extern "C" fn dealloc(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    if let Ok(layout) = Layout::from_size_align(len, 1) {
        unsafe {
            std_dealloc(ptr, layout);
        }
    }
}

/// Reads the linear memory slice at `(ptr, len)` and returns the whitespace-delimited word count.
#[no_mangle]
pub extern "C" fn map_line(ptr: *const u8, len: usize) -> usize {
    if ptr.is_null() || len == 0 {
        return 0;
    }
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    let line = str::from_utf8(bytes).unwrap_or("");
    line.split_whitespace().count()
}
