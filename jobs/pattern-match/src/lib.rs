//! Log anomaly & multi-entrypoint WebAssembly compute kernel targeted for `wasm32-wasip1`.

use std::alloc::{alloc as std_alloc, dealloc as std_dealloc, Layout};
use std::slice;
use std::str;

const SEVERITY_PATTERNS: &[&str] = &["error", "critical", "fatal", "panic"];

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
#[allow(clippy::not_unsafe_ptr_arg_deref)]
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

fn read_utf8_slice<'a>(ptr: *const u8, len: usize) -> &'a str {
    if ptr.is_null() || len == 0 {
        return "";
    }
    let bytes = unsafe { slice::from_raw_parts(ptr, len) };
    str::from_utf8(bytes).unwrap_or("")
}

/// Counts lines containing critical/error severity markers (`ERROR`, `CRITICAL`, `FATAL`, `PANIC`).
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn map_line(ptr: *const u8, len: usize) -> usize {
    let text = read_utf8_slice(ptr, len);
    text.lines()
        .filter(|line| {
            let lower = line.to_ascii_lowercase();
            SEVERITY_PATTERNS.iter().any(|pat| lower.contains(pat))
        })
        .count()
}

/// Counts non-empty lines inside the chunk (selectable via `--entry-fn count_lines`).
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn count_lines(ptr: *const u8, len: usize) -> usize {
    read_utf8_slice(ptr, len)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

/// Counts non-whitespace UTF-8 characters (selectable via `--entry-fn count_chars`).
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn count_chars(ptr: *const u8, len: usize) -> usize {
    read_utf8_slice(ptr, len)
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_severity_lines_and_custom_entry_points() {
        let log_chunk = "INFO startup complete\nERROR connection refused\nWARN slow disk\nFATAL out of memory\n";
        let ptr = alloc(log_chunk.len());
        unsafe {
            std::ptr::copy_nonoverlapping(log_chunk.as_ptr(), ptr, log_chunk.len());
        }
        assert_eq!(map_line(ptr, log_chunk.len()), 2);
        assert_eq!(count_lines(ptr, log_chunk.len()), 4);
        assert!(count_chars(ptr, log_chunk.len()) > 40);
        dealloc(ptr, log_chunk.len());
    }
}
