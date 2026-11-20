//! Two-phase MapReduce word-frequency WebAssembly kernel targeted for `wasm32-wasip1`.
//!
//! Exports both scalar `map_line` and variable-length packed `(ptr << 32) | len` Map/Reduce
//! functions (`map_bytes` and `reduce_bytes`) with zero external crate dependencies.

use std::alloc::{alloc as std_alloc, dealloc as std_dealloc, Layout};
use std::collections::BTreeMap;
use std::fmt::Write as _;
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

fn pack_output_bytes(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
    let out_ptr = alloc(bytes.len());
    if out_ptr.is_null() {
        return 0;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    }
    let packed = ((out_ptr as usize as u64) << 32) | ((bytes.len() as u32) as u64);
    packed as i64
}

fn escape_json_str(input: &str, out: &mut String) {
    out.push('"');
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn serialize_counts_json(counts: &BTreeMap<String, usize>) -> String {
    let mut out = String::from("{");
    for (idx, (word, count)) in counts.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        escape_json_str(word, &mut out);
        let _ = write!(out, ":{count}");
    }
    out.push('}');
    out
}

fn parse_flat_json_object(json_str: &str, counts: &mut BTreeMap<String, usize>) {
    let bytes = json_str.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            i += 1;
            let mut key = String::new();
            while i < bytes.len() {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                    match bytes[i] {
                        b'"' => key.push('"'),
                        b'\\' => key.push('\\'),
                        b'n' => key.push('\n'),
                        b'r' => key.push('\r'),
                        b't' => key.push('\t'),
                        other => key.push(other as char),
                    }
                    i += 1;
                } else if bytes[i] == b'"' {
                    i += 1;
                    break;
                } else {
                    let rest = &json_str[i..];
                    if let Some(ch) = rest.chars().next() {
                        key.push(ch);
                        i += ch.len_utf8();
                    } else {
                        break;
                    }
                }
            }

            while i < bytes.len() && (bytes[i] == b':' || bytes[i].is_ascii_whitespace()) {
                i += 1;
            }
            let num_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if num_start < i {
                if let Ok(val) = json_str[num_start..i].parse::<usize>() {
                    *counts.entry(key).or_insert(0) += val;
                }
            }
        } else {
            i += 1;
        }
    }
}

/// Scalar entrypoint: returns total whitespace-delimited word count.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn map_line(ptr: *const u8, len: usize) -> usize {
    read_utf8_slice(ptr, len).split_whitespace().count()
}

/// Map phase entrypoint (`(ptr, len) -> i64`): builds a deterministic JSON word-frequency map.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn map_bytes(ptr: *const u8, len: usize) -> i64 {
    let text = read_utf8_slice(ptr, len);
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for raw_token in text.split_whitespace() {
        let token = raw_token
            .trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
            .to_ascii_lowercase();
        if !token.is_empty() {
            *counts.entry(token).or_insert(0) += 1;
        }
    }
    let json = serialize_counts_json(&counts);
    pack_output_bytes(json.as_bytes())
}

/// Reduce phase entrypoint (`(ptr, len) -> i64`): merges newline-separated intermediate JSON maps.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn reduce_bytes(ptr: *const u8, len: usize) -> i64 {
    let text = read_utf8_slice(ptr, len);
    let mut merged: BTreeMap<String, usize> = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            parse_flat_json_object(trimmed, &mut merged);
        }
    }
    let json = serialize_counts_json(&merged);
    pack_output_bytes(json.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unpack_and_read(packed: i64) -> String {
        let u = packed as u64;
        let ptr = ((u >> 32) & 0xFFFF_FFFF) as usize as *mut u8;
        let len = (u & 0xFFFF_FFFF) as usize;
        let bytes = unsafe { slice::from_raw_parts(ptr, len) };
        let s = str::from_utf8(bytes).expect("valid utf8").to_string();
        dealloc(ptr, len);
        s
    }

    #[test]
    fn map_and_reduce_merge_word_frequencies() {
        let c1 = "Nitro Compute edge, nitro wasm!";
        let c2 = "Edge compute: Rust + Wasm.";

        let p1 = map_bytes(c1.as_ptr(), c1.len());
        let p2 = map_bytes(c2.as_ptr(), c2.len());
        let m1 = unpack_and_read(p1);
        let m2 = unpack_and_read(p2);

        assert_eq!(m1, r#"{"compute":1,"edge":1,"nitro":2,"wasm":1}"#);
        assert_eq!(m2, r#"{"compute":1,"edge":1,"rust":1,"wasm":1}"#);

        let shuffled = format!("{m1}\n{m2}");
        let pr = reduce_bytes(shuffled.as_ptr(), shuffled.len());
        let reduced = unpack_and_read(pr);
        assert_eq!(
            reduced,
            r#"{"compute":2,"edge":2,"nitro":2,"rust":1,"wasm":2}"#
        );
    }
}
