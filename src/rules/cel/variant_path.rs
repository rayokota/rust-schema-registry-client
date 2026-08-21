// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The JSONPath subset used by `variants.path(v, path)` - a port of the Java/Python/Go
//! VariantPath. Supports `$`, `$.field`, `$.field.subfield`, `$[i]`, `$["quoted key"]` /
//! `$['quoted key']`. Resolution failures (missing field, out-of-bounds index, type
//! mismatch) return `Ok(None)`; malformed paths return `Err`. Identifier names follow
//! `[A-Za-z_][A-Za-z0-9_]*`; use the quoted form for other keys. Negative indices are
//! rejected. Quoted-key escapes recognize only `\\` and backslash+quote (option B); any
//! other escape is a parse error.

use crate::serdes::variant::{Type, Variant};

enum Segment {
    Field(String),
    Index(usize),
}

/// Walk `root` following `path`. Returns the resolved Variant, `Ok(None)` on a resolution
/// miss, or `Err(msg)` on a malformed path.
pub fn walk(root: &Variant, path: &str) -> Result<Option<Variant>, String> {
    let segments = parse(path)?;
    let mut current = Some(root.clone());
    for seg in segments {
        let cur = match current {
            Some(c) => c,
            None => return Ok(None),
        };
        current = match seg {
            Segment::Field(key) => {
                if cur.get_type() == Type::Object {
                    cur.get_field_by_key(&key)
                } else {
                    None
                }
            }
            Segment::Index(idx) => {
                if cur.get_type() == Type::Array {
                    cur.get_element_at_index(idx)
                } else {
                    None
                }
            }
        };
    }
    Ok(current)
}

fn parse(path: &str) -> Result<Vec<Segment>, String> {
    let bytes = path.as_bytes();
    if bytes.is_empty() {
        return Err("variant path must start with '$'".to_string());
    }
    if bytes[0] != b'$' {
        return Err(format!("variant path must start with '$', got: {path}"));
    }
    let mut out = Vec::new();
    let mut pos = 1usize;
    while pos < bytes.len() {
        match bytes[pos] {
            b'.' => {
                pos += 1;
                if pos >= bytes.len() || !is_ident_start(bytes[pos]) {
                    return Err(format!(
                        "expected identifier (starting with a letter or '_') after '.' in variant path: {path}"
                    ));
                }
                let start = pos;
                pos += 1;
                while pos < bytes.len() && is_ident_part(bytes[pos]) {
                    pos += 1;
                }
                out.push(Segment::Field(path[start..pos].to_string()));
            }
            b'[' => {
                pos += 1;
                if pos >= bytes.len() {
                    return Err(format!("unexpected end of input after '[' in variant path: {path}"));
                }
                if bytes[pos] == b'"' || bytes[pos] == b'\'' {
                    let (key, next) = read_quoted_key(path, bytes, pos)?;
                    pos = next;
                    out.push(Segment::Field(key));
                } else {
                    let (idx, next) = read_index(path, bytes, pos)?;
                    pos = next;
                    out.push(Segment::Index(idx));
                }
                if pos >= bytes.len() || bytes[pos] != b']' {
                    return Err(format!("expected ']' in variant path: {path}"));
                }
                pos += 1;
            }
            other => {
                return Err(format!(
                    "unexpected character '{}' in variant path: {path}",
                    other as char
                ));
            }
        }
    }
    Ok(out)
}

fn read_quoted_key(path: &str, bytes: &[u8], mut pos: usize) -> Result<(String, usize), String> {
    let quote = bytes[pos];
    pos += 1;
    let mut key = String::new();
    while pos < bytes.len() {
        let c = bytes[pos];
        pos += 1;
        if c == b'\\' {
            if pos >= bytes.len() {
                return Err(format!("unterminated escape at end of quoted key in variant path: {path}"));
            }
            let esc = bytes[pos];
            pos += 1;
            if esc == b'\\' || esc == quote {
                key.push(esc as char);
            } else {
                return Err(format!(
                    "unsupported escape in quoted key of variant path (only '\\\\' and backslash+quote are allowed): {path}"
                ));
            }
        } else if c == quote {
            return Ok((key, pos));
        } else {
            // Preserve multi-byte UTF-8: fall back to the source slice for non-ASCII.
            if c < 0x80 {
                key.push(c as char);
            } else {
                // Copy the full UTF-8 code point from the source string.
                let ch_start = pos - 1;
                let s = &path[ch_start..];
                let ch = s.chars().next().unwrap();
                key.push(ch);
                pos = ch_start + ch.len_utf8();
            }
        }
    }
    Err(format!("unterminated quoted key in variant path: {path}"))
}

fn read_index(path: &str, bytes: &[u8], mut pos: usize) -> Result<(usize, usize), String> {
    if bytes[pos] == b'-' {
        return Err(format!("negative indices are not supported in variant path: {path}"));
    }
    let start = pos;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == start {
        return Err(format!("expected integer index in variant path: {path}"));
    }
    match path[start..pos].parse::<i64>() {
        Ok(n) if n >= 0 && n <= i32::MAX as i64 => Ok((n as usize, pos)),
        _ => Err(format!("index out of int range in variant path: {path}")),
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_part(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}
