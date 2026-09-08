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
//! mismatch) return `Ok(None)`; malformed paths return `Err`. Dotted identifiers start with a
//! Unicode alphabetic character or `_`, then Unicode alphanumeric characters or `_`, matching
//! Java's `Character.isLetter`/`isLetterOrDigit`; use the quoted form for other keys. Negative
//! indices are rejected. Quoted-key escapes recognize only `\\` and backslash+quote (option B);
//! any other escape is a parse error.

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
    // Iterate over Unicode scalar values (chars), not bytes, so that non-ASCII
    // identifier keys like `$.café` / `$.über` / CJK resolve. Java's readIdent uses
    // Character.isLetter / isLetterOrDigit (Unicode-aware); Rust's char::is_alphabetic /
    // char::is_alphanumeric closely match those.
    let chars: Vec<char> = path.chars().collect();
    if chars.is_empty() {
        return Err("variant path must start with '$'".to_string());
    }
    if chars[0] != '$' {
        return Err(format!("variant path must start with '$', got: {path}"));
    }
    let mut out = Vec::new();
    let mut pos = 1usize;
    while pos < chars.len() {
        match chars[pos] {
            '.' => {
                pos += 1;
                if pos >= chars.len() || !is_ident_start(chars[pos]) {
                    return Err(format!(
                        "expected identifier (starting with a letter or '_') after '.' in variant path: {path}"
                    ));
                }
                let start = pos;
                pos += 1;
                while pos < chars.len() && is_ident_part(chars[pos]) {
                    pos += 1;
                }
                out.push(Segment::Field(chars[start..pos].iter().collect()));
            }
            '[' => {
                pos += 1;
                if pos >= chars.len() {
                    return Err(format!(
                        "unexpected end of input after '[' in variant path: {path}"
                    ));
                }
                if chars[pos] == '"' || chars[pos] == '\'' {
                    let (key, next) = read_quoted_key(path, &chars, pos)?;
                    pos = next;
                    out.push(Segment::Field(key));
                } else {
                    let (idx, next) = read_index(path, &chars, pos)?;
                    pos = next;
                    out.push(Segment::Index(idx));
                }
                if pos >= chars.len() || chars[pos] != ']' {
                    return Err(format!("expected ']' in variant path: {path}"));
                }
                pos += 1;
            }
            other => {
                return Err(format!(
                    "unexpected character '{other}' in variant path: {path}"
                ));
            }
        }
    }
    Ok(out)
}

fn read_quoted_key(path: &str, chars: &[char], mut pos: usize) -> Result<(String, usize), String> {
    let quote = chars[pos];
    pos += 1;
    let mut key = String::new();
    while pos < chars.len() {
        let c = chars[pos];
        pos += 1;
        if c == '\\' {
            if pos >= chars.len() {
                return Err(format!(
                    "unterminated escape at end of quoted key in variant path: {path}"
                ));
            }
            let esc = chars[pos];
            pos += 1;
            if esc == '\\' || esc == quote {
                key.push(esc);
            } else {
                return Err(format!(
                    "unsupported escape in quoted key of variant path (only '\\\\' and backslash+quote are allowed): {path}"
                ));
            }
        } else if c == quote {
            return Ok((key, pos));
        } else {
            key.push(c);
        }
    }
    Err(format!("unterminated quoted key in variant path: {path}"))
}

fn read_index(path: &str, chars: &[char], mut pos: usize) -> Result<(usize, usize), String> {
    if chars[pos] == '-' {
        return Err(format!(
            "negative indices are not supported in variant path: {path}"
        ));
    }
    let start = pos;
    // Indices are ASCII digits only (Java parses via Integer.parseInt); non-ASCII digits
    // are intentionally not accepted here.
    while pos < chars.len() && chars[pos].is_ascii_digit() {
        pos += 1;
    }
    if pos == start {
        return Err(format!("expected integer index in variant path: {path}"));
    }
    let digits: String = chars[start..pos].iter().collect();
    match digits.parse::<i64>() {
        Ok(n) if n >= 0 && n <= i32::MAX as i64 => Ok((n as usize, pos)),
        _ => Err(format!("index out of int range in variant path: {path}")),
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serdes::variant::Variant;

    fn resolved_long(root: &Variant, path: &str) -> i64 {
        walk(root, path)
            .unwrap_or_else(|e| panic!("path {path} errored: {e}"))
            .unwrap_or_else(|| panic!("path {path} did not resolve"))
            .get_long()
            .unwrap()
    }

    // Regression: dotted identifiers were ASCII-only (is_ascii_alphabetic/alphanumeric on bytes),
    // so `$.café` was a parse/CEL error. The scanner now iterates over Unicode scalar values and
    // uses char::is_alphabetic / char::is_alphanumeric (matching Java's isLetter/isLetterOrDigit).
    #[test]
    fn dotted_non_ascii_identifier_resolves() {
        let v = Variant::parse_json(r#"{"café": 1, "über": 2, "名前": 3}"#).unwrap();
        assert_eq!(resolved_long(&v, "$.café"), 1);
        assert_eq!(resolved_long(&v, "$.über"), 2);
        assert_eq!(resolved_long(&v, "$.名前"), 3);
    }

    #[test]
    fn ascii_and_quoted_forms_still_work() {
        let v = Variant::parse_json(r#"{"café": 1, "foo": 2, "a1_b": 3}"#).unwrap();
        // ASCII dotted.
        assert_eq!(resolved_long(&v, "$.foo"), 2);
        assert_eq!(resolved_long(&v, "$.a1_b"), 3);
        // Quoted key with non-ASCII (both quote styles).
        assert_eq!(resolved_long(&v, r#"$["café"]"#), 1);
        assert_eq!(resolved_long(&v, "$['café']"), 1);
    }

    #[test]
    fn indices_remain_ascii_digits() {
        let v = Variant::parse_json(r#"{"xs": [10, 20, 30]}"#).unwrap();
        assert_eq!(resolved_long(&v, "$.xs[1]"), 20);
        // A digit-leading ident after '.' is still rejected.
        assert!(walk(&v, "$.1abc").is_err());
    }
}
