pub fn wildcard_match(text: &str, matcher: &str) -> bool {
    let rex = wildcard_to_regexp(matcher, ".");
    let pattern = regex::Regex::new(&rex).unwrap();
    pattern
        .find(text)
        .map(|m| m.start() == 0 && m.end() == text.len())
        .unwrap_or(false)
}

fn wildcard_to_regexp(pattern: &str, separator: &str) -> String {
    let mut dst = String::new();
    let pat = "**".to_string() + separator + "*";
    let src = pattern.replace(&pat, "**");
    let src = src.chars().collect::<Vec<char>>();
    let mut i = 0;
    let size = src.len();
    while i < size {
        let c = src[i];
        i += 1;
        if c == '*' {
            // One char lookahead for **
            if i < size && src[i] == '*' {
                dst += ".*";
                i += 1;
            } else {
                dst = dst + "[^" + separator + "]*";
            }
        } else if c == '?' {
            dst = dst + "[^" + separator + "]";
        } else if c == '.'
            || c == '+'
            || c == '{'
            || c == '}'
            || c == '('
            || c == ')'
            || c == '|'
            || c == '^'
            || c == '$'
        {
            // These need to be escaped in regular expressions
            dst.push('\\');
            dst.push(c);
        } else if c == '\\' {
            let tuple = double_slashes(&dst, &src, i);
            dst = tuple.0;
            i = tuple.1;
        } else {
            dst.push(c);
        }
    }
    dst
}

fn double_slashes(dst: &str, src: &[char], i: usize) -> (String, usize) {
    let mut dst = dst.to_string();
    dst.push('\\');
    let mut i = i;
    if i + 1 < src.len() {
        dst.push('\\');
        dst.push(src[i]);
        i += 1;
    } else {
        // A backslash at the very end is treated like an escaped backslash
        dst.push('\\');
    }
    (dst, i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_matcher() {
        assert_eq!(wildcard_match("", "Foo"), false);
        assert_eq!(wildcard_match("Foo", ""), false);
        assert_eq!(wildcard_match("", ""), true);
        assert_eq!(wildcard_match("Foo", "Foo"), true);
        assert_eq!(wildcard_match("", "*"), true);
        assert_eq!(wildcard_match("", "?"), false);
        assert_eq!(wildcard_match("Foo", "Fo*"), true);
        assert_eq!(wildcard_match("Foo", "Fo?"), true);
        assert_eq!(wildcard_match("Foo Bar and Catflag", "Fo*"), true);
        assert_eq!(wildcard_match("New Bookmarks", "N?w ?o?k??r?s"), true);
        assert_eq!(wildcard_match("Foo", "Bar"), false);
        assert_eq!(wildcard_match("Foo Bar Foo", "F*o Bar*"), true);
        assert_eq!(wildcard_match("Adobe Acrobat Installer", "Ad*er"), true);
        assert_eq!(wildcard_match("Foo", "*Foo"), true);
        assert_eq!(wildcard_match("BarFoo", "*Foo"), true);
        assert_eq!(wildcard_match("Foo", "Foo*"), true);
        assert_eq!(wildcard_match("FooBar", "Foo*"), true);
        assert_eq!(wildcard_match("FOO", "*Foo"), false);
        assert_eq!(wildcard_match("BARFOO", "*Foo"), false);
        assert_eq!(wildcard_match("FOO", "Foo*"), false);
        assert_eq!(wildcard_match("FOOBAR", "Foo*"), false);
        assert_eq!(wildcard_match("eve", "eve*"), true);
        assert_eq!(wildcard_match("alice.bob.eve", "a*.bob.eve"), true);
        assert_eq!(wildcard_match("alice.bob.eve", "a*"), false);
        assert_eq!(wildcard_match("alice.bob.eve", "a**"), true);
        assert_eq!(wildcard_match("alice.bob.eve", "alice.bob*"), false);
        assert_eq!(wildcard_match("alice.bob.eve", "alice.bob**"), true);
    }
}
