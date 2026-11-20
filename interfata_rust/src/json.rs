//! Construirea manuala a raspunsurilor JSON (fara serde).

/// Escapeaza un sir pentru a fi pus intre ghilimele intr-un document JSON.
pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Un sir JSON complet, cu ghilimele.
pub fn str(s: &str) -> String {
    format!("\"{}\"", esc(s))
}

/// Un sir JSON optional (`null` pentru None).
pub fn opt_str(s: Option<&str>) -> String {
    match s {
        Some(v) => str(v),
        None => "null".to_string(),
    }
}

/// Construieste un obiect JSON din perechi (cheie, valoare deja serializata).
pub fn obj(fields: &[(&str, String)]) -> String {
    let parts: Vec<String> = fields
        .iter()
        .map(|(k, v)| format!("{}:{}", str(k), v))
        .collect();
    format!("{{{}}}", parts.join(","))
}

/// Construieste un tablou JSON din valori deja serializate.
pub fn arr(items: &[String]) -> String {
    format!("[{}]", items.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_special_characters() {
        assert_eq!(esc("a\"b\\c\nd\te"), "a\\\"b\\\\c\\nd\\te");
        assert_eq!(esc("\u{1}"), "\\u0001");
        assert_eq!(esc("ăîșț ✓ 💥"), "ăîșț ✓ 💥");
    }

    #[test]
    fn builds_objects_and_arrays() {
        let o = obj(&[("a", "1".into()), ("b", str("x\"y")), ("c", opt_str(None))]);
        assert_eq!(o, r#"{"a":1,"b":"x\"y","c":null}"#);
        assert_eq!(arr(&["1".into(), "2".into()]), "[1,2]");
    }
}
