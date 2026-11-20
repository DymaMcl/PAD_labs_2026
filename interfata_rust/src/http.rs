//! Un server HTTP/1.1 minimal (doar ce are nevoie panoul) si utilitare
//! pentru cereri: parsare, query string, raspunsuri.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;

/// Dimensiunea maxima acceptata pentru corpul unei cereri.
const MAX_BODY: usize = 1 << 20;

/// O cerere HTTP primita de la browser.
#[derive(Debug, Default)]
pub struct Request {
    pub method: String,
    /// Calea fara query string, ex. `/api/start`.
    pub path: String,
    /// Calea completa, cu query string (folosita la proxy).
    pub target: String,
    pub query: Query,
    pub body: Vec<u8>,
}

/// Parametrii din query string, decodati.
#[derive(Debug, Default, Clone)]
pub struct Query(pub Vec<(String, String)>);

impl Query {
    pub fn parse(s: &str) -> Query {
        let mut out = Vec::new();
        for part in s.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = match part.split_once('=') {
                Some((k, v)) => (k, v),
                None => (part, ""),
            };
            out.push((percent_decode(k), percent_decode(v)));
        }
        Query(out)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Valoare booleana: "1", "true", "on", "da" -> true.
    pub fn flag(&self, key: &str) -> bool {
        matches!(
            self.get(key),
            Some("1") | Some("true") | Some("on") | Some("da")
        )
    }
}

/// Decodeaza `%XX` si `+` dintr-o componenta de URL.
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(b) => {
                        out.push(b);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Citeste o cerere de pe conexiune.
pub fn read_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "conexiune inchisa",
        ));
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();

    let mut content_length = 0usize;
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h)? == 0 {
            break;
        }
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((name, value)) = h.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    if content_length > MAX_BODY {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "corp prea mare"));
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;

    let (path, q) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.clone(), String::new()),
    };
    Ok(Request {
        method,
        path,
        target,
        query: Query::parse(&q),
        body,
    })
}

/// Un raspuns HTTP.
pub struct Response {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(status: u16, body: String) -> Response {
        Response {
            status,
            content_type: "application/json; charset=utf-8".into(),
            body: body.into_bytes(),
        }
    }

    pub fn ok_json(body: String) -> Response {
        Response::json(200, body)
    }

    pub fn error(status: u16, msg: &str) -> Response {
        Response::json(
            status,
            crate::json::obj(&[("error", crate::json::str(msg))]),
        )
    }

    pub fn static_file(content_type: &str, body: &'static str) -> Response {
        Response {
            status: 200,
            content_type: content_type.into(),
            body: body.as_bytes().to_vec(),
        }
    }
}

pub fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Scrie raspunsul si inchide logic conexiunea (Connection: close).
pub fn write_response(stream: &mut TcpStream, r: &Response) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        r.status,
        reason(r.status),
        r.content_type,
        r.body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(&r.body)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_percent_and_plus() {
        assert_eq!(percent_decode("a%20b+c"), "a b c");
        assert_eq!(percent_decode("ACC-1%2Fx"), "ACC-1/x");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%C8%99"), "ș");
    }

    #[test]
    fn parses_query() {
        let q = Query::parse("kind=consumer&name=c1&naive=1&empty=&x");
        assert_eq!(q.get("kind"), Some("consumer"));
        assert_eq!(q.get("name"), Some("c1"));
        assert!(q.flag("naive"));
        assert!(!q.flag("empty"));
        assert_eq!(q.get("x"), Some(""));
        assert_eq!(q.get("lipsa"), None);
    }
}
