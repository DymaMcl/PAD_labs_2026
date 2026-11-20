//! Client HTTP minimal folosit ca proxy catre broker (:7070) si catre pagina
//! de administrare a serviciului de conturi (:7081). Browserul vorbeste doar
//! cu panoul, deci nu exista probleme de CORS.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

/// Raspunsul primit de la serviciul din spate.
pub struct Upstream {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// Trimite o cerere catre 127.0.0.1:port si intoarce raspunsul complet.
pub fn forward(
    port: u16,
    method: &str,
    target: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<Upstream, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(800))
        .map_err(|_| format!("serviciul de pe portul {port} nu ruleaza"))?;
    s.set_read_timeout(Some(timeout)).ok();
    s.set_write_timeout(Some(Duration::from_secs(5))).ok();

    let head = format!(
        "{method} {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    s.write_all(body).map_err(|e| e.to_string())?;

    let mut raw = Vec::new();
    s.read_to_end(&mut raw)
        .map_err(|e| format!("citire raspuns: {e}"))?;
    parse_response(&raw)
}

/// Parseaza un raspuns HTTP/1.1 complet (inclusiv Transfer-Encoding: chunked).
pub fn parse_response(raw: &[u8]) -> Result<Upstream, String> {
    let split = find(raw, b"\r\n\r\n").ok_or("raspuns HTTP incomplet")?;
    let head = String::from_utf8_lossy(&raw[..split]);
    let rest = &raw[split + 4..];
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or("linie de status invalida")?;

    let mut content_type = "application/octet-stream".to_string();
    let mut chunked = false;
    let mut length: Option<usize> = None;
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            let (k, v) = (k.trim(), v.trim());
            if k.eq_ignore_ascii_case("content-type") {
                content_type = v.to_string();
            } else if k.eq_ignore_ascii_case("transfer-encoding")
                && v.eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            } else if k.eq_ignore_ascii_case("content-length") {
                length = v.parse().ok();
            }
        }
    }
    let body = if chunked {
        decode_chunked(rest)?
    } else if let Some(n) = length {
        rest[..n.min(rest.len())].to_vec()
    } else {
        rest.to_vec()
    };
    Ok(Upstream {
        status,
        content_type,
        body,
    })
}

/// Decodeaza un corp trimis in bucati (chunked).
pub fn decode_chunked(mut data: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let eol = find(data, b"\r\n").ok_or("chunk fara dimensiune")?;
        let size_str = String::from_utf8_lossy(&data[..eol]);
        let size_hex = size_str.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| format!("dimensiune de chunk invalida: {size_hex}"))?;
        data = &data[eol + 2..];
        if size == 0 {
            return Ok(out);
        }
        if data.len() < size {
            return Err("chunk trunchiat".into());
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size..];
        if data.starts_with(b"\r\n") {
            data = &data[2..];
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_length_response() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 5\r\n\r\n{\"a\"}";
        let r = parse_response(raw).unwrap();
        assert_eq!(r.status, 201);
        assert_eq!(r.content_type, "application/json");
        assert_eq!(r.body, b"{\"a\"}");
    }

    #[test]
    fn parses_chunked_response() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n6;ext=1\r\npedia \r\nE\r\nin \r\n\r\nchunks.\r\n0\r\n\r\n";
        let r = parse_response(raw).unwrap();
        assert_eq!(
            String::from_utf8(r.body).unwrap(),
            "Wikipedia in \r\n\r\nchunks."
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_response(b"nimic").is_err());
        assert!(decode_chunked(b"zz\r\n").is_err());
    }

    #[test]
    fn closed_port_is_reported() {
        // portul 1 nu are niciun serviciu pe 127.0.0.1
        let e = forward(1, "GET", "/", b"", Duration::from_secs(1))
            .err()
            .unwrap();
        assert!(e.contains("nu ruleaza"));
    }
}
