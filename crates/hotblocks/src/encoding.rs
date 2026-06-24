use axum::http::{HeaderMap, header::ACCEPT_ENCODING};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    Gzip,
    Zstd
}

impl ContentEncoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContentEncoding::Gzip => "gzip",
            ContentEncoding::Zstd => "zstd"
        }
    }

    /// Respond with zstd if the client mentions it in `Accept-Encoding`, otherwise with gzip
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let accepts_zstd = headers
            .get(ACCEPT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("zstd"));
        if accepts_zstd {
            ContentEncoding::Zstd
        } else {
            ContentEncoding::Gzip
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(accept_encoding: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, accept_encoding.parse().unwrap());
        headers
    }

    #[test]
    fn no_header_defaults_to_gzip() {
        assert_eq!(ContentEncoding::from_headers(&HeaderMap::new()), ContentEncoding::Gzip);
    }

    #[test]
    fn gzip_only() {
        assert_eq!(ContentEncoding::from_headers(&headers("gzip")), ContentEncoding::Gzip);
    }

    #[test]
    fn zstd_preferred_when_mentioned() {
        assert_eq!(ContentEncoding::from_headers(&headers("zstd")), ContentEncoding::Zstd);
        assert_eq!(
            ContentEncoding::from_headers(&headers("gzip, deflate, zstd")),
            ContentEncoding::Zstd
        );
    }
}
