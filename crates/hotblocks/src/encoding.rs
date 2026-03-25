#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    Gzip,
    Zstd,
}

impl ContentEncoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContentEncoding::Gzip => "gzip",
            ContentEncoding::Zstd => "zstd",
        }
    }

    /// Parse Accept-Encoding header respecting quality values.
    /// See <https://datatracker.ietf.org/doc/html/rfc7231#section-5.3.4>
    /// At equal q, prefers zstd over gzip.
    /// Returns None if neither gzip nor zstd is accepted.
    pub fn from_accept_encoding(header: Option<&str>) -> Option<Self> {
        let header = header?;

        let mut best: Option<Self> = None;
        let mut best_q: f32 = -1.0;

        for part in header.split(',') {
            let part = part.trim();
            let (name, q) = if let Some((name, params)) = part.split_once(';') {
                let q = params
                    .split(';')
                    .find_map(|p| p.trim().strip_prefix("q="))
                    .and_then(|v| v.trim().parse::<f32>().ok())
                    .unwrap_or(1.0);
                (name.trim(), q)
            } else {
                (part, 1.0)
            };

            if q <= 0.0 {
                continue;
            }

            let encoding = match name {
                "zstd" => Some(ContentEncoding::Zstd),
                "gzip" => Some(ContentEncoding::Gzip),
                _ => None,
            };

            if let Some(enc) = encoding {
                if q > best_q || (q == best_q && enc == ContentEncoding::Zstd) {
                    best_q = q;
                    best = Some(enc);
                }
            }
        }

        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_header() {
        assert_eq!(ContentEncoding::from_accept_encoding(None), None);
    }

    #[test]
    fn empty_header() {
        assert_eq!(ContentEncoding::from_accept_encoding(Some("")), None);
    }

    #[test]
    fn only_zstd() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("zstd")),
            Some(ContentEncoding::Zstd)
        );
    }

    #[test]
    fn only_gzip() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("gzip")),
            Some(ContentEncoding::Gzip)
        );
    }

    #[test]
    fn zstd_and_gzip_equal_q_prefers_zstd() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("gzip, zstd")),
            Some(ContentEncoding::Zstd)
        );
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("zstd, gzip")),
            Some(ContentEncoding::Zstd)
        );
    }

    #[test]
    fn gzip_higher_q() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("gzip;q=1.0, zstd;q=0.5")),
            Some(ContentEncoding::Gzip)
        );
    }

    #[test]
    fn zstd_higher_q() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("gzip;q=0.5, zstd;q=1.0")),
            Some(ContentEncoding::Zstd)
        );
    }

    #[test]
    fn unsupported_encodings_only() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("deflate, br")),
            None
        );
    }

    #[test]
    fn mixed_with_unsupported() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("br, deflate, gzip;q=0.8")),
            Some(ContentEncoding::Gzip)
        );
    }

    #[test]
    fn q_zero_excluded() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("gzip;q=0, zstd")),
            Some(ContentEncoding::Zstd)
        );
    }

    #[test]
    fn q_zero_both() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("gzip;q=0, zstd;q=0")),
            None
        );
    }

    #[test]
    fn whitespace_handling() {
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("  gzip ; q=0.5 , zstd ; q=0.9 ")),
            Some(ContentEncoding::Zstd)
        );
    }

    #[test]
    fn wildcard_ignored() {
        // We don't handle *, just pick from what we support
        assert_eq!(
            ContentEncoding::from_accept_encoding(Some("*, gzip;q=0.5")),
            Some(ContentEncoding::Gzip)
        );
    }
}
