//! Shared types: configuration, errors, identifiers, metrics registry.
//!
//! Part of ANKAA-ENGINE. See `docs/prd/PRD-v4.1.md`.
// `unsafe` is used only for `getrlimit` in `config::memlock_limit_bytes`.
#![deny(unsafe_op_in_unsafe_fn)]

pub mod config;
pub use config::{Config, ConfigError, Secret};

/// Crate version string (from Cargo).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Constant-time byte-slice equality for secrets (length difference leaks only the length).
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Extract a bearer token from an `Authorization` header value (`Bearer <token>`, scheme
/// case-insensitive, surrounding whitespace ignored). Returns `None` when the scheme differs.
#[must_use]
pub fn bearer_token(header_value: &str) -> Option<&str> {
    let v = header_value.trim();
    let (scheme, rest) = v.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let t = rest.trim();
    (!t.is_empty()).then_some(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_parsing() {
        assert_eq!(bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(bearer_token("bearer   abc  "), Some("abc"));
        assert_eq!(bearer_token("Basic abc"), None);
        assert_eq!(bearer_token("Bearer"), None);
        assert_eq!(bearer_token("Bearer "), None);
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}

/// A URL that is safe to log: everything before the last `@` (user name, password, token) is
/// replaced by `***`, and a query string is dropped. `nats://user:pw@host:4222` becomes
/// `nats://***@host:4222`. When in doubt it redacts: a `@` in a path is treated as credentials.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let url = url.split(['?', '#']).next().unwrap_or(url);
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (Some(s), r),
        None => (None, url),
    };
    let rest = match rest.rfind('@') {
        Some(at) => format!("***@{}", &rest[at + 1..]),
        None => rest.to_owned(),
    };
    match scheme {
        Some(s) => format!("{s}://{rest}"),
        None => rest,
    }
}

#[cfg(test)]
mod redact_tests {
    use super::redact_url;

    #[test]
    fn urls_are_redacted_for_logs() {
        assert_eq!(redact_url("nats://127.0.0.1:4222"), "nats://127.0.0.1:4222");
        assert_eq!(
            redact_url("nats://user:s3cret@broker:4222"),
            "nats://***@broker:4222"
        );
        assert_eq!(
            redact_url("tls://tok3n@broker:4222/x"),
            "tls://***@broker:4222/x"
        );
        assert_eq!(redact_url("host:9092,other:9092"), "host:9092,other:9092");
        // a password with a slash, and credentials without a scheme
        assert_eq!(
            redact_url("nats://user:pa/ss@broker:4222"),
            "nats://***@broker:4222"
        );
        assert_eq!(redact_url("user:pw@host:9092"), "***@host:9092");
        // query strings (keys, signatures) are dropped
        assert_eq!(redact_url("wss://h/stream?apiKey=abc"), "wss://h/stream");
    }
}
