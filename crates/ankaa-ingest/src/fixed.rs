//! Decimal string → fixed-point `i64` without floating point.

/// Errors from [`parse_fixed`].
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone, Copy)]
pub enum ParseError {
    /// Empty input or no digits.
    #[error("empty number")]
    Empty,
    /// Unexpected byte.
    #[error("invalid character at {0}")]
    InvalidChar(usize),
    /// More fractional digits than `scale` (would need rounding; we refuse silently losing precision).
    #[error("too many fractional digits (scale {0})")]
    Precision(u8),
    /// Value does not fit in `i64`.
    #[error("overflow")]
    Overflow,
}

/// Parse an ASCII decimal such as `"74670.01000000"` into `i64` ticks at `scale` decimals
/// (e.g. scale 8 → `7_467_001_000_000`). Accepts an optional leading `-`, no exponent, no
/// thousands separators. Fewer fractional digits than `scale` are zero-padded; more are an
/// error (feeds publish at a fixed precision; silently rounding would hide a schema change).
pub fn parse_fixed(s: &[u8], scale: u8) -> Result<i64, ParseError> {
    if s.is_empty() {
        return Err(ParseError::Empty);
    }
    let (neg, mut i) = if s[0] == b'-' { (true, 1) } else { (false, 0) };
    let mut mant: i64 = 0;
    let mut digits = 0usize;
    let mut frac_digits: Option<u8> = None;
    while i < s.len() {
        let c = s[i];
        match c {
            b'0'..=b'9' => {
                mant = mant
                    .checked_mul(10)
                    .and_then(|m| m.checked_add(i64::from(c - b'0')))
                    .ok_or(ParseError::Overflow)?;
                digits += 1;
                if let Some(f) = frac_digits.as_mut() {
                    *f += 1;
                    if *f > scale {
                        return Err(ParseError::Precision(scale));
                    }
                }
            }
            b'.' if frac_digits.is_none() => frac_digits = Some(0),
            _ => return Err(ParseError::InvalidChar(i)),
        }
        i += 1;
    }
    if digits == 0 {
        return Err(ParseError::Empty);
    }
    let pad = scale - frac_digits.unwrap_or(0);
    for _ in 0..pad {
        mant = mant.checked_mul(10).ok_or(ParseError::Overflow)?;
    }
    Ok(if neg { -mant } else { mant })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exchange_decimals() {
        assert_eq!(parse_fixed(b"74670.01000000", 8), Ok(7_467_001_000_000));
        assert_eq!(parse_fixed(b"0.00400000", 8), Ok(400_000));
        assert_eq!(parse_fixed(b"1", 8), Ok(100_000_000));
        assert_eq!(parse_fixed(b"-2.5", 2), Ok(-250));
        assert_eq!(parse_fixed(b"0", 0), Ok(0));
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(parse_fixed(b"", 8), Err(ParseError::Empty));
        assert_eq!(parse_fixed(b"-", 8), Err(ParseError::Empty));
        assert_eq!(parse_fixed(b"1.2.3", 8), Err(ParseError::InvalidChar(3)));
        assert_eq!(parse_fixed(b"1e5", 8), Err(ParseError::InvalidChar(1)));
        assert_eq!(
            parse_fixed(b"0.123456789", 8),
            Err(ParseError::Precision(8))
        );
        assert_eq!(
            parse_fixed(b"99999999999999999999", 0),
            Err(ParseError::Overflow)
        );
    }
}
