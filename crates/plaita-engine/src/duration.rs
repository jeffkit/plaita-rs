//! ISO 8601 duration parsing for flow/node timeouts (port of plaita-go
//! `pkg/duration`, contract from plaita runner `_parse_timeout`).
//!
//! - unset = no timeout;
//! - an explicit zero (`PT0S`, `PT`) is a SET zero budget (the strictest
//!   limit — it times out immediately), never "no timeout" (plaita#138);
//! - fractional seconds (`PT0.3S`) and days/weeks (`P1D`) are supported;
//! - year/month components are rejected: they have no fixed length.

use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DurationError(pub String);

/// A parsed ISO 8601 duration. `None` means "not configured".
pub type Timeout = Option<Duration>;

/// Parses an ISO 8601 duration string with fixed-length components.
/// `"PT"` parses as an explicit zero budget.
pub fn parse_iso8601(s: &str) -> Result<Duration, DurationError> {
    let s = s.trim();
    let err = |m: &str| Err(DurationError(m.to_string()));
    let rest = match s.strip_prefix('P') {
        Some(r) => r,
        None => return err(&format!("could not parse duration string {s:?}")),
    };
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };

    let mut days: i64 = 0;
    // date components: nW nD (Y/M rejected)
    let mut num = String::new();
    let mut unit_seen = date_part.is_empty();
    for c in date_part.chars() {
        if c.is_ascii_digit() {
            num.push(c);
        } else {
            let n: i64 = if num.is_empty() {
                return err(&format!("could not parse duration string {s:?}"));
            } else {
                num.parse()
                    .map_err(|_| DurationError(format!("invalid duration component in {s:?}")))?
            };
            num.clear();
            match c {
                'Y' | 'M' => {
                    return err(&format!(
                        "duration {s:?} contains year/month components, which have no fixed length"
                    ))
                }
                'W' => days += n * 7,
                'D' => days += n,
                _ => return err(&format!("could not parse duration string {s:?}")),
            }
            unit_seen = true;
        }
    }
    if !num.is_empty() || !unit_seen {
        return err(&format!("could not parse duration string {s:?}"));
    }

    let mut secs = days as f64 * 86400.0;
    if let Some(tp) = time_part {
        let mut num = String::new();
        let mut unit_seen = tp.is_empty();
        for c in tp.chars() {
            if c.is_ascii_digit() || c == '.' {
                num.push(c);
            } else {
                let v: f64 = if num.is_empty() {
                    return err(&format!("could not parse duration string {s:?}"));
                } else {
                    num.parse().map_err(|_| {
                        DurationError(format!("invalid duration component in {s:?}"))
                    })?
                };
                num.clear();
                match c {
                    'H' => secs += v * 3600.0,
                    'M' => secs += v * 60.0,
                    'S' => secs += v,
                    _ => return err(&format!("could not parse duration string {s:?}")),
                }
                unit_seen = true;
            }
        }
        if !num.is_empty() || !unit_seen {
            return err(&format!("could not parse duration string {s:?}"));
        }
    }

    if secs.is_nan() || secs.is_infinite() {
        return err(&format!("invalid duration component in {s:?}"));
    }
    Ok(Duration::from_secs_f64(secs))
}

/// Renders a duration back to the engine's canonical ISO spelling.
pub fn to_iso(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs.fract() == 0.0 {
        format!("PT{}S", secs as i64)
    } else {
        let mut s = format!("{secs}");
        if !s.contains('.') {
            s.push_str(".0");
        }
        format!("PT{s}S")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_render() {
        assert_eq!(parse_iso8601("PT3S").unwrap(), Duration::from_secs(3));
        assert_eq!(
            parse_iso8601("PT1.5S").unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(parse_iso8601("PT").unwrap(), Duration::ZERO);
        assert_eq!(parse_iso8601("PT0S").unwrap(), Duration::ZERO);
        assert_eq!(parse_iso8601("P1D").unwrap(), Duration::from_secs(86400));
        assert_eq!(
            parse_iso8601("P1W").unwrap(),
            Duration::from_secs(7 * 86400)
        );
        assert_eq!(parse_iso8601("PT2M30S").unwrap(), Duration::from_secs(150));
        assert_eq!(parse_iso8601("PT1H").unwrap(), Duration::from_secs(3600));
        assert!(parse_iso8601("P1Y").is_err());
        assert!(parse_iso8601("P1M").is_err());
        assert!(parse_iso8601("soon").is_err());
        assert!(parse_iso8601("").is_err());
        assert_eq!(to_iso(Duration::from_secs(3)), "PT3S");
        assert_eq!(to_iso(Duration::from_millis(1500)), "PT1.5S");
    }
}
