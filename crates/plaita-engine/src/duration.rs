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
/// `"PT"` parses as an explicit zero budget. Components are order-sensitive
/// with single occurrences (the Go `isoPattern`: date `nW` then `nD`, time
/// `nH` then `nM` then `nS`) — `PT1S1H` / `PT1H1H` / `P1D1D` are illegal
/// spellings, never sums (#13). Numbers are `\d+(\.\d+)?`: `PT.5S` is not a
/// legal component (Go regexp / Python isodate both reject the leading dot).
/// Out-of-range values (seconds past the `Duration` domain, overflowing
/// component arithmetic) are a named error — fail-closed, never a panic
/// (std `from_secs_f64` panics in every profile).
pub fn parse_iso8601(s: &str) -> Result<Duration, DurationError> {
    let s = s.trim();
    let err = |m: &str| Err(DurationError(m.to_string()));
    let unparseable = |s: &str| DurationError(format!("could not parse duration string {s:?}"));
    let bad_order = |s: &str| {
        DurationError(format!(
            "could not parse duration string {s:?}: duplicate or out-of-order duration component"
        ))
    };
    let unrepresentable = |s: &str| DurationError(format!("duration {s:?} is not representable"));
    let rest = match s.strip_prefix('P') {
        Some(r) => r,
        None => return err(&format!("could not parse duration string {s:?}")),
    };
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };

    let mut days: i64 = 0;
    // date components: at most one nW then at most one nD (Y/M rejected)
    let mut num = String::new();
    let mut last_rank: i32 = -1;
    for c in date_part.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: i64 = if num.is_empty() {
            return Err(unparseable(s));
        } else {
            num.parse()
                .map_err(|_| DurationError(format!("invalid duration component in {s:?}")))?
        };
        num.clear();
        let rank = match c {
            'Y' | 'M' => {
                return err(&format!(
                    "duration {s:?} contains year/month components, which have no fixed length"
                ))
            }
            'W' => 0,
            'D' => 1,
            _ => return Err(unparseable(s)),
        };
        if rank <= last_rank {
            return Err(bad_order(s));
        }
        last_rank = rank;
        match c {
            'W' => {
                let w = n.checked_mul(7).ok_or_else(|| unrepresentable(s))?;
                days = days.checked_add(w).ok_or_else(|| unrepresentable(s))?;
            }
            _ => days = days.checked_add(n).ok_or_else(|| unrepresentable(s))?,
        }
    }
    if !num.is_empty() {
        return Err(unparseable(s));
    }

    let mut secs = days as f64 * 86400.0;
    if let Some(tp) = time_part {
        // time components: at most one nH, then nM, then nS
        let chars: Vec<char> = tp.chars().collect();
        let mut i = 0usize;
        let mut last_rank: i32 = -1;
        while i < chars.len() {
            if !chars[i].is_ascii_digit() {
                return Err(unparseable(s));
            }
            // scan \d+(\.\d+)? — a leading dot is not a number
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i < chars.len() && chars[i] == '.' {
                i += 1;
                if i >= chars.len() || !chars[i].is_ascii_digit() {
                    return Err(unparseable(s));
                }
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let v: f64 = chars[start..i]
                .iter()
                .collect::<String>()
                .parse()
                .map_err(|_| DurationError(format!("invalid duration component in {s:?}")))?;
            let rank = match chars.get(i) {
                Some('H') => 0,
                Some('M') => 1,
                Some('S') => 2,
                _ => return Err(unparseable(s)), // 尾数字无单位 / 非法单位
            };
            i += 1;
            if rank <= last_rank {
                return Err(bad_order(s));
            }
            last_rank = rank;
            secs += match rank {
                0 => v * 3600.0,
                1 => v * 60.0,
                _ => v,
            };
        }
    }

    if secs.is_nan() || secs.is_infinite() {
        return err(&format!("invalid duration component in {s:?}"));
    }
    Duration::try_from_secs_f64(secs).map_err(|_| unrepresentable(s))
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

    #[test]
    fn out_of_range_is_named_error_not_panic() {
        let cases = [
            "P999999999999999D",       // days → secs past the u64 seconds domain
            "PT99999999999999999999S", // seconds past the u64 seconds domain
            "P9223372036854775807W", // n*7 overflows i64 (debug); wraps then hits from_secs_f64 (release)
            "P9223372036854775807D", // days accumulation overflows i64
        ];
        for s in cases {
            let r = std::panic::catch_unwind(|| parse_iso8601(s))
                .expect("parse_iso8601 must not panic on out-of-range durations");
            assert!(
                r.is_err(),
                "out-of-range {s:?} must be a named error, got {r:?}"
            );
        }
        // big-but-representable budgets keep parsing
        assert!(parse_iso8601("P106751991167301D").is_ok());
    }

    #[test]
    fn duplicate_or_out_of_order_components_rejected() {
        // #13: Go isoPattern / Python isodate 都拒乱序与重复分量——那是非法
        // 拼写，不是求和（旧解析器把 PT1S1S 当 2s、PT1S1H 当 3601s 预算生效）
        let cases = [
            "PT1S1H", // S 在 H 后：乱序
            "PT1M1H", // H 在 M 后：乱序
            "PT1H1H", // 重复 H
            "PT1M1M", // 重复 M
            "PT1S1S", // 重复 S（求和机制的秒级实证形状）
            "P1D1D",  // 重复 D
            "P1W1W",  // 重复 W
            "P1D1W",  // W 必须在 D 前（Go isoPattern 顺序）
        ];
        for s in cases {
            let e = parse_iso8601(s).expect_err(s);
            assert!(
                e.0.contains("could not parse duration string")
                    && e.0.contains("duplicate or out-of-order"),
                "{s:?}: {e}"
            );
        }
        // 尾部数字无单位是普通解析失败（非乱序族）
        let e = parse_iso8601("PT1H2").expect_err("PT1H2");
        assert!(e.0.contains("could not parse duration string"), "{e}");
    }

    #[test]
    fn strict_order_legal_surface_kept() {
        // #13 正控：跳分量合法（H→S 跳过 M）、周+日、分数秒照旧
        assert_eq!(parse_iso8601("PT1H1S").unwrap(), Duration::from_secs(3601));
        assert_eq!(parse_iso8601("PT0.3S").unwrap(), Duration::from_millis(300));
        assert_eq!(
            parse_iso8601("P1W1D").unwrap(),
            Duration::from_secs(8 * 86400)
        );
        // 对齐 Go \d+(\.\d+)? 与 Python isodate：前导点不接受
        assert!(parse_iso8601("PT.5S").is_err());
    }
}
