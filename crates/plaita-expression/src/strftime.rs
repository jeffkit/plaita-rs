//! `strftime` — Python time formatting over chrono timestamps.
//!
//! Directive set mirrors the C89/Python common core; unknown directives pass
//! through literally (most libc/platform strftime implementations do).

use chrono::{Datelike, Timelike};

pub fn py_strftime(t: &chrono::DateTime<chrono::Local>, layout: &str) -> String {
    let chars: Vec<char> = layout.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '%' || i + 1 >= chars.len() {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        i += 1;
        let d = chars[i];
        i += 1;
        out.push_str(&directive(t, d));
    }
    out
}

fn weekday_mon0(t: &chrono::DateTime<chrono::Local>) -> u32 {
    t.weekday().num_days_from_monday()
}

fn day_of_year(t: &chrono::DateTime<chrono::Local>) -> u32 {
    t.ordinal()
}

fn iso_week(t: &chrono::DateTime<chrono::Local>) -> u32 {
    t.iso_week().week()
}

fn directive(t: &chrono::DateTime<chrono::Local>, d: char) -> String {
    match d {
        'Y' => format!("{:04}", t.year()),
        'y' => format!("{:02}", t.year() % 100),
        'm' => format!("{:02}", t.month()),
        'd' => format!("{:02}", t.day()),
        'e' => format!("{:2}", t.day()),
        'H' => format!("{:02}", t.hour()),
        'I' => {
            let h = t.hour12().1;
            format!("{h:02}")
        }
        'M' => format!("{:02}", t.minute()),
        'S' => format!("{:02}", t.second()),
        'f' => format!("{:06}", t.nanosecond() / 1_000),
        'p' => if t.hour() < 12 { "AM" } else { "PM" }.to_string(),
        'a' => t.format("%a").to_string(),
        'A' => t.format("%A").to_string(),
        'b' | 'h' => t.format("%b").to_string(),
        'B' => t.format("%B").to_string(),
        'j' => format!("{:03}", day_of_year(t)),
        'w' => t.weekday().num_days_from_sunday().to_string(),
        'u' => (weekday_mon0(t) + 1).to_string(),
        'U' => format!(
            "{:02}",
            (day_of_year(t) + 6 - t.weekday().num_days_from_sunday()) / 7
        ),
        'W' => format!("{:02}", (day_of_year(t) + 6 - weekday_mon0(t)) / 7),
        'G' => format!("{:04}", t.iso_week().year()),
        'V' => format!("{:02}", iso_week(t)),
        // plaita _fn_now/_fn_today 用 naive datetime：%z/%Z 渲染为空串
        'z' | 'Z' => String::new(),
        'c' => t.format("%a %b %e %H:%M:%S %Y").to_string(),
        'x' => t.format("%m/%d/%y").to_string(),
        'X' => t.format("%H:%M:%S").to_string(),
        'D' => t.format("%m/%d/%y").to_string(),
        'F' => t.format("%Y-%m-%d").to_string(),
        'T' => t.format("%H:%M:%S").to_string(),
        'R' => t.format("%H:%M").to_string(),
        '%' => "%".to_string(),
        'n' => "\n".to_string(),
        't' => "\t".to_string(),
        other => format!("%{other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn directives() {
        let t = chrono::Local.with_ymd_and_hms(2026, 3, 7, 9, 5, 3).unwrap();
        assert_eq!(py_strftime(&t, "%Y-%m-%d"), "2026-03-07");
        assert_eq!(py_strftime(&t, "%H:%M:%S"), "09:05:03");
        assert_eq!(py_strftime(&t, "%y%m%d"), "260307");
        assert_eq!(py_strftime(&t, "%j"), "066");
        assert_eq!(py_strftime(&t, "%I %p"), "09 AM");
        assert_eq!(py_strftime(&t, "%Y%m%d%H%M%S").len(), 14);
        assert_eq!(py_strftime(&t, "plain"), "plain");
        assert_eq!(py_strftime(&t, "%%"), "%");
    }
}
