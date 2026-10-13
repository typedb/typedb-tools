/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub(crate) struct LoadStats {
    pub(crate) rows_attempted: usize,
    pub(crate) rows_committed: usize,
    pub(crate) rows_rejected: usize,
}

pub(crate) fn print_progress(stats: &LoadStats, started: Instant, bytes_done: u64, bytes_total: u64) {
    let elapsed = started.elapsed();
    let rate = format_rate(stats.rows_attempted as u64, elapsed);
    let bytes = format_bytes_progress(bytes_done, bytes_total);
    let eta = estimate_eta(bytes_done, bytes_total, elapsed)
        .map(|d| format!("ETA {}", format_duration(d)))
        .unwrap_or_else(|| "ETA --".to_owned());
    println!("[{}] {} rows  {}  bytes {}  {}", format_duration(elapsed), stats.rows_attempted, rate, bytes, eta,);
}

pub(crate) fn print_summary(stats: &LoadStats, started: Instant) {
    let elapsed = started.elapsed();
    println!("Loaded in {}.", format_duration(elapsed));
    println!("  Rows attempted: {:>8}", stats.rows_attempted);
    println!("  Rows committed: {:>8}", stats.rows_committed);
    println!("  Rows rejected:  {:>8}", stats.rows_rejected);
}

pub(crate) fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else if secs > 0 {
        format!("{}.{:01}s", secs, d.subsec_millis() / 100)
    } else {
        format!("0.{:01}s", d.subsec_millis() / 100)
    }
}

fn format_rate(rows: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return "-- rows/sec".to_owned();
    }
    let rate = rows as f64 / secs;
    if rate >= 1000.0 { format!("{:.1}k rows/sec", rate / 1000.0) } else { format!("{:.0} rows/sec", rate) }
}

fn format_bytes_progress(done: u64, total: u64) -> String {
    if total == 0 {
        return format!("{}", format_bytes(done));
    }
    let percent = (done as f64 / total as f64 * 100.0).clamp(0.0, 100.0);
    format!("{}/{} ({:.0}%)", format_bytes(done), format_bytes(total), percent)
}

fn format_bytes(n: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let n_f = n as f64;
    if n_f >= GB {
        format!("{:.2} GB", n_f / GB)
    } else if n_f >= MB {
        format!("{:.2} MB", n_f / MB)
    } else if n_f >= KB {
        format!("{:.1} KB", n_f / KB)
    } else {
        format!("{n} B")
    }
}

fn estimate_eta(bytes_done: u64, bytes_total: u64, elapsed: Duration) -> Option<Duration> {
    if bytes_done == 0 || bytes_done >= bytes_total {
        return None;
    }
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs <= 0.0 {
        return None;
    }
    let bytes_per_sec = bytes_done as f64 / elapsed_secs;
    if bytes_per_sec <= 0.0 {
        return None;
    }
    let remaining = (bytes_total - bytes_done) as f64;
    Some(Duration::from_secs_f64(remaining / bytes_per_sec))
}
