/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Crash reporting. Only panics are reported; expected user-facing errors are not.
//! Reports go to the TypeDB diagnostics relay (typedb-web, netlify/edge-functions/diagnostics-report), which maps
//! this provider-neutral payload onto whichever error tracker is current. The payload schema is documented there.

use std::{backtrace::Backtrace, env, panic, panic::PanicHookInfo, time::Duration};

use home::home_dir;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const REPORTING_URL: &str = "https://typedb.com/api/diagnostics/v1/report";

const ENV_URL: &str = "TYPEDB_CONSOLE_DIAGNOSTICS_URL";
const ENV_DEBUG: &str = "TYPEDB_CONSOLE_DIAGNOSTICS_DEBUG";
const ENV_TEST_PANIC: &str = "TYPEDB_CONSOLE_DIAGNOSTICS_TEST_PANIC";

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

struct Config {
    url: String,
    version: &'static str,
    debug: bool,
}

pub fn init(version: &'static str) {
    let url = env::var(ENV_URL).unwrap_or_else(|_| REPORTING_URL.to_owned());
    let debug = env::var(ENV_DEBUG).is_ok();
    let config = Config { url, version, debug };
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        previous_hook(info);
        report_panic(&config, info);
    }));
    if env::var(ENV_TEST_PANIC).is_ok() {
        panic!("diagnostics test panic");
    }
}

fn report_panic(config: &Config, info: &PanicHookInfo<'_>) {
    let message = panic_message(info);
    let backtrace = Backtrace::force_capture().to_string();
    let payload = build_payload(config, &message, &backtrace);
    if config.debug {
        eprintln!("[diagnostics] sending to {}:\n{}", config.url, serde_json::to_string_pretty(&payload).unwrap());
    }
    let agent = ureq::AgentBuilder::new().timeout(SEND_TIMEOUT).build();
    match agent.post(&config.url).set("Content-Type", "application/json").send_string(&payload.to_string()) {
        Ok(response) if config.debug => eprintln!("[diagnostics] sent, status {}", response.status()),
        Err(err) if config.debug => eprintln!("[diagnostics] send failed: {err}"),
        _ => (),
    }
}

fn build_payload(config: &Config, message: &str, backtrace: &str) -> Value {
    json!({
        "product": "console",
        "version": config.version,
        "install_id": install_id(),
        "os": env::consts::OS,
        "arch": env::consts::ARCH,
        "error": {
            "kind": "Panic",
            "message": message,
            "lang": "rust",
            "frames": parse_frames(backtrace),
            "backtrace": backtrace,
        },
    })
}

fn panic_message(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    let message = if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_owned()
    };
    match info.location() {
        Some(location) => format!("{message} (at {}:{})", location.file(), location.line()),
        None => message,
    }
}

// std backtrace text: "  N: function\n             at file:line:col"; innermost frame first
fn parse_frames(backtrace: &str) -> Vec<Value> {
    let mut frames: Vec<Value> = Vec::new();
    for line in backtrace.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("at ") {
            if let Some(frame) = frames.last_mut() {
                let (filename, lineno) = split_location(rest);
                frame["file"] = json!(filename);
                if let Some(lineno) = lineno {
                    frame["line"] = json!(lineno);
                }
            }
        } else if let Some((_, function)) = trimmed.split_once(": ") {
            let in_app = function.starts_with(crate_name());
            frames.push(json!({ "function": function, "in_app": in_app }));
        }
    }
    trim_to_short_backtrace(frames)
}

// same window RUST_BACKTRACE=1 shows: between std's end/begin short-backtrace markers, minus the panic machinery
fn trim_to_short_backtrace(frames: Vec<Value>) -> Vec<Value> {
    let function = |frame: &Value| frame["function"].as_str().unwrap_or("").to_owned();
    let Some(start) = frames.iter().position(|f| function(f).contains("__rust_end_short_backtrace")) else {
        return frames;
    };
    let end = frames.iter().position(|f| function(f).contains("__rust_begin_short_backtrace")).unwrap_or(frames.len());
    frames[start + 1..end]
        .iter()
        .skip_while(|f| {
            let name = function(f);
            name.contains("core::panicking::") || name.contains("rust_begin_unwind")
        })
        .cloned()
        .collect()
}

// crate name differs between cargo and bazel builds
fn crate_name() -> &'static str {
    module_path!().split("::").next().unwrap_or("")
}

// "file:line:col" or "file:line"
fn split_location(location: &str) -> (&str, Option<u64>) {
    let is_number = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
    let mut file = location;
    let mut line = None;
    for _ in 0..2 {
        match file.rsplit_once(':') {
            Some((rest, number)) if is_number(number) => {
                line = number.parse().ok();
                file = rest;
            }
            _ => break,
        }
    }
    (file, line)
}

// stable anonymous id per machine user; no raw username or path leaves the machine
fn install_id() -> String {
    let user = env::var("USER").or_else(|_| env::var("USERNAME")).unwrap_or_default();
    let home = home_dir().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default();
    let digest = Sha256::digest(format!("typedb-console:{user}:{home}").as_bytes());
    format!("{digest:x}")
}
