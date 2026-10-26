/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Schema-coverage harness, skipped unless SCHEMAS_DIR is set: feeds a directory of
//! .tql define queries (e.g. extracted from typedb-behaviour) through `Migration::prepare` with an
//! empty database and reports which fail to parse into the schema model.
//!
//!     SCHEMAS_DIR=<dir> cargo test -p typedb-migration --test behaviour-schemas -- --nocapture

use std::fs;

use typedb_migration::{Migration, PrepareError};

#[test]
fn parse_all_behaviour_schemas() {
    let Ok(dir) = std::env::var("SCHEMAS_DIR") else {
        eprintln!("SCHEMAS_DIR not set; skipping");
        return;
    };
    let empty_db = std::env::temp_dir().join(format!("empty-{}.duckdb", std::process::id()));
    let _ = fs::remove_file(&empty_db);
    drop(duckdb::Connection::open(&empty_db).unwrap());

    let (mut ok, mut setup_failures) = (0, Vec::new());
    for entry in fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("tql") {
            continue;
        }
        let schema = fs::read_to_string(&path).unwrap();
        match Migration::prepare(&empty_db, &schema, &std::env::temp_dir()) {
            Ok(_) | Err(PrepareError::Invalid { .. }) => ok += 1,
            Err(PrepareError::Setup(message)) => {
                setup_failures.push((path.file_name().unwrap().to_string_lossy().into_owned(), message));
            }
        }
    }
    println!("parsed OK (or valid mapping issues): {ok}");
    println!("setup failures: {}", setup_failures.len());
    let mut by_message: std::collections::BTreeMap<String, (usize, String)> = Default::default();
    for (file, message) in &setup_failures {
        // group by the message shape: strip quoted names and digits
        let mut key = String::new();
        let mut in_quote = false;
        for c in message.chars().take(160) {
            match c {
                '\'' => {
                    in_quote = !in_quote;
                    key.push('\'');
                }
                _ if in_quote => {}
                c if c.is_ascii_digit() => {}
                c => key.push(c),
            }
        }
        let entry = by_message.entry(key).or_insert((0, file.clone()));
        entry.0 += 1;
    }
    for (message, (count, example)) in by_message {
        println!("  [{count}x] {} (e.g. {example})", message.replace('\n', " / "));
    }
    let _ = fs::remove_file(&empty_db);
}
