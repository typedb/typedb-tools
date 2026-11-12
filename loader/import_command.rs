/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::{self, Path};

use crate::cli::Args;

const CONSOLE: &str = "typedb console";
const PLAIN_PUNCTUATION: &str = if cfg!(windows) { "-_./:,=@+\\" } else { "-_./:,=@+" };

// As typed into a shell, with absolute paths, so that it runs from anywhere.
pub(crate) fn console_import_command(args: &Args, database: &str, schema_path: &Path, data_path: &Path) -> String {
    let mut command = String::from(CONSOLE);
    if let Some(addresses) = &args.addresses {
        command += &format!(" --address {}", shell_quoted(addresses));
    }
    if let Some(username) = &args.username {
        command += &format!(" --username {}", shell_quoted(username));
    }
    if args.tls_disabled.unwrap_or(false) {
        command += " --tls-disabled";
    }
    if let Some(root_ca) = &args.tls_root_ca {
        command += &format!(" --tls-root-ca {}", shell_quoted(root_ca));
    }
    let import = format!("database import {database} {} {}", absolute(schema_path), absolute(data_path));
    format!("{command} --command={}", shell_quoted(&import))
}

fn absolute(path: &Path) -> String {
    path::absolute(path).unwrap_or_else(|_| path.to_path_buf()).display().to_string()
}

fn shell_quoted(argument: &str) -> String {
    let is_plain = |character: char| character.is_ascii_alphanumeric() || PLAIN_PUNCTUATION.contains(character);
    if !argument.is_empty() && argument.chars().all(is_plain) {
        argument.to_owned()
    } else if cfg!(windows) {
        format!("\"{}\"", argument.replace('"', "\\\""))
    } else {
        format!("'{}'", argument.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn command(options: &[&str], schema_path: &str, data_path: &str) -> String {
        let args = Args::parse_from(["typedb-loader", "--duckdb", "x.duckdb"].iter().chain(options));
        console_import_command(&args, "db", Path::new(schema_path), Path::new(data_path))
    }

    #[cfg(unix)]
    #[test]
    fn the_import_command_survives_a_shell() {
        assert_eq!(
            command(&["--address", "localhost:1729", "--username", "admin", "--tls-disabled"], "/s.tql", "/out/db.d"),
            "typedb console --address localhost:1729 --username admin --tls-disabled \
             --command='database import db /s.tql /out/db.d'"
        );
        assert_eq!(
            command(&["--username", "it's me", "--tls-root-ca", "/my certs/ca.pem"], "/s.tql", "/out/db.d"),
            r#"typedb console --username 'it'\''s me' --tls-root-ca '/my certs/ca.pem' "#.to_owned()
                + "--command='database import db /s.tql /out/db.d'"
        );
    }

    #[cfg(windows)]
    #[test]
    fn the_import_command_survives_a_shell() {
        assert_eq!(
            command(&["--address", "localhost:1729", "--username", "my user"], r"C:\data\s.tql", r"C:\out\db.d"),
            r#"typedb console --address localhost:1729 --username "my user" "#.to_owned()
                + r#"--command="database import db C:\data\s.tql C:\out\db.d""#
        );
    }
}
