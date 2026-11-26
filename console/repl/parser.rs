/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;

use typedb_driver::{Address, FileCompression, ServerRouting};
use typeql::common::error::TypeQLError;

use crate::repl::command::{CommandResult, ReplError};

pub(crate) const FILE_COMPRESSIONS: [(&str, FileCompression); 2] =
    [("uncompressed", FileCompression::Uncompressed), ("zstd", FileCompression::Zstd)];

pub(crate) fn get_word(input: &str) -> Option<usize> {
    if input.trim().is_empty() {
        None
    } else {
        let after_starting_whitespace = input.find(|c: char| !c.is_whitespace()).unwrap_or(0);
        match input[after_starting_whitespace..].find(char::is_whitespace) {
            None => Some(input.len()),
            Some(pos) => Some(after_starting_whitespace + pos),
        }
    }
}

/// Read a maximum-length query from the input.
/// This query must either be explicitly terminated with 'end', or be valid and have an empty following newline
/// If there is a valid query, and the newline occurs much later, we still return that newline
/// as that may the user's intended query end but there's a query parse error
pub(crate) fn parse_one_query(mut input: &str) -> Option<usize> {
    // We maximally try to parse as many lines into a query as we can.
    // If we fail and there is no parseable query, we return the full string
    match typeql::parse_query_from(input) {
        Ok((query, mut after_query_pos)) => {
            // Note: Query parsing may consume any trailing whitespace, which we should undo
            let query_string = &input[0..after_query_pos];
            let tail_whitespace_count = query_string.len() - query_string.trim_end().len();
            after_query_pos -= tail_whitespace_count;

            if query.has_explicit_end() {
                return Some(after_query_pos);
            } else {
                let remaining_input = &input[after_query_pos..];
                let after_newline_pos = find_empty_line(remaining_input);
                match after_newline_pos {
                    None => None,
                    Some(after_newline_pos) => Some(after_query_pos + after_newline_pos),
                }
            }
        }
        Err(err) => {
            // If we fail and there is no parseable query, we simply search for an empty newline and return that index
            // sometimes TypeQL will hit an error, and stop parsing at that line even though it's not the end of a query
            // this will degrade the query error pointer! So if we have a line number of the parsing error, we'll look for the newline
            // after that line, instead of just the first newline
            let mut start_line = 0;
            let mut start_col = 0;
            for error in err.errors() {
                if let TypeQLError::SyntaxErrorDetailed { error_line_nr, error_col, .. } = error {
                    let line_nr = *error_line_nr - 1;
                    if line_nr > start_line {
                        start_line = line_nr;
                        start_col = *error_col; //note: 1-indexed, but this works out to move the pos forward one to skip the first col
                    }
                }
            }
            let mut after_error_pos = 0;
            for _ in 0..start_line {
                const NEWLINE: &str = "\n";
                match input.find(NEWLINE) {
                    None => {
                        // unexpected, fall back behaviour
                        return find_empty_line(input);
                    }
                    Some(pos) => {
                        after_error_pos += pos + NEWLINE.len();
                        input = &input[pos + NEWLINE.len()..]
                    }
                }
            }
            after_error_pos += start_col;
            let remaining_input = &input[start_col..];
            let newline_after_error_pos = find_empty_line(remaining_input);
            match newline_after_error_pos {
                None => None,
                Some(newline_after_error_pos) => Some(after_error_pos + newline_after_error_pos),
            }
        }
    }
}

pub(crate) fn parse_server_routing(input: &[String]) -> CommandResult<ServerRouting> {
    match input.first() {
        Some(address) => {
            let address: Address =
                address.parse().map_err(|err: typedb_driver::Error| Box::new(err) as Box<dyn Error + Send>)?;
            Ok(ServerRouting::Direct { address })
        }
        None => Ok(ServerRouting::Auto),
    }
}

pub(crate) fn parse_file_compression(input: Option<&String>) -> CommandResult<FileCompression> {
    let Some(input) = input else {
        return Ok(FileCompression::Uncompressed);
    };
    match FILE_COMPRESSIONS.iter().find(|(name, _)| name == input) {
        Some((_, compression)) => Ok(*compression),
        None => Err(Box::new(ReplError {
            message: format!("Unrecognised compression '{input}', expected 'uncompressed' or 'zstd'."),
        })),
    }
}

fn find_empty_line(mut input: &str) -> Option<usize> {
    const PATTERN: &str = "\n";
    let mut pos = 0;
    while let Some(newline_pos) = input.find(PATTERN) {
        let after_newline_pos = newline_pos + 1;
        let next_newline_pos = match input[after_newline_pos..].find(PATTERN) {
            None => return None,
            Some(next_newline_pos) => after_newline_pos + next_newline_pos,
        };
        pos += after_newline_pos;
        if input[after_newline_pos..next_newline_pos].trim().is_empty() {
            // pos is at the same character as after_newline_pos in the original input
            return Some(pos + (next_newline_pos - after_newline_pos) + 1);
        }
        input = &input[after_newline_pos..];
    }
    None
}
