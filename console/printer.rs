/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use clap::builder::styling::Style;
use itertools::Itertools;
use typedb_cli_common::{
    format_colored,
    printer::{STYLE_GREEN, STYLE_RED},
};
use typedb_driver::{
    IID, Replica, ReplicationRole, Server,
    answer::{ConceptDocument, ConceptRow},
    concept::{Concept, Value},
};

const TABLE_INDENT: &'static str = "   ";
const CONTENT_INDENT: &'static str = "    ";
const TABLE_DASHES: usize = 7;

fn println(string: &str) {
    println!("{}", string)
}

pub(crate) fn print_servers_table(servers: HashSet<Server>) {
    const COLUMN_NUM: usize = 5;
    #[derive(Debug)]
    struct Row {
        id: String,
        address: String,
        role: String,
        term: String,
        status: (String, Style),
    }

    let mut rows = Vec::new();
    rows.push(Row {
        id: "id".to_string(),
        address: "address".to_string(),
        role: "role".to_string(),
        term: "term".to_string(),
        status: ("status".to_string(), Style::new()),
    });

    for server in servers.into_iter().sorted_by_key(|server| server.id()) {
        let role = match server.role() {
            Some(ReplicationRole::Primary) => "primary",
            Some(ReplicationRole::Candidate) => "candidate",
            Some(ReplicationRole::Secondary) => "secondary",
            None => "",
        }
        .to_string();

        let term = server.term().map(|t| t.to_string()).unwrap_or_default();

        let status = match &server {
            Server::Available(_) => ("available".to_string(), STYLE_GREEN),
            Server::Unavailable { .. } => ("unavailable".to_string(), STYLE_RED),
        };

        rows.push(Row {
            id: server.id().to_string(),
            address: server.address().map(|address| address.to_string()).unwrap_or_default(),
            role,
            term,
            status,
        });
    }

    // Compute max content length per column (without padding)
    let mut width_id = 0usize;
    let mut width_address = 0usize;
    let mut width_role = 0usize;
    let mut width_term = 0usize;
    let mut width_status = 0usize;

    for r in &rows {
        width_id = width_id.max(r.id.len());
        width_address = width_address.max(r.address.len());
        width_role = width_role.max(r.role.len());
        width_term = width_term.max(r.term.len());
        width_status = width_status.max(r.status.0.len());
    }

    // Add 2 spaces per column (one at the beginning and one at the end)
    width_id += 2;
    width_address += 2;
    width_role += 2;
    width_term += 2;
    width_status += 2;

    fn print_cell(content: &str, width: usize, style: Option<Style>) {
        // One space on the left, one at the right, rest is extra right padding
        let content_len = content.len();
        let base_len = content_len + 2; // left + right space
        let extra_padding = width.saturating_sub(base_len);
        let styled_content = format_colored!(style.unwrap_or_default(), "{content}");
        print!(" {}{} ", styled_content, " ".repeat(extra_padding));
    }

    const PIPES_NUM: usize = COLUMN_NUM - 1;
    let total_width = width_id + width_address + width_role + width_term + width_status + PIPES_NUM;

    for (row_index, row) in rows.iter().enumerate() {
        print_cell(&row.id, width_id, None);
        print!("|");
        print_cell(&row.address, width_address, None);
        print!("|");
        print_cell(&row.role, width_role, None);
        print!("|");
        print_cell(&row.term, width_term, None);
        print!("|");
        print_cell(&row.status.0, width_status, Some(row.status.1));
        println!();

        if row_index == 0 {
            println!("{}", "-".repeat(total_width));
        }
    }
}

pub(crate) fn print_document(document: ConceptDocument) {
    // Note: inefficient, but easy...
    match serde_json::from_str::<serde_json::Value>(&document.into_json().to_string()) {
        Ok(parsed) => match serde_json::to_string_pretty(&parsed) {
            Ok(pretty) => {
                println(&pretty);
            }
            Err(err) => {
                println(&format!("Error trying to pretty-print JSON: {}", err));
            }
        },
        Err(err) => {
            println(&format!("Error trying to parse JSON: {}", err));
        }
    }
}

pub(crate) fn print_row(row: ConceptRow, is_first: bool) {
    let variable_column_width = row.get_column_names().iter().map(|s| s.len()).max().unwrap_or(0);
    if is_first {
        println(&line_dash_separator(variable_column_width));
    }
    println(&concept_row_display_string(&row, variable_column_width));
}

fn concept_row_display_string(concept_row: &ConceptRow, variable_column_width: usize) -> String {
    let column_names = concept_row.get_column_names();
    let content = column_names
        .iter()
        .map(|column_name| {
            let concept = concept_row.get(column_name).unwrap_or_else(|_| None);
            let mut string = String::new();
            string.push('$');
            string.push_str(column_name);
            string.push_str(&" ".repeat(variable_column_width - column_name.len() + 1));
            string.push_str("| ");
            string.push_str(&concept_display_string(concept));
            string
        })
        .collect::<Vec<_>>()
        .join("\n");

    let mut string = String::new();
    string.push_str(&indent(CONTENT_INDENT, &content));
    string.push('\n');
    string.push_str(&line_dash_separator(variable_column_width));
    string
}

fn concept_display_string(concept: Option<&Concept>) -> String {
    match concept {
        None => "".to_owned(),
        Some(concept) => match concept {
            Concept::EntityType(type_) => {
                format!("{}", format_type(&type_.label))
            }
            Concept::RelationType(type_) => {
                format!("{}", format_type(&type_.label))
            }
            Concept::RoleType(type_) => {
                format!("{}", format_type(&type_.label))
            }
            Concept::AttributeType(type_) => {
                format!("{}", format_type(&type_.label))
            }
            Concept::Entity(entity) => {
                format!(
                    "{}, {}",
                    entity.type_.as_ref().map(|t| format_isa(t.label())).unwrap_or(String::new()),
                    format_iid(&entity.iid),
                )
            }
            Concept::Relation(relation) => {
                format!(
                    "{}, {}",
                    relation.type_.as_ref().map(|t| format_isa(t.label())).unwrap_or(String::new()),
                    format_iid(&relation.iid),
                )
            }
            Concept::Attribute(attribute) => {
                format!(
                    "{} {}",
                    attribute.type_.as_ref().map(|t| format_isa(t.label())).unwrap_or(String::new()),
                    format_value(&attribute.value),
                )
            }
            Concept::Value(value) => format_value(&value),
        },
    }
}

fn format_type(label: &str) -> String {
    format!("\x1b[95m{}\x1b[0m", label)
}

fn format_iid(iid: &IID) -> String {
    format!("{} {}", format_keyword("iid"), iid)
}

fn format_isa(label: &str) -> String {
    format!("{} \x1b[95m{}\x1b[0m", format_keyword("isa"), label)
}

fn format_value(value: &Value) -> String {
    format!("{}", value)
}

fn format_keyword(keyword: &str) -> String {
    format!("\x1b[94m{}\x1b[0m", keyword)
}

fn indent(indent: &str, string: &str) -> String {
    string.split('\n').map(|s| format!("{}{}", indent, s)).collect::<Vec<_>>().join("\n")
}

fn line_dash_separator(additional_dashes_num: usize) -> String {
    indent(TABLE_INDENT, &"-".repeat(TABLE_DASHES + additional_dashes_num))
}
