/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use duckdb::Connection;

use crate::{
    catalog::ColumnType,
    diagnostics::{Diagnostic, Diagnostics},
    mapping::{IID_COLUMN, MappingPlan, OwnsMapping, RoleBinding, TableMapping},
    schema::{Schema, ValueConstraints, ValueType},
    value::{MAX_EPOCH_DAYS, MAX_EPOCH_SECONDS, MIN_EPOCH_DAYS, MIN_EPOCH_SECONDS},
};

pub(crate) fn validate_data(conn: &Connection, schema: &Schema, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    validate_disjoint_instance_ids(conn, plan, diagnostics);
    // Values first: the checks after them read values cast to the attribute's value type,
    // which fails on a value the type cannot represent, so such columns are not checked
    // further.
    let mut unrepresentable = HashSet::new();
    for table in &plan.tables {
        for owns in &table.owns {
            let errors = diagnostics.errors.len();
            validate_values(conn, table, owns, diagnostics);
            validate_timestamp_precision(conn, plan, table, owns, diagnostics);
            if diagnostics.errors.len() > errors {
                unrepresentable.insert((table.table.as_str(), owns.column.as_str()));
            }
        }
    }
    let representable = |table: &TableMapping, owns: &OwnsMapping| {
        !unrepresentable.contains(&(table.table.as_str(), owns.column.as_str()))
    };

    for table in &plan.tables {
        validate_ownership_cardinalities(conn, schema, table, &representable, diagnostics);
        for owns in table.owns.iter().filter(|owns| representable(table, owns)) {
            if let Some(attribute) = schema.attribute(&owns.attribute) {
                validate_value_constraints(conn, table, owns, &owns.attribute, &attribute.constraints, diagnostics);
                for (supertype, constraints) in &attribute.inherited_constraints {
                    validate_value_constraints(conn, table, owns, supertype, constraints, diagnostics);
                }
            }
        }
        if table.is_relation {
            validate_role_cardinalities(conn, table, diagnostics);
            validate_players_imported_first(conn, table, diagnostics);
        }
    }
    validate_uniqueness(conn, plan, &representable, diagnostics);
    validate_plays_cardinalities(conn, plan, diagnostics);
    validate_plays_lower_bounds(conn, schema, plan, diagnostics);
}

/// Whether a column's values all passed the representability checks.
type Representable<'a> = dyn Fn(&TableMapping, &OwnsMapping) -> bool + 'a;

/// Tables mapped to the same type share its object-id space: an `_iid` used by two of
/// them would name one instance.
fn validate_disjoint_instance_ids(conn: &Connection, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    let mut labels: Vec<&str> = plan.tables.iter().map(|table| table.type_label.as_str()).collect();
    labels.sort_unstable();
    labels.dedup();
    for label in labels {
        let tables: Vec<&TableMapping> = plan.tables.iter().filter(|table| table.type_label == label).collect();
        if tables.len() < 2 {
            continue;
        }
        let union = tables
            .iter()
            .map(|table| format!("SELECT {} AS id FROM {}", quoted(&table.iid_column), quoted(&table.table)))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let count = query_count(
            conn,
            &format!("SELECT count(*) FROM (SELECT id FROM ({union}) GROUP BY id HAVING count(*) > 1)"),
        );
        report_count(diagnostics, count, |count| {
            let names: Vec<&str> = tables.iter().map(|table| table.table.as_str()).collect();
            format!(
                "tables {names:?} are all mapped to '{label}' and share {} {IID_COLUMN} value(s); \
                 instances of one type need distinct {IID_COLUMN} values across its tables",
                count,
            )
        });
    }
}

fn validate_ownership_cardinalities(
    conn: &Connection,
    schema: &Schema,
    table: &TableMapping,
    representable: &Representable<'_>,
    diagnostics: &mut Diagnostics,
) {
    let object = match table.is_relation {
        true => schema.relation(&table.type_label),
        false => schema.entity(&table.type_label),
    };
    for owns in object.iter().flat_map(|object| object.owns.iter()) {
        let (attribute, card_min, card_max) = (&owns.attribute, owns.card_min, owns.card_max);
        let columns: Vec<&OwnsMapping> =
            table.owns.iter().filter(|column| schema.is_attribute_subtype(&column.attribute, attribute)).collect();
        if columns.is_empty() {
            if card_min >= 1 {
                let violations = count_where(conn, &table.table, "true");
                report_count(diagnostics, violations, |violations| {
                    format!(
                        "table '{}': no column populates '{attribute}' or its subtypes, so its {violations} row(s) \
                         violate '{}' owns '{attribute}' @card({card_min}..)",
                        table.table, table.type_label,
                    )
                });
            }
            continue;
        }
        if !columns.iter().all(|column| representable(table, column)) {
            continue;
        }
        let values = columns.iter().map(|owns| owns.tagged_values()).collect::<Vec<_>>();
        let count = format!("len(list_distinct({}))", values.join(" || "));
        let names: Vec<&str> = columns.iter().map(|owns| owns.column.as_str()).collect();
        let source = if names.len() == 1 { format!("'{}'", names[0]) } else { format!("columns {names:?}") };

        if card_min >= 1 {
            let violations = count_where(conn, &table.table, &format!("{count} < {card_min}"));
            report_count(diagnostics, violations, |violations| {
                format!(
                    "table '{}': {} row(s) hold fewer than {card_min} distinct value(s) in {source}, violating \
                     '{}' owns '{attribute}' @card({card_min}..)",
                    table.table, violations, table.type_label,
                )
            });
        }
        if let Some(card_max) = card_max {
            let violations = count_where(conn, &table.table, &format!("{count} > {card_max}"));
            report_count(diagnostics, violations, |violations| {
                format!(
                    "table '{}': {} row(s) hold more than {card_max} distinct value(s) in {source}, violating \
                     '{}' owns '{attribute}' @card(..{card_max})",
                    table.table, violations, table.type_label,
                )
            });
        }
    }
}

fn validate_values(conn: &Connection, table: &TableMapping, owns: &OwnsMapping, diagnostics: &mut Diagnostics) {
    let mut check = |condition: String, problem: &str| {
        let count = count_values(conn, table, owns, &condition);
        report_count(diagnostics, count, |count| {
            format!("table '{}': {} value(s) in '{}' {problem}", table.table, count, owns.column)
        });
    };

    match &owns.column_type {
        ColumnType::Integer { always_fits_i64 } => {
            if !always_fits_i64 {
                check(
                    "v > 9223372036854775807 OR v < -9223372036854775808".to_owned(),
                    "exceed TypeDB's 64-bit integer range",
                );
            }
            if owns.value_type == ValueType::Double {
                check(
                    "v BETWEEN -9223372036854775808 AND 9223372036854775807 AND v::DOUBLE::HUGEINT <> v::HUGEINT"
                        .to_owned(),
                    &format!("have no exact double representation, which attribute '{}' needs", owns.attribute),
                );
            }
        }
        ColumnType::Float | ColumnType::Double => {
            check("isnan(v) OR isinf(v)".to_owned(), "are NaN or infinite, which a TypeDB double cannot be")
        }
        ColumnType::Decimal { width, scale } if owns.value_type == ValueType::Decimal => {
            if *scale > 19 {
                diagnostics.error(Diagnostic::data(format!(
                    "table '{}': column '{}' has DECIMAL scale {scale}, beyond TypeDB's 19 fractional digits",
                    table.table, owns.column
                )));
            } else if width - scale.min(width) > 18 {
                check(
                    "v >= 9223372036854775808 OR v < -9223372036854775808".to_owned(),
                    "exceed TypeDB's decimal integer range",
                );
            }
        }
        ColumnType::Date => check(
            format!(
                "CASE WHEN isinf(v) THEN true ELSE (v - DATE '1970-01-01') NOT BETWEEN {MIN_EPOCH_DAYS} AND \
                 {MAX_EPOCH_DAYS} END"
            ),
            "are infinite or outside TypeDB's date range",
        ),
        ColumnType::Timestamp { .. } | ColumnType::TimestampTz => check(
            format!(
                "CASE WHEN isinf(v) THEN true ELSE epoch(v) < {MIN_EPOCH_SECONDS} OR epoch(v) >= {} END",
                MAX_EPOCH_SECONDS + 1
            ),
            "are infinite or outside TypeDB's datetime range",
        ),
        ColumnType::Interval => {
            check("v::VARCHAR LIKE '%-%'".to_owned(), "have negative components, which a TypeDB duration cannot have");
            // DuckDB's client API hands out the time part in i64 nanoseconds.
            check(
                "datepart('hour', v)::HUGEINT * 3600000000 + datepart('minute', v) * 60000000 \
                 + datepart('microseconds', v) > 9223372036854775"
                    .to_owned(),
                "have a time part beyond 2562047 hours, which the loader cannot read exactly; express such spans \
                 in days or months",
            );
        }
        _ => {}
    }
}

fn validate_timestamp_precision(
    conn: &Connection,
    plan: &MappingPlan,
    table: &TableMapping,
    owns: &OwnsMapping,
    diagnostics: &mut Diagnostics,
) {
    if owns.column_type != (ColumnType::Timestamp { nanoseconds: false }) {
        return;
    }
    let has_nanosecond_source = plan.tables.iter().flat_map(|table| table.owns.iter()).any(|other| {
        other.attribute == owns.attribute && other.column_type == ColumnType::Timestamp { nanoseconds: true }
    });
    if !has_nanosecond_source {
        return;
    }
    // TIMESTAMP_NS spans i64 nanoseconds from the epoch; TRY_CAST raises instead of failing.
    let condition = "NOT isinf(v) AND abs(epoch_us(v)) > 9223372036854775";
    let count = count_values(conn, table, owns, condition);
    report_count(diagnostics, count, |count| {
        format!(
            "table '{}': {count} value(s) in '{}' are outside TIMESTAMP_NS's range (years 1677 to 2262), which \
             attribute '{}' needs because other columns populate it with TIMESTAMP_NS values",
            table.table, owns.column, owns.attribute
        )
    });
}

fn validate_value_constraints(
    conn: &Connection,
    table: &TableMapping,
    owns: &OwnsMapping,
    declared_on: &str,
    constraints: &ValueConstraints,
    diagnostics: &mut Diagnostics,
) {
    let value = owns.scalar("v");
    let check = |diagnostics: &mut Diagnostics, condition: String, description: &str| {
        let count = count_values(conn, table, owns, &condition);
        report_count(diagnostics, count, |count| {
            format!(
                "table '{}': {} value(s) in '{}' violate attribute '{declared_on}' {description}",
                table.table, count, owns.column,
            )
        });
    };
    let unchecked = |diagnostics: &mut Diagnostics, description: &str| {
        diagnostics.warning(Diagnostic::data(format!(
            "attribute '{declared_on}' {description} uses literals that cannot be checked in SQL; the server will \
             enforce it at import"
        )))
    };

    // TypeDB's @regex matches anywhere in the value, unless the pattern anchors itself. DuckDB
    // runs RE2, which rejects some of the syntax of TypeDB's regex engine and reads some
    // differently: those patterns are left to the server.
    if let Some(regex) = &constraints.regex {
        let pattern = sql_string(regex);
        let compiles = conn.query_row(&format!("SELECT regexp_matches('', {pattern})"), [], |_| Ok(())).is_ok();
        let description = format!("@regex({regex:?})");
        match compiles && !RE2_DIVERGENT_SYNTAX.iter().any(|syntax| regex.contains(syntax)) {
            true => check(diagnostics, format!("NOT regexp_matches({value}, {pattern})"), &description),
            false => unchecked(diagnostics, &description),
        }
    }
    if let Some(values) = &constraints.values {
        match values.iter().map(|literal| sql_literal(literal)).collect::<Option<Vec<_>>>() {
            Some(literals) => check(diagnostics, format!("{value} NOT IN ({})", literals.join(", ")), "@values(...)"),
            None => unchecked(diagnostics, "@values(...)"),
        }
    }
    for (bound, operator, name) in
        [(&constraints.range_min, "<", "@range lower bound"), (&constraints.range_max, ">", "@range upper bound")]
    {
        if let Some(bound) = bound {
            match sql_literal(bound) {
                Some(literal) => check(diagnostics, format!("{value} {operator} {literal}"), name),
                None => unchecked(diagnostics, name),
            }
        }
    }
}

const RE2_DIVERGENT_SYNTAX: [&str; 6] = ["\\b{", "\\<", "\\>", "&&", "--", "~~"];

fn validate_role_cardinalities(conn: &Connection, table: &TableMapping, diagnostics: &mut Diagnostics) {
    let mut role_names: Vec<&str> = table.roles.iter().map(|role| role.role.as_str()).collect();
    role_names.sort_unstable();
    role_names.dedup();
    for role_name in role_names {
        let bindings: Vec<&RoleBinding> = table.roles.iter().filter(|role| role.role == role_name).collect();
        let (card_min, card_max) = (bindings[0].card_min, bindings[0].card_max);
        let players = distinct_player_count(&bindings);
        let columns: Vec<&str> = bindings.iter().map(|role| role.column.as_str()).collect();
        if card_min >= 1 {
            let count = count_where(conn, &table.table, &format!("{players} < {card_min}"));
            report_count(diagnostics, count, |count| {
                format!(
                    "table '{}': {} row(s) fill role '{}:{role_name}' with fewer than {card_min} player(s) \
                     (columns {columns:?})",
                    table.table, count, table.type_label,
                )
            });
        }
        if let Some(card_max) = card_max {
            let count = count_where(conn, &table.table, &format!("{players} > {card_max}"));
            report_count(diagnostics, count, |count| {
                format!(
                    "table '{}': {} row(s) fill role '{}:{role_name}' with more than {card_max} player(s) \
                     (columns {columns:?}), violating its @card(..{card_max})",
                    table.table, count, table.type_label,
                )
            });
        }
    }
}

fn validate_players_imported_first(conn: &Connection, table: &TableMapping, diagnostics: &mut Diagnostics) {
    let bindings: Vec<&RoleBinding> = table.roles.iter().collect();
    let count = count_where(conn, &table.table, &format!("{} = 0", filled_count(&bindings)));
    report_count(diagnostics, count, |count| {
        format!(
            "table '{}': {} row(s) fill no role; TypeDB deletes a relation without role players, so these rows \
             would be lost",
            table.table, count,
        )
    });

    if table.reference_cycle.is_empty() {
        return;
    }
    let iid = quoted(&table.iid_column);
    let imported_first: Vec<String> = table
        .roles
        .iter()
        .filter_map(|role| {
            let column = quoted(&role.column);
            if !table.reference_cycle.contains(&role.player_table) {
                Some(format!("{column} IS NOT NULL"))
            } else if role.player_table == table.table {
                Some(format!("{column} < {iid}"))
            } else {
                None
            }
        })
        .collect();
    let condition = match imported_first.is_empty() {
        true => "true".to_owned(),
        false => format!("NOT ({})", imported_first.join(" OR ")),
    };
    let count = count_where(conn, &table.table, &format!("{} > 0 AND {condition}", filled_count(&bindings)));
    report_count(diagnostics, count, |count| {
        format!(
            "table '{}': {} row(s) have only role players that are imported after them: tables {:?} reference \
             each other, and TypeDB deletes a relation that has no role players when its transaction commits. \
             Give each row a player outside the cycle, or one that is an earlier row (by {}) of the same table",
            table.table, count, table.reference_cycle, table.iid_column,
        )
    });
}

fn validate_uniqueness(
    conn: &Connection,
    plan: &MappingPlan,
    representable: &Representable<'_>,
    diagnostics: &mut Diagnostics,
) {
    let mut scopes: Vec<(&str, &str)> = plan
        .tables
        .iter()
        .flat_map(|table| table.owns.iter())
        .filter(|owns| owns.unique)
        .map(|owns| (owns.attribute.as_str(), owns.declared_by.as_str()))
        .collect();
    scopes.sort_unstable();
    scopes.dedup();
    for (attribute, declared_by) in scopes {
        let columns: Vec<(&TableMapping, &OwnsMapping)> = plan
            .tables
            .iter()
            .flat_map(|table| table.owns.iter().map(move |owns| (table, owns)))
            .filter(|(_, owns)| owns.unique && owns.attribute == attribute && owns.declared_by == declared_by)
            .collect();
        if !columns.iter().all(|(table, owns)| representable(table, owns)) {
            continue;
        }
        let sources: Vec<String> = columns
            .iter()
            .map(|(table, owns)| {
                format!(
                    "SELECT {} AS t, {} AS i, {} AS v FROM {}",
                    sql_string(&table.table),
                    quoted(&table.iid_column),
                    owns.identity(&owns.value_expression()),
                    quoted(&table.table)
                )
            })
            .collect();
        let sql = format!(
            "SELECT count(*) FROM (SELECT v FROM (SELECT DISTINCT t, i, v FROM ({}) WHERE v IS NOT NULL) \
             GROUP BY v HAVING count(*) > 1)",
            sources.join(" UNION ALL ")
        );
        let count = query_count(conn, &sql);
        let key = plan
            .tables
            .iter()
            .flat_map(|table| table.owns.iter())
            .any(|owns| owns.key && owns.attribute == attribute && owns.declared_by == declared_by);
        let annotation = if key { "@key" } else { "@unique" };
        report_count(diagnostics, count, |count| {
            format!(
                "{} value(s) of '{attribute}' are owned by more than one instance, violating '{declared_by}' owns \
                 '{attribute}' {annotation}",
                count,
            )
        });
    }
}

fn validate_plays_cardinalities(conn: &Connection, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    let mut scopes: Vec<(&str, &str)> = plan
        .tables
        .iter()
        .flat_map(|table| table.roles.iter())
        .filter(|role| role.player_card_max.is_some())
        .map(|role| (role.player_table.as_str(), role.scoped_role.as_str()))
        .collect();
    scopes.sort_unstable();
    scopes.dedup();
    for (player_table, scoped_role) in scopes {
        let bindings: Vec<(&TableMapping, &RoleBinding)> = plan
            .tables
            .iter()
            .flat_map(|table| table.roles.iter().map(move |role| (table, role)))
            .filter(|(_, role)| role.player_table == player_table && role.scoped_role == scoped_role)
            .collect();
        let upper = bindings.iter().filter_map(|(_, role)| role.player_card_max).min().expect("filtered above");
        let references = bindings
            .iter()
            .map(|(table, role)| {
                format!(
                    "SELECT {} AS t, {} AS r, {} AS p FROM {} WHERE {2} IS NOT NULL",
                    sql_string(&table.table),
                    quoted(&table.iid_column),
                    quoted(&role.column),
                    quoted(&table.table)
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let sql = format!(
            "SELECT count(*) FROM (SELECT p FROM (SELECT DISTINCT t, r, p FROM ({references})) GROUP BY p HAVING count(*) > {upper})"
        );
        let count = query_count(conn, &sql);
        report_count(diagnostics, count, |count| {
            let columns: Vec<String> =
                bindings.iter().map(|(table, role)| format!("{}.{}", table.table, role.column)).collect();
            format!(
                "{} row(s) of '{player_table}' play '{scoped_role}' more than {upper} time(s) (through {columns:?}), \
                 violating plays '{scoped_role}' @card(..{upper})",
                count,
            )
        });
    }
}

fn validate_plays_lower_bounds(conn: &Connection, schema: &Schema, plan: &MappingPlan, diagnostics: &mut Diagnostics) {
    for player in &plan.tables {
        let object = match player.is_relation {
            true => schema.relation(&player.type_label),
            false => schema.entity(&player.type_label),
        };
        for plays in object.iter().flat_map(|object| object.plays.iter()).filter(|plays| plays.card_min >= 1) {
            let bindings: Vec<(&TableMapping, &RoleBinding)> = plan
                .tables
                .iter()
                .filter(|table| {
                    table.is_relation && schema.relation_ancestry(&table.type_label).contains(&plays.relation)
                })
                .flat_map(|table| table.roles.iter().map(move |role| (table, role)))
                .filter(|(_, role)| role.player_table == player.table && role.role == plays.role)
                .collect();
            let played = bindings
                .iter()
                .map(|(table, role)| {
                    format!(
                        "SELECT DISTINCT {} AS t, {} AS r, {} AS p FROM {}",
                        sql_string(&table.table),
                        quoted(&table.iid_column),
                        quoted(&role.column),
                        quoted(&table.table)
                    )
                })
                .collect::<Vec<_>>();
            let condition = match played.is_empty() {
                true => "true".to_owned(),
                false => format!(
                    "(SELECT count(*) FROM ({}) WHERE p = {}) < {}",
                    played.join(" UNION "),
                    quoted(&player.iid_column),
                    plays.card_min
                ),
            };
            let count = count_where(conn, &player.table, &condition);
            report_count(diagnostics, count, |count| {
                format!(
                    "table '{}': {} row(s) play '{}:{}' fewer than {} time(s), violating '{}' plays '{}:{}' @card({}..)",
                    player.table,
                    count,
                    plays.relation,
                    plays.role,
                    plays.card_min,
                    player.type_label,
                    plays.relation,
                    plays.role,
                    plays.card_min
                )
            });
        }
    }
}

fn count_values(conn: &Connection, table: &TableMapping, owns: &OwnsMapping, condition: &str) -> Result<i64, String> {
    let sql = format!(
        "SELECT count(*) FROM (SELECT {} AS v FROM {}) WHERE v IS NOT NULL AND ({condition})",
        owns.value_expression(),
        quoted(&table.table)
    );
    query_count(conn, &sql)
}

fn distinct_player_count(bindings: &[&RoleBinding]) -> String {
    let players = bindings
        .iter()
        .map(|role| {
            let column = quoted(&role.column);
            format!(
                "CASE WHEN {column} IS NULL THEN NULL ELSE {} || {column}::VARCHAR END",
                sql_string(&format!("{}/", role.player_table))
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("len(list_distinct(list_value({players})))")
}

fn filled_count(bindings: &[&RoleBinding]) -> String {
    if bindings.is_empty() {
        return "0".to_owned();
    }
    bindings
        .iter()
        .map(|role| format!("(CASE WHEN {} IS NULL THEN 0 ELSE 1 END)", quoted(&role.column)))
        .collect::<Vec<_>>()
        .join(" + ")
}

fn sql_literal(typeql_literal: &str) -> Option<String> {
    let literal = typeql_literal.trim();
    if let Some(inner) = literal.strip_prefix('"').and_then(|rest| rest.strip_suffix('"')) {
        return (!inner.contains('\\')).then(|| sql_string(inner));
    }
    let number = literal.strip_suffix("dec").unwrap_or(literal);
    let is_number = number.parse::<i64>().is_ok()
        || (number.parse::<f64>().is_ok_and(f64::is_finite) && number.chars().all(|c| "0123456789.-+eE".contains(c)));
    if is_number {
        return Some(number.to_owned());
    }
    if literal == "true" || literal == "false" {
        return Some(literal.to_owned());
    }
    if literal.len() == 10 && literal.as_bytes()[4] == b'-' && literal.as_bytes()[7] == b'-' {
        return Some(format!("DATE '{literal}'"));
    }
    None
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn count_where(conn: &Connection, table: &str, condition: &str) -> Result<i64, String> {
    query_count(conn, &format!("SELECT count(*) FROM {} WHERE {condition}", quoted(table)))
}

fn query_count(conn: &Connection, sql: &str) -> Result<i64, String> {
    conn.query_row(sql, [], |row| row.get::<_, i64>(0)).map_err(|err| err.to_string())
}

fn report_count(diagnostics: &mut Diagnostics, count: Result<i64, String>, message: impl Fn(i64) -> String) {
    match count {
        Ok(0) => {}
        Ok(count) => diagnostics.error(Diagnostic::data(message(count))),
        Err(err) => {
            diagnostics.error(Diagnostic::data(format!("a validation query failed to run ({err}) for: {}", message(0))))
        }
    }
}
