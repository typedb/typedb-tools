/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! End-to-end tests of the migration library, without a server: a DuckDB fixture is
//! built programmatically, converted through the public API, and the output is decoded
//! and checked for the import format's guarantees (item ordering, scoped role labels,
//! checksum consistency, deterministic ids).

use std::{error::Error, fs, path::Path};

use duckdb::Connection;
use prost::Message;
use typedb_migration::{Migration, PrepareError};
use typedb_protocol::migration::{Item, item, migration_value};

const SCHEMA: &str = r#"define
  attribute name value string;
  attribute age value integer;
  attribute salary value decimal;
  attribute tag value string;
  entity person,
    owns name @card(1..1),
    owns age,
    owns tag @card(0..10),
    plays employment:employee,
    plays mentorship:student,
    plays mentorship:mentor @card(0..2);
  entity company,
    owns name @card(1..1),
    plays employment:employer;
  relation employment,
    relates employee @card(1..1),
    relates employer @card(1..1),
    owns salary;
  relation mentorship,
    relates student @card(1..1),
    relates mentor @card(1..1);
"#;

/// Every role the schema declares is filled: person plays three roles across two relation
/// tables, and mentorship needs a directive because both of its FK columns target person.
fn build_fixture(db_path: &Path) -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        r#"
        CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL, age INTEGER, tags VARCHAR[]);
        COMMENT ON COLUMN person.tags IS '@typedb owns tag';
        CREATE TABLE company (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL);
        CREATE TABLE employment (
            _iid UBIGINT NOT NULL UNIQUE,
            person_id UBIGINT NOT NULL REFERENCES person(_iid),
            company_id UBIGINT NOT NULL REFERENCES company(_iid),
            salary DECIMAL(12,2),
            PRIMARY KEY (person_id, company_id)
        );
        CREATE TABLE mentorship (
            _iid UBIGINT PRIMARY KEY,
            mentor_id UBIGINT NOT NULL REFERENCES person(_iid),
            student_id UBIGINT NOT NULL REFERENCES person(_iid)
        );
        COMMENT ON TABLE mentorship IS '@typedb relation mentorship (mentor = mentor_id, student = student_id)';
        INSERT INTO person VALUES (1, 'Alice', 30, ['a','b']), (2, 'Bob', NULL, NULL);
        INSERT INTO company VALUES (10, 'Acme');
        INSERT INTO employment VALUES (100, 1, 10, 50000.25), (101, 2, 10, NULL);
        INSERT INTO mentorship VALUES (200, 1, 2);
        "#,
    )?;
    Ok(())
}

/// Prepares and runs a migration of the fixture, returning the data file's path.
fn run_migration(dir: &Path) -> Result<std::path::PathBuf, PrepareError> {
    let schema_text = fs::read_to_string(dir.join("schema.tql")).expect("schema is written by setup");
    let migration = Migration::prepare(&dir.join("fixture.duckdb"), &schema_text)?;
    let out = dir.join("out");
    fs::create_dir_all(&out).expect("output dir");
    let data_path = out.join("fixture.data.typedb");
    let file = fs::File::create(&data_path).expect("output file");
    migration.run(file, "duckdb:fixture.duckdb").expect("migration runs");
    Ok(data_path)
}

fn setup(test_name: &str) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!("typedb-migration-{test_name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    build_fixture(&dir.join("fixture.duckdb"))?;
    fs::write(dir.join("schema.tql"), SCHEMA)?;
    Ok(dir)
}

fn decode_items(data: &[u8]) -> Result<Vec<item::Item>, Box<dyn Error>> {
    let mut buf = data;
    let mut items = Vec::new();
    while !buf.is_empty() {
        let len = prost::decode_length_delimiter(&mut buf)?;
        let (message, rest) = buf.split_at(len);
        buf = rest;
        items.push(Item::decode(message)?.item.expect("item is set"));
    }
    Ok(items)
}

#[test]
fn converts_and_produces_a_well_formed_import_file() -> Result<(), Box<dyn Error>> {
    let dir = setup("clean")?;
    let data_path = run_migration(&dir).expect("validation passes");

    let items = decode_items(&fs::read(&data_path)?)?;
    assert_references_resolve(&items);

    // Checksums match the item stream.
    let item::Item::Checksums(checksums) = items.last().unwrap() else { panic!() };
    let attributes: Vec<_> =
        items.iter().filter_map(|i| if let item::Item::Attribute(a) = i { Some(a) } else { None }).collect();
    let entities: Vec<_> =
        items.iter().filter_map(|i| if let item::Item::Entity(e) = i { Some(e) } else { None }).collect();
    let relations: Vec<_> =
        items.iter().filter_map(|i| if let item::Item::Relation(r) = i { Some(r) } else { None }).collect();
    assert_eq!(checksums.attribute_count, attributes.len() as i64);
    assert_eq!(checksums.entity_count, entities.len() as i64);
    assert_eq!(checksums.relation_count, relations.len() as i64);
    let ownerships: usize =
        entities.iter().map(|e| e.attributes.len()).chain(relations.iter().map(|r| r.attributes.len())).sum();
    assert_eq!(checksums.ownership_count, ownerships as i64);
    let role_players: usize = relations.iter().flat_map(|r| r.roles.iter().map(|role| role.players.len())).sum();
    assert_eq!(checksums.role_count, role_players as i64);

    // 2 persons + 1 company; 2 employments + 1 mentorship; every distinct name/age/tag/salary value.
    assert_eq!(entities.len(), 3);
    assert_eq!(relations.len(), 3);
    assert_eq!(attributes.len(), 3 + 1 + 2 + 1); // names, age, tags, salary

    // Every declared role is filled, with its relation-scoped label.
    let mut role_labels: Vec<&str> =
        relations.iter().flat_map(|r| r.roles.iter().map(|role| role.label.as_str())).collect();
    role_labels.sort();
    role_labels.dedup();
    assert_eq!(role_labels, ["employment:employee", "employment:employer", "mentorship:mentor", "mentorship:student"]);

    // Attribute ids are deduplicated and referenced.
    let attribute_ids: std::collections::HashSet<&str> = attributes.iter().map(|a| a.id.as_str()).collect();
    assert_eq!(attribute_ids.len(), attributes.len(), "attribute ids are not unique");
    for entity in &entities {
        for owned in &entity.attributes {
            assert!(attribute_ids.contains(owned.id.as_str()), "dangling ownership {}", owned.id);
        }
    }

    // Deterministic: converting again produces byte-identical output.
    let first = fs::read(&data_path)?;
    let data_path = run_migration(&dir).expect("validation passes");
    let second = fs::read(&data_path)?;
    assert_eq!(first, second, "migration output is not deterministic");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// Attributes, then entities, then relations: ownerships and role players reference items
/// already streamed, so the importer resolves them on arrival instead of parking them as
/// pending. Only relation tables referencing each other in a cycle can reference ahead.
#[test]
fn items_are_streamed_attributes_then_entities_then_relations() -> Result<(), Box<dyn Error>> {
    let dir = setup("order")?;
    let data_path = run_migration(&dir).expect("validation passes");
    let items = decode_items(&fs::read(&data_path)?)?;

    let kinds: Vec<&str> = items
        .iter()
        .map(|item| match item {
            item::Item::Header(_) => "header",
            item::Item::Attribute(_) => "attribute",
            item::Item::Entity(_) => "entity",
            item::Item::Relation(_) => "relation",
            item::Item::Checksums(_) => "checksums",
        })
        .collect();
    let mut runs = kinds.clone();
    runs.dedup();
    assert_eq!(runs, ["header", "attribute", "entity", "relation", "checksums"], "items out of order: {kinds:?}");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// A relation is a table, as in TypeDB: an FK column on an entity table has no TypeDB form
/// and is rejected with the staging SQL for the relation table it stands for.
#[test]
fn fk_column_on_entity_table_is_rejected() -> Result<(), Box<dyn Error>> {
    let dir = setup("entity-fk")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch("CREATE TABLE pet (_iid UBIGINT PRIMARY KEY, owner_id UBIGINT REFERENCES person(_iid));")?;
    drop(conn);
    let schema = fs::read_to_string(dir.join("schema.tql"))?.replace("entity person,", "entity pet;\n  entity person,");
    fs::write(dir.join("schema.tql"), schema)?;

    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected a structure error for the FK column on an entity table");
    };
    assert!(
        errors.iter().any(|e| e.message.contains("owner_id") && e.message.contains("INSERT INTO")),
        "no error suggests staging a relation table: {errors:?}"
    );

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn plays_cardinality_is_validated() -> Result<(), Box<dyn Error>> {
    let dir = setup("plays-card")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    // Alice mentors three times, violating `plays mentorship:mentor @card(0..2)`.
    conn.execute_batch(
        "INSERT INTO person VALUES (3, 'Carol', NULL, NULL), (4, 'Dan', NULL, NULL);
         INSERT INTO mentorship VALUES (201, 1, 3), (202, 1, 4);",
    )?;
    drop(conn);

    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected a data error for the plays cardinality");
    };
    assert!(
        errors.iter().any(|e| e.message.contains("mentor_id") && e.message.contains("@card(..2)")),
        "no error names the plays cardinality: {errors:?}"
    );

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// TypeDB compares strings byte-wise: a NOCASE column must not merge 'Bob' and 'BOB'
/// into one attribute, and list columns of string-like types keep every element.
#[test]
fn string_values_are_distinct_byte_wise() -> Result<(), Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!("typedb-migration-collation-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch(
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR COLLATE NOCASE, tags UUID[]);
         COMMENT ON COLUMN person.tags IS '@typedb owns tag';
         INSERT INTO person VALUES
             (1, 'Bob', ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002']),
             (2, 'BOB', NULL);",
    )?;
    drop(conn);
    fs::write(
        dir.join("schema.tql"),
        "define attribute name value string; attribute tag value string; entity person, owns name, owns tag @card(0..);",
    )?;

    let data_path = run_migration(&dir).expect("validation passes");
    let items = decode_items(&fs::read(&data_path)?)?;
    let attributes = items.iter().filter(|item| matches!(item, item::Item::Attribute(_))).count();
    assert_eq!(attributes, 2 + 2, "two names and two tags expected");
    let owned: usize = items
        .iter()
        .filter_map(|item| if let item::Item::Entity(entity) = item { Some(entity.attributes.len()) } else { None })
        .sum();
    assert_eq!(owned, 2 + 1 + 0 + 1);

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn unmapped_column_fails_and_writes_nothing() -> Result<(), Box<dyn Error>> {
    let dir = setup("unmapped")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch("ALTER TABLE person ADD COLUMN mystery VARCHAR;")?;
    drop(conn);

    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected validation errors");
    };
    assert!(errors.iter().any(|e| e.message.contains("mystery")), "no error names the column: {errors:?}");
    assert!(!dir.join("out/fixture.data.typedb").exists(), "output written despite errors");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn data_violations_fail_before_output() -> Result<(), Box<dyn Error>> {
    let dir = setup("violations")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    // Twelve distinct tags violate `owns tag @card(0..10)`. Seeded with an INSERT:
    // DuckDB cannot UPDATE rows referenced by a foreign key.
    conn.execute_batch("INSERT INTO person VALUES (7, 'Zed', 1, ['a','b','c','d','e','f','g','h','i','j','k','l']);")?;
    drop(conn);

    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected validation errors");
    };
    assert!(errors.iter().any(|e| e.message.contains("tag")), "no error names the ownership: {errors:?}");
    assert!(!dir.join("out/fixture.data.typedb").exists());

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn tables_mapped_to_one_type_need_disjoint_iids() -> Result<(), Box<dyn Error>> {
    let dir = setup("shared-type")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch(
        "CREATE TABLE contractor (_iid UBIGINT PRIMARY KEY, name VARCHAR NOT NULL);
         COMMENT ON TABLE contractor IS '@typedb entity person';
         INSERT INTO contractor VALUES (3, 'Carol');",
    )?;
    drop(conn);
    run_migration(&dir).expect("disjoint ids across tables of one type are accepted");

    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch("INSERT INTO contractor VALUES (1, 'Dan');")?;
    drop(conn);
    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected a data error for the _iid shared by person and contractor");
    };
    assert!(
        errors.iter().any(|e| e.message.contains("contractor") && e.message.contains("_iid")),
        "no error names the overlapping tables: {errors:?}"
    );

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn missing_iid_column_is_rejected() -> Result<(), Box<dyn Error>> {
    let dir = setup("no-iid")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch("CREATE TABLE project (id BIGINT PRIMARY KEY, name VARCHAR);")?;
    drop(conn);
    let schema = fs::read_to_string(dir.join("schema.tql"))? + "define entity project, owns name;\n";
    // a second define query would not parse; append to the single one instead
    let schema = schema.replace("define entity project", "  entity project");
    fs::write(dir.join("schema.tql"), schema)?;

    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected a structure error for the missing _iid column");
    };
    assert!(
        errors.iter().any(|e| e.message.contains("_iid") && e.message.contains("project")),
        "no error names the contract: {errors:?}"
    );

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn wrongly_typed_iid_column_is_rejected() -> Result<(), Box<dyn Error>> {
    let dir = setup("bad-iid")?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch("CREATE TABLE project (_iid BIGINT PRIMARY KEY, name VARCHAR);")?;
    drop(conn);
    let schema = fs::read_to_string(dir.join("schema.tql"))?
        .replace("entity person,", "entity project, owns name;\n  entity person,");
    fs::write(dir.join("schema.tql"), schema)?;

    let Err(PrepareError::Invalid { errors, .. }) = run_migration(&dir) else {
        panic!("expected a structure error for the BIGINT _iid column");
    };
    assert!(errors.iter().any(|e| e.message.contains("UBIGINT")), "no error names the required type: {errors:?}");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// Roles inherited from a super-relation must be emitted with the declaring relation's
/// scope: a real server rejects `commenting:author` when `author` belongs to
/// `content-engagement`. Specialised parent roles must not resurface as unfilled roles.
#[test]
fn inherited_roles_are_scoped_to_their_declaring_relation() -> Result<(), Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!("typedb-migration-scoping-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    let conn = Connection::open(dir.join("fixture.duckdb"))?;
    conn.execute_batch(
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE post (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE posting (
             _iid UBIGINT PRIMARY KEY,
             author_id UBIGINT NOT NULL REFERENCES person(_iid),
             post_id UBIGINT NOT NULL REFERENCES post(_iid)
         );
         INSERT INTO person VALUES (1);
         INSERT INTO post VALUES (2);
         INSERT INTO posting VALUES (3, 1, 2);",
    )?;
    drop(conn);
    fs::write(
        dir.join("schema.tql"),
        r#"define
          entity person, plays interaction:author;
          entity post, plays posting:post;
          relation interaction @abstract, relates subject @card(1), relates author as subject;
          relation posting, sub interaction, relates post @card(1);
        "#,
    )?;

    let data_path = run_migration(&dir).expect("validation passes");
    let items = decode_items(&fs::read(&data_path)?)?;
    let relation = items
        .iter()
        .find_map(|i| if let item::Item::Relation(r) = i { Some(r) } else { None })
        .expect("one relation emitted");
    let mut labels: Vec<&str> = relation.roles.iter().map(|role| role.label.as_str()).collect();
    labels.sort();
    assert_eq!(labels, ["interaction:author", "posting:post"], "roles must carry the declaring relation's scope");

    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// A database and schema of their own, for tests that need neither the shared fixture nor
/// a server.
fn scratch(test_name: &str, schema: &str, sql: &str) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let dir = std::env::temp_dir().join(format!("typedb-migration-{test_name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;
    Connection::open(dir.join("fixture.duckdb"))?.execute_batch(sql)?;
    fs::write(dir.join("schema.tql"), schema)?;
    Ok(dir)
}

fn prepare(dir: &Path) -> Result<Migration, PrepareError> {
    Migration::prepare(&dir.join("fixture.duckdb"), &fs::read_to_string(dir.join("schema.tql")).expect("schema"))
}

/// The validation errors, which the test expects there to be.
fn validation_errors(dir: &Path) -> Vec<String> {
    match prepare(dir) {
        Err(PrepareError::Invalid { errors, .. }) => errors.into_iter().map(|error| error.message).collect(),
        Err(PrepareError::Setup(message)) => panic!("expected validation errors, got a setup error: {message}"),
        Ok(_) => panic!("expected validation errors, but validation passed"),
    }
}

fn convert(dir: &Path) -> Result<Vec<item::Item>, Box<dyn Error>> {
    let migration = prepare(dir).map_err(|err| format!("validation failed: {err:?}"))?;
    let path = dir.join("out.data.typedb");
    migration.run(fs::File::create(&path)?, "duckdb:fixture.duckdb")?;
    decode_items(&fs::read(&path)?)
}

fn assert_error(errors: &[String], fragments: &[&str]) {
    assert!(
        errors.iter().any(|error| fragments.iter().all(|fragment| error.contains(fragment))),
        "no error mentions all of {fragments:?}: {errors:#?}"
    );
}

const NODES: &str = r#"define
  attribute name value string;
  entity node, owns name, plays link:a, plays link:b;
  relation link, relates a, relates b, owns name;
"#;

/// TypeDB deletes a relation without role players at commit: such rows would vanish.
#[test]
fn relation_rows_filling_no_role_are_rejected() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "no-players",
        NODES,
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, a_id UBIGINT REFERENCES node(_iid),
                            b_id UBIGINT REFERENCES node(_iid), name VARCHAR);
         COMMENT ON TABLE link IS '@typedb relation link (a = a_id, b = b_id)';
         INSERT INTO node VALUES (1, 'n1');
         INSERT INTO link VALUES (10, 1, NULL, 'one player'), (11, NULL, NULL, 'no players');",
    )?;
    assert_error(&validation_errors(&dir), &["link", "1 row(s) fill no role"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

const NESTED: &str = r#"define
  attribute name value string;
  entity node, owns name, plays base:member;
  relation base, relates member, plays ref:target, plays ref:source;
  relation ref, relates target, relates source, owns name, plays ref:target;
"#;

/// A relation whose players are relations is streamed after them, whatever the table names.
#[test]
fn relation_tables_are_streamed_after_their_players() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "players-first",
        NESTED,
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         CREATE TABLE zbase (_iid UBIGINT PRIMARY KEY, n_id UBIGINT NOT NULL REFERENCES node(_iid));
         COMMENT ON TABLE zbase IS '@typedb relation base';
         CREATE TABLE aref (_iid UBIGINT PRIMARY KEY, z_id UBIGINT NOT NULL REFERENCES zbase(_iid), name VARCHAR);
         COMMENT ON TABLE aref IS '@typedb relation ref (target = z_id)';
         INSERT INTO node VALUES (1, 'n1');
         INSERT INTO zbase VALUES (5, 1);
         INSERT INTO aref VALUES (7, 5, 'r');",
    )?;
    let labels: Vec<String> = convert(&dir)?
        .into_iter()
        .filter_map(|item| match item {
            item::Item::Relation(relation) => Some(relation.label),
            _ => None,
        })
        .collect();
    assert_eq!(labels, ["base", "ref"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// In a self-referencing table, a row needs a player outside the cycle or an earlier row.
#[test]
fn relation_cycles_need_a_player_imported_first() -> Result<(), Box<dyn Error>> {
    let sql = "CREATE TABLE node (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         CREATE TABLE base (_iid UBIGINT PRIMARY KEY, n_id UBIGINT NOT NULL REFERENCES node(_iid));
         CREATE TABLE ref (_iid UBIGINT PRIMARY KEY, base_id UBIGINT REFERENCES base(_iid),
                           ref_id UBIGINT REFERENCES ref(_iid), name VARCHAR);
         COMMENT ON TABLE ref IS '@typedb relation ref (source = base_id, target = ref_id)';
         INSERT INTO node VALUES (1, 'n1');
         INSERT INTO base VALUES (5, 1);
         INSERT INTO ref VALUES (7, 5, NULL, 'first');
         INSERT INTO ref VALUES (8, NULL, 7, 'earlier player');";
    let dir = scratch("cycle-ok", NESTED, sql)?;
    convert(&dir)?;
    let _ = fs::remove_dir_all(&dir);

    let dir = scratch("cycle-forward", NESTED, &format!("{sql} INSERT INTO ref VALUES (6, NULL, 8, 'later player');"))?;
    assert_error(&validation_errors(&dir), &["ref", "1 row(s) have only role players that are imported after them"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

const EVENTS: &str = r#"define
  attribute happened value datetime;
  attribute day value date;
  attribute score value double;
  attribute span value duration;
  entity event, owns happened @card(0..), owns day @card(0..), owns score @card(0..), owns span;
"#;

#[test]
fn nanosecond_timestamps_and_date_lists_convert_exactly() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "nanos",
        EVENTS,
        "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, happened TIMESTAMP_NS, day DATE[]);
         INSERT INTO event VALUES (1, '2020-01-01 00:00:00.000000001', ['2020-01-01', '2020-01-02']),
                                  (2, '2020-01-01 00:00:00.000000002', NULL),
                                  (3, '1969-12-31 23:59:59.999999999', NULL);",
    )?;
    let mut datetimes = Vec::new();
    let mut dates = 0;
    for item in convert(&dir)? {
        if let item::Item::Attribute(attribute) = item {
            match attribute.value.and_then(|value| value.value) {
                Some(migration_value::Value::Datetime(datetime)) => datetimes.push((datetime.seconds, datetime.nanos)),
                Some(migration_value::Value::Date(_)) => dates += 1,
                other => panic!("unexpected value {other:?}"),
            }
        }
    }
    assert_eq!(datetimes, [(-1, 999_999_999), (1_577_836_800, 1), (1_577_836_800, 2)]);
    assert_eq!(dates, 2);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn values_typedb_cannot_store_exactly_are_rejected() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "unstorable",
        EVENTS,
        "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, happened TIMESTAMP, day DATE, score BIGINT,
                             span INTERVAL);
         INSERT INTO event VALUES (1, 'infinity', '-infinity', 9007199254740993, INTERVAL '1 month -2 days'),
                                  (2, '2020-01-01', '2020-01-01', 9007199254740992, INTERVAL '1 day');
         CREATE TABLE measurement (_iid UBIGINT PRIMARY KEY, score DOUBLE[]);
         COMMENT ON TABLE measurement IS '@typedb entity event';
         INSERT INTO measurement VALUES (3, ['nan'::DOUBLE, 1.5]);",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["'happened'", "infinite or outside TypeDB's datetime range"]);
    assert_error(&errors, &["'day'", "infinite or outside TypeDB's date range"]);
    assert_error(&errors, &["'score'", "1 value(s)", "no exact double representation"]);
    assert_error(&errors, &["measurement", "NaN or infinite"]);
    assert_error(&errors, &["'span'", "negative components"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

const TAGS: &str = r#"define
  attribute tag value string;
  attribute email value string;
  entity person, owns tag @card(2..3), owns email @unique;
  entity employee sub person;
"#;

/// Equal values are one attribute and NULL elements none: `['a', 'a', NULL]` is one tag.
#[test]
fn ownership_cardinality_counts_distinct_values_across_columns() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "list-card",
        TAGS,
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, tag VARCHAR[], extra_tag VARCHAR);
         COMMENT ON COLUMN person.extra_tag IS '@typedb owns tag';
         INSERT INTO person VALUES (1, ['a', 'a', NULL], NULL), (2, ['a', 'b'], 'c'), (3, ['a', 'b', 'c'], 'd');",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["1 row(s) hold fewer than 2 distinct value(s)", "extra_tag"]);
    assert_error(&errors, &["1 row(s) hold more than 3 distinct value(s)"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// `@unique` spans every instance of the declaring type: other tables, subtypes and lists.
#[test]
fn uniqueness_spans_tables_subtypes_and_lists() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "unique",
        TAGS,
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, tag VARCHAR[], email VARCHAR[]);
         CREATE TABLE employee (_iid UBIGINT PRIMARY KEY, tag VARCHAR[], email VARCHAR);
         INSERT INTO person VALUES (1, ['a', 'b'], ['x@y', 'x@y']), (2, ['a', 'b'], ['p@q']);
         INSERT INTO employee VALUES (1, ['a', 'b'], 'p@q');",
    )?;
    assert_error(&validation_errors(&dir), &["1 value(s) of 'email'", "'person' owns 'email' @unique"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn role_cardinality_upper_bound_is_validated() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "relates-card",
        "define\n  entity node, plays pair:m;\n  relation pair, relates m;\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE pair (_iid UBIGINT PRIMARY KEY, x UBIGINT REFERENCES node(_iid), y UBIGINT REFERENCES node(_iid));
         COMMENT ON TABLE pair IS '@typedb relation pair (m = x, m = y)';
         INSERT INTO node VALUES (1), (2);
         INSERT INTO pair VALUES (9, 1, 2), (10, 1, NULL);",
    )?;
    assert_error(&validation_errors(&dir), &["1 row(s) fill role 'pair:m' with more than 1 player(s)"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// A supertype's value constraints keep applying to its subtypes.
#[test]
fn inherited_value_constraints_are_validated() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "inherited-constraints",
        "define\n  attribute code @abstract, value string @regex(\"^[A-Z]+$\");\n  attribute country-code sub code;\n  \
         entity place, owns country-code;\n",
        "CREATE TABLE place (_iid UBIGINT PRIMARY KEY, country_code VARCHAR);
         INSERT INTO place VALUES (1, 'GB'), (2, 'gb');",
    )?;
    assert_error(&validation_errors(&dir), &["1 value(s) in 'country_code' violate attribute 'code' @regex"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn abstract_attributes_cannot_be_populated() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "abstract-attribute",
        "define\n  attribute code @abstract, value string;\n  attribute country-code sub code;\n  \
         entity place, owns code;\n",
        "CREATE TABLE place (_iid UBIGINT PRIMARY KEY, code VARCHAR);",
    )?;
    assert_error(&validation_errors(&dir), &["'code'", "abstract"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// Tables of other schemas and views are never read, which the user hears about.
#[test]
fn views_and_other_schemas_are_reported() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "ignored",
        "define\n  entity node;\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY);
         CREATE VIEW node_view AS SELECT * FROM node;
         CREATE SCHEMA staging; CREATE TABLE staging.node (_iid UBIGINT PRIMARY KEY);",
    )?;
    let migration = prepare(&dir).expect("validation passes");
    let warnings: Vec<&str> = migration.warnings().iter().map(|warning| warning.message.as_str()).collect();
    assert!(warnings.iter().any(|w| w.contains("view main.node_view is not loaded")), "{warnings:?}");
    assert!(warnings.iter().any(|w| w.contains("table staging.node is not loaded")), "{warnings:?}");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// Every ownership and role player names an item of the same file, as the importer requires.
fn assert_references_resolve(items: &[item::Item]) {
    let mut attributes = std::collections::HashSet::new();
    let mut objects = std::collections::HashSet::new();
    for item in items {
        match item {
            item::Item::Attribute(attribute) => attributes.insert(attribute.id.clone()),
            item::Item::Entity(entity) => objects.insert(entity.id.clone()),
            item::Item::Relation(relation) => objects.insert(relation.id.clone()),
            _ => false,
        };
    }
    for item in items {
        let (owned, players) = match item {
            item::Item::Entity(entity) => (&entity.attributes, Vec::new()),
            item::Item::Relation(relation) => {
                (&relation.attributes, relation.roles.iter().flat_map(|role| role.players.iter()).collect())
            }
            _ => continue,
        };
        for ownership in owned {
            assert!(attributes.contains(&ownership.id), "ownership of a missing attribute {}", ownership.id);
        }
        for player in players {
            assert!(objects.contains(&player.id), "missing role player {}", player.id);
        }
    }
}

/// Columns of different numeric types populating one attribute are deduplicated together;
/// SQL's own type unification would round (DECIMAL(38,0) with DECIMAL(10,9) is DECIMAL(38,0)).
#[test]
fn mixed_numeric_columns_of_one_attribute_keep_every_value() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "mixed-numeric",
        "define\n  attribute price value decimal;\n  attribute weight value double;\n  \
         entity item, owns price @card(0..), owns weight @card(0..);\n",
        "CREATE TABLE item (_iid UBIGINT PRIMARY KEY, price DECIMAL(38,0), weight BIGINT);
         CREATE TABLE gadget (_iid UBIGINT PRIMARY KEY, price DECIMAL(10,9), weight DECIMAL(10,2));
         COMMENT ON TABLE gadget IS '@typedb entity item';
         CREATE TABLE widget (_iid UBIGINT PRIMARY KEY, price BIGINT, weight FLOAT);
         COMMENT ON TABLE widget IS '@typedb entity item';
         INSERT INTO item VALUES (1, 1, 2);
         INSERT INTO gadget VALUES (2, 1.25, 2.5);
         INSERT INTO widget VALUES (3, 1, 2.5);",
    )?;
    let items = convert(&dir)?;
    assert_references_resolve(&items);
    let attributes = items.iter().filter(|item| matches!(item, item::Item::Attribute(_))).count();
    assert_eq!(attributes, 4, "expected prices 1 and 1.25, weights 2 and 2.5");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

#[test]
fn timestamps_beside_nanosecond_columns_must_fit_their_range() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "mixed-timestamps",
        EVENTS,
        "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, happened TIMESTAMP_NS);
         CREATE TABLE old_event (_iid UBIGINT PRIMARY KEY, happened TIMESTAMP);
         COMMENT ON TABLE old_event IS '@typedb entity event';
         INSERT INTO event VALUES (1, '2020-01-01 00:00:00.000000001');
         INSERT INTO old_event VALUES (2, '1500-01-01'), (3, '2020-01-01');",
    )?;
    assert_error(&validation_errors(&dir), &["old_event", "1 value(s)", "TIMESTAMP_NS's range"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// TypeDB holds a relation's players as a set: naming a player twice in a role is one player.
#[test]
fn a_player_named_twice_in_a_role_counts_once() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "repeated-player",
        "define\n  entity node, plays pair:m @card(0..1);\n  entity hub, plays pair:m;\n  \
         relation pair, relates m @card(1..1);\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE hub (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE pair (_iid UBIGINT PRIMARY KEY, x UBIGINT REFERENCES node(_iid), y UBIGINT REFERENCES node(_iid),
                            h UBIGINT REFERENCES hub(_iid));
         COMMENT ON TABLE pair IS '@typedb relation pair (m = x, m = y, m = h)';
         INSERT INTO node VALUES (1); INSERT INTO hub VALUES (1);
         INSERT INTO pair VALUES (9, 1, 1, NULL);",
    )?;
    assert_references_resolve(&convert(&dir)?);
    let _ = fs::remove_dir_all(&dir);

    // node 1 and hub 1 are two players, whatever their ids.
    let dir = scratch(
        "two-players",
        "define\n  entity node, plays pair:m;\n  entity hub, plays pair:m;\n  relation pair, relates m @card(1..1);\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE hub (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE pair (_iid UBIGINT PRIMARY KEY, x UBIGINT REFERENCES node(_iid), h UBIGINT REFERENCES hub(_iid));
         COMMENT ON TABLE pair IS '@typedb relation pair (m = x, m = h)';
         INSERT INTO node VALUES (1); INSERT INTO hub VALUES (1);
         INSERT INTO pair VALUES (9, 1, 1);",
    )?;
    assert_error(&validation_errors(&dir), &["fill role 'pair:m' with more than 1 player(s)"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// TypeDB counts an ownership's `@card` over the owned attribute type and its subtypes.
#[test]
fn ownership_cardinality_counts_attribute_subtypes() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "subtype-card",
        "define\n  attribute name value string;\n  attribute surname sub name;\n  \
         entity person, owns name @card(0..1), owns surname;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR, surname VARCHAR);
         INSERT INTO person VALUES (1, 'Ann', 'Lee'), (2, 'Bo', NULL), (3, NULL, 'Kim');",
    )?;
    assert_error(
        &validation_errors(&dir),
        &["1 row(s) hold more than 1 distinct value(s)", "\"surname\"", "@card(..1)"],
    );
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// DuckDB merges values TypeDB keeps apart (-0.0 and 0.0; intervals compared normalised, so
/// 1 month = 30 days): attributes and ownerships must still agree on every id.
#[test]
fn values_sql_merges_but_typedb_distinguishes_stay_consistent() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "sql-merged-values",
        "define\n  attribute weight value double;\n  attribute span value duration;\n  \
         entity thing, owns weight, owns span @card(0..);\n",
        "CREATE TABLE thing (_iid UBIGINT PRIMARY KEY, weight DOUBLE, span INTERVAL, spans INTERVAL[]);
         COMMENT ON COLUMN thing.spans IS '@typedb owns span';
         INSERT INTO thing VALUES (1, '-0.0'::DOUBLE, INTERVAL '1 month', [INTERVAL '30 days', INTERVAL '1 month']),
                                  (2, 0.0, INTERVAL '1 day', NULL), (3, '-0.0'::DOUBLE, INTERVAL '24 hours', []);",
    )?;
    let items = convert(&dir)?;
    assert_references_resolve(&items);
    let labels: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            item::Item::Attribute(attribute) => Some(attribute.label.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(labels, ["weight", "span", "span", "span", "span"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// TypeDB ownerships and role players are sets: repeats within a row are written once.
#[test]
fn repeated_values_and_players_are_written_once() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "repeats",
        "define\n  attribute tag value string;\n  entity node, owns tag @card(0..), plays link:end;\n  \
         relation link, relates end @card(0..3);\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY, tag VARCHAR, tags VARCHAR[]);
         COMMENT ON COLUMN node.tags IS '@typedb owns tag';
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, a UBIGINT REFERENCES node(_iid), b UBIGINT REFERENCES node(_iid));
         COMMENT ON TABLE link IS '@typedb relation link (end = a, end = b)';
         INSERT INTO node VALUES (1, 'x', ['x', 'y', 'x']);
         INSERT INTO link VALUES (1, 1, 1);",
    )?;
    let items = convert(&dir)?;
    for item in &items {
        match item {
            item::Item::Entity(entity) => assert_eq!(entity.attributes.len(), 2),
            item::Item::Relation(relation) => assert_eq!(relation.roles[0].players.len(), 1),
            item::Item::Checksums(checksums) => {
                assert_eq!((checksums.ownership_count, checksums.role_count), (2, 1))
            }
            _ => {}
        }
    }
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// A relation directive's bindings are checked, not silently dropped or trusted.
#[test]
fn relation_directive_bindings_are_checked() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "bindings",
        "define\n  entity node, plays link:a, plays link:b;\n  entity rock;\n  relation link, relates a, relates b;\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE rock (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, x UBIGINT REFERENCES node(_iid), y UBIGINT REFERENCES node(_iid),
                            r UBIGINT REFERENCES rock(_iid));
         COMMENT ON TABLE link IS '@typedb relation link (a = x, b = x, b = yy, a = r, b = y)';
         COMMENT ON COLUMN link.y IS '@typedb skip';",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["binds role 'b' to column 'x', which the directive already binds"]);
    assert_error(&errors, &["binds role 'b' to column 'yy', which the table does not have"]);
    assert_error(&errors, &["binds role 'b' to column 'y', which is marked '@typedb skip'"]);
    assert_error(&errors, &["column 'r' is bound to role 'link:a'", "'rock'", "does not play that role"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// A player table per role: the columns of one role may target several player tables.
#[test]
fn one_role_is_inferred_for_several_player_tables() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "role-per-player-table",
        "define\n  entity person, plays ownership:owner;\n  entity company, plays ownership:owner;\n  \
         entity car, plays ownership:owned;\n  relation ownership, relates owner, relates owned;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE company (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE car (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE ownership (_iid UBIGINT PRIMARY KEY, person_id UBIGINT REFERENCES person(_iid),
                                 company_id UBIGINT REFERENCES company(_iid), car_id UBIGINT REFERENCES car(_iid));
         INSERT INTO person VALUES (1); INSERT INTO company VALUES (1); INSERT INTO car VALUES (1), (2);
         INSERT INTO ownership VALUES (1, 1, NULL, 1), (2, NULL, 1, 2);",
    )?;
    let items = convert(&dir)?;
    assert_references_resolve(&items);
    let owner_roles = items
        .iter()
        .filter_map(|item| match item {
            item::Item::Relation(relation) => {
                Some(relation.roles.iter().filter(|role| role.label == "ownership:owner").count())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(owner_roles, [1, 1]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// TypeDB's @regex matches anywhere in a value; syntax RE2 would read differently is left to
/// the server.
#[test]
fn regex_constraints_match_as_in_typedb() -> Result<(), Box<dyn Error>> {
    let schema = |regex: &str| {
        format!("define\n  attribute code value string @regex(\"{regex}\");\n  entity item, owns code;\n")
    };
    let sql = "CREATE TABLE item (_iid UBIGINT PRIMARY KEY, code VARCHAR); INSERT INTO item VALUES (1, 'xAb9y');";
    let dir = scratch("regex-anywhere", &schema(r"[A-Z][a-z]\d"), sql)?;
    prepare(&dir).map_err(|err| format!("a partial match must pass: {err:?}"))?;
    let dir = scratch("regex-anchored", &schema(r"^[A-Z][a-z]\d$"), sql)?;
    assert_error(&validation_errors(&dir), &["1 value(s) in 'code' violate attribute 'code' @regex"]);
    let dir = scratch("regex-rust-only", &schema(r"^[a-z--b]+$"), sql)?;
    let migration = prepare(&dir).map_err(|err| format!("expected a warning only: {err:?}"))?;
    assert!(migration.warnings().iter().any(|warning| warning.message.contains("cannot be checked in SQL")));
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// Ownership and plays lower bounds hold for every row, also where no column or table
/// populates them; an ownership's bound counts the attribute's subtypes.
#[test]
fn lower_bounds_hold_where_nothing_populates_them() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "lower-bounds",
        "define\n  attribute name @abstract, value string;\n  attribute nick sub name;\n  attribute code value string;\n  \
         entity person, owns name @card(1..2), owns nick @card(0..), owns code @key, plays friendship:friend @card(1..);\n  \
         relation friendship, relates friend @card(0..2);\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, nick VARCHAR[]);
         INSERT INTO person VALUES (1, ['a', 'b', 'c']), (2, ['a']);",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["no column populates 'code'", "2 row(s) violate 'person' owns 'code' @card(1..)"]);
    assert_error(&errors, &["1 row(s) hold more than 2 distinct value(s) in 'nick'", "owns 'name' @card(..2)"]);
    assert_error(&errors, &["table 'person': 2 row(s) play 'friendship:friend' fewer than 1 time(s)"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// Values a column cannot represent are reported once, not again by every check reading them.
#[test]
fn unrepresentable_values_are_reported_once() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "unrepresentable",
        "define\n  attribute amount value decimal;\n  attribute span value duration;\n  \
         entity item, owns amount @unique, owns span;\n",
        "CREATE TABLE item (_iid UBIGINT PRIMARY KEY, amount UBIGINT, span INTERVAL);
         INSERT INTO item VALUES (1, 18446744073709551615, INTERVAL '2562048 hours');",
    )?;
    let errors = validation_errors(&dir);
    assert_eq!(errors.len(), 2, "{errors:#?}");
    assert_error(&errors, &["'amount' exceed TypeDB's 64-bit integer range"]);
    assert_error(&errors, &["'span' have a time part beyond 2562047 hours"]);
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}

/// The source is opened read-only: a pending write-ahead log is read, not checkpointed into it.
#[test]
fn the_source_database_is_left_untouched() -> Result<(), Box<dyn Error>> {
    let dir = scratch("read-only", "define\n  entity node;\n", "PRAGMA disable_checkpoint_on_shutdown;")?;
    let db = dir.join("fixture.duckdb");
    let conn = Connection::open(&db)?;
    conn.execute_batch(
        "PRAGMA disable_checkpoint_on_shutdown; CREATE TABLE node (_iid UBIGINT PRIMARY KEY); INSERT INTO node VALUES (1);",
    )?;
    drop(conn);
    let wal = dir.join("fixture.duckdb.wal");
    let before = (fs::read(&db)?, fs::read(&wal)?);
    let entities = convert(&dir)?.into_iter().filter(|item| matches!(item, item::Item::Entity(_))).count();
    assert_eq!(entities, 1);
    assert!(before == (fs::read(&db)?, fs::read(&wal)?), "the source database files changed");
    let _ = fs::remove_dir_all(&dir);
    Ok(())
}
