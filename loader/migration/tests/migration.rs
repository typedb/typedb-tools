/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs, io,
    path::PathBuf,
    sync::mpsc,
    thread,
    time::Duration,
};

use duckdb::Connection;
use prost::Message;
use typedb_migration::{Checksums, Migration, PrepareError, Source};
use typedb_protocol::migration::{Item, MigrationValue, item, migration_value};

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

const FIXTURE: &str = r#"
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
"#;

const NESTED: &str = r#"define
  attribute name value string;
  entity node, owns name, plays base:member;
  relation base, relates member, plays ref:target, plays ref:source;
  relation ref, relates target, relates source, owns name, plays ref:target;
"#;

const EVENTS: &str = r#"define
  attribute happened value datetime;
  attribute day value date;
  attribute score value double;
  attribute span value duration;
  attribute count value integer;
  entity event, owns happened @card(0..), owns day @card(0..), owns score @card(0..), owns span, owns count;
"#;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn scratch(test_name: &str, schema: &str, sql: &str) -> Result<Scratch, Box<dyn Error>> {
    let dir = Scratch(std::env::temp_dir().join(format!("typedb-migration-{test_name}-{}", std::process::id())));
    fs::create_dir_all(&dir.0)?;
    Connection::open(dir.0.join("fixture.duckdb"))?.execute_batch(sql)?;
    fs::write(dir.0.join("schema.tql"), schema)?;
    Ok(dir)
}

fn prepare(dir: &Scratch) -> Result<Migration, PrepareError> {
    let schema = fs::read_to_string(dir.0.join("schema.tql")).expect("schema");
    Migration::prepare(&Source::DuckDb(dir.0.join("fixture.duckdb")), &schema, &dir.0.join("spill"))
}

fn validation_errors(dir: &Scratch) -> Vec<String> {
    match prepare(dir) {
        Err(PrepareError::Invalid { errors, .. }) => errors,
        Err(PrepareError::Setup(message)) => panic!("expected validation errors, got a setup error: {message}"),
        Ok(_) => panic!("expected validation errors, but validation passed"),
    }
}

fn run(dir: &Scratch) -> Result<Vec<u8>, Box<dyn Error>> {
    let migration = prepare(dir).map_err(|err| format!("validation failed: {err:?}"))?;
    let mut output = Vec::new();
    migration.run(&mut output, "3.0.0-test", "duckdb:fixture.duckdb")?;
    Ok(output)
}

fn convert(dir: &Scratch) -> Result<Vec<item::Item>, Box<dyn Error>> {
    decode_items(&run(dir)?)
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

fn assert_error(errors: &[String], fragments: &[&str]) {
    assert!(
        errors.iter().any(|error| fragments.iter().all(|fragment| error.contains(fragment))),
        "no error mentions all of {fragments:?}: {errors:#?}"
    );
}

fn assert_file_guarantees(items: &[item::Item]) {
    let item::Item::Header(header) = &items[0] else { panic!("the header comes first") };
    let guarantees = header.guarantees.as_ref().expect("the header states its guarantees");
    assert_eq!(guarantees.ordering(), item::header::guarantees::Ordering::AttributesEntitiesRelations);
    assert!(
        guarantees.attributes_unique && guarantees.players_by_label_scoped_ids && guarantees.owned_attributes_by_value
    );

    let mut attributes: HashMap<&str, &item::Attribute> = HashMap::new();
    let mut objects: HashMap<&str, (&str, u64)> = HashMap::new();
    let mut next_ids: HashMap<&str, u64> = HashMap::new();
    let mut last_kind = 0;
    for item in items {
        let kind = match item {
            item::Item::Header(_) => 0,
            item::Item::Attribute(_) => 1,
            item::Item::Entity(_) => 2,
            item::Item::Relation(_) => 3,
            item::Item::Checksums(_) => 4,
        };
        assert!(kind >= last_kind, "items out of order");
        last_kind = kind;
        let (id, object) = match item {
            item::Item::Attribute(attribute) => {
                assert!(attributes.insert(&attribute.id, attribute).is_none(), "attribute {} twice", attribute.id);
                continue;
            }
            item::Item::Entity(entity) => (&entity.id, (entity.label.as_str(), entity.label_scoped_id)),
            item::Item::Relation(relation) => (&relation.id, (relation.label.as_str(), relation.label_scoped_id)),
            _ => continue,
        };
        let object = (object.0, object.1.expect("every object carries its label-scoped id"));
        let next_id = next_ids.entry(object.0).or_default();
        assert_eq!(object.1, *next_id, "{id} is out of sequence");
        *next_id += 1;
        assert!(objects.insert(id, object).is_none(), "object {id} twice");
    }
    // Values compare by their encoding, which tells -0.0 from 0.0.
    let encoded = |value: &Option<MigrationValue>| value.as_ref().map(Message::encode_to_vec);
    let mut values = HashSet::new();
    assert!(attributes.values().all(|a| values.insert((a.label.as_str(), encoded(&a.value)))), "an attribute repeats");

    let mut counts = Checksums::default();
    for item in items {
        let (owned, players) = match item {
            item::Item::Entity(entity) => {
                counts.entity_count += 1;
                (&entity.attributes, Vec::new())
            }
            item::Item::Relation(relation) => {
                counts.relation_count += 1;
                for role in &relation.roles {
                    let ids: HashSet<&str> = role.players.iter().map(|player| player.id.as_str()).collect();
                    assert_eq!(ids.len(), role.players.len(), "{} repeats a player in {}", relation.id, role.label);
                }
                (&relation.attributes, relation.roles.iter().flat_map(|role| role.players.iter()).collect())
            }
            _ => continue,
        };
        let owned_ids: HashSet<&str> = owned.iter().map(|ownership| ownership.id.as_str()).collect();
        assert_eq!(owned_ids.len(), owned.len(), "an item repeats an ownership");
        counts.ownership_count += owned.len() as i64;
        counts.role_count += players.len() as i64;
        for ownership in owned {
            let attribute =
                attributes.get(ownership.id.as_str()).unwrap_or_else(|| panic!("missing attribute {}", ownership.id));
            let by_value = ownership.attribute_by_value.as_ref().expect("owned attributes carry their value");
            assert!(
                by_value.label == attribute.label && encoded(&by_value.value) == encoded(&attribute.value),
                "{} differs",
                ownership.id
            );
        }
        for player in players {
            let object = objects.get(player.id.as_str()).unwrap_or_else(|| panic!("missing role player {}", player.id));
            let by_id = player.player_by_label_scoped_id.as_ref().expect("players carry their label-scoped id");
            assert_eq!((by_id.label.as_str(), by_id.label_scoped_id), *object, "{} differs", player.id);
        }
    }
    counts.attribute_count = attributes.len() as i64;
    let item::Item::Checksums(checksums) = items.last().expect("the checksums come last") else { panic!() };
    assert_eq!(*checksums, counts);
}

#[test]
fn converts_and_produces_a_well_formed_import_file() -> Result<(), Box<dyn Error>> {
    let dir = scratch("clean", SCHEMA, FIXTURE)?;
    let first = run(&dir)?;
    let items = decode_items(&first)?;
    assert_file_guarantees(&items);

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

    assert_eq!(entities.len(), 3);
    assert_eq!(relations.len(), 3);
    assert_eq!(attributes.len(), 3 + 1 + 2 + 1); // names, age, tags, salary

    let mut role_labels: Vec<&str> =
        relations.iter().flat_map(|r| r.roles.iter().map(|role| role.label.as_str())).collect();
    role_labels.sort();
    role_labels.dedup();
    assert_eq!(role_labels, ["employment:employee", "employment:employer", "mentorship:mentor", "mentorship:student"]);

    let ids: Vec<(&str, Option<u64>)> = entities.iter().map(|e| (e.id.as_str(), e.label_scoped_id)).collect();
    assert_eq!(ids, [("company:0", Some(0)), ("person:0", Some(0)), ("person:1", Some(1))]);
    let ids: Vec<(&str, Option<u64>)> = relations.iter().map(|r| (r.id.as_str(), r.label_scoped_id)).collect();
    assert_eq!(ids, [("employment:0", Some(0)), ("employment:1", Some(1)), ("mentorship:0", Some(0))]);
    let mut ids: Vec<&str> = attributes.iter().map(|a| a.id.as_str()).collect();
    ids.sort();
    assert_eq!(
        ids,
        ["age:30", "name:Acme", "name:Alice", "name:Bob", "salary:50000.2500000000000000000", "tag:a", "tag:b"]
    );

    assert_eq!(first, run(&dir)?, "migration output is not deterministic");

    Ok(())
}

#[test]
fn fk_column_on_entity_table_is_rejected() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "entity-fk",
        "define\n  entity person;\n  entity pet;\n  entity employee sub person;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE pet (_iid UBIGINT PRIMARY KEY, owner_id UBIGINT REFERENCES person(_iid));
         CREATE TABLE employee (_iid UBIGINT PRIMARY KEY REFERENCES person(_iid));",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["table 'pet': FK column 'owner_id'", "relations are tables"]);
    assert_error(&errors, &["table 'employee': _iid is a FOREIGN KEY"]);
    Ok(())
}

#[test]
fn structure_and_mapping_errors_name_the_table_and_the_fix() -> Result<(), Box<dyn Error>> {
    let schema = "define\n  entity person;\n  entity friend_ship;\n  relation friend-ship, relates friend;\n";
    let check = |sql: &str, fragment: &str| -> Result<(), Box<dyn Error>> {
        let dir = scratch("structure", schema, sql)?;
        assert_error(&validation_errors(&dir), &[fragment]);
        Ok(())
    };
    let tables = [
        (
            "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, mystery VARCHAR)",
            "column 'mystery' has no matching attribute",
        ),
        ("CREATE TABLE person (id BIGINT PRIMARY KEY)", "table 'person' has no _iid column"),
        ("CREATE TABLE person (_iid BIGINT PRIMARY KEY)", "table 'person': _iid must be UBIGINT"),
        ("CREATE TABLE person (_iid UBIGINT)", "table 'person': _iid must be declared NOT NULL"),
        ("CREATE TABLE person (_iid UBIGINT NOT NULL)", "table 'person': _iid must carry a declared UNIQUE"),
        (
            "CREATE TABLE friend_ship (_iid UBIGINT PRIMARY KEY)",
            "table 'friend_ship' matches both an entity and a relation",
        ),
        (
            "CREATE TABLE stranger (_iid UBIGINT PRIMARY KEY)",
            "table 'stranger' has no matching entity or relation type",
        ),
    ];
    for (sql, fragment) in tables {
        check(sql, fragment)?;
    }
    let directives = [
        ("TABLE person IS '@typedb entity'", "table 'person': unrecognised directive '@typedb entity'"),
        ("TABLE person IS '@typedb relation person (a'", "malformed @typedb relation directive: missing closing ')'"),
        (
            "COLUMN person._iid IS '@typedb skip'",
            "table 'person': _iid holds the instances' ids and takes no directive",
        ),
        (
            "COLUMN person.x IS '@typedb relation boss'",
            "table 'person': column 'x' cannot be a relation: relations are tables",
        ),
        ("COLUMN person.x IS '@typedb own x'", "table 'person': column 'x': unrecognised directive '@typedb own x'"),
    ];
    for (comment, fragment) in directives {
        check(&format!("CREATE TABLE person (_iid UBIGINT PRIMARY KEY, x INTEGER); COMMENT ON {comment}"), fragment)?;
    }
    Ok(())
}

#[test]
fn skipped_tables_and_columns_are_not_read() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "skip",
        "define\n  attribute name value string;\n  entity person, owns name;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR, photo BLOB);
         COMMENT ON COLUMN person.photo IS '@typedb skip';
         CREATE TABLE scratch (x INTEGER);
         COMMENT ON TABLE scratch IS '@TypeDB skip';
         INSERT INTO person VALUES (1, 'Ann', 'ff'::BLOB);",
    )?;
    let items = convert(&dir)?;
    assert_eq!(items.iter().filter(|item| matches!(item, item::Item::Entity(_))).count(), 1);
    Ok(())
}

#[test]
fn every_column_type_feeds_attributes_and_ownerships_alike() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "column-types",
        "define\n  attribute seen value datetime-tz;\n  attribute amount value decimal;\n  \
         attribute ratio value double;\n  attribute big value integer;\n  attribute flag value boolean;\n  \
         entity reading, owns seen, owns amount @card(0..), owns ratio @card(0..), owns big, owns flag;\n",
        "CREATE TABLE reading (_iid UBIGINT PRIMARY KEY, seen TIMESTAMPTZ, amounts DECIMAL(10,2)[], ratios INTEGER[],
                               big HUGEINT, flag BOOLEAN);
         COMMENT ON COLUMN reading.amounts IS '@typedb owns amount';
         COMMENT ON COLUMN reading.ratios IS '@typedb owns ratio';
         CREATE TABLE old_reading (_iid UBIGINT PRIMARY KEY, amounts BIGINT[], big BIGINT, flag BOOLEAN);
         COMMENT ON TABLE old_reading IS '@typedb entity reading';
         COMMENT ON COLUMN old_reading.amounts IS '@typedb owns amount';
         INSERT INTO reading VALUES (1, '2020-01-01 00:00:00+02', [1.50, 2.25, 1.5], [1, 2], 9223372036854775807, true);
         INSERT INTO old_reading VALUES (2, [3], -9223372036854775808, false);",
    )?;
    let items = convert(&dir)?;
    assert_file_guarantees(&items);
    let mut values: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            item::Item::Attribute(attribute) => {
                Some(format!("{}={:?}", attribute.label, attribute.value.as_ref()?.value))
            }
            _ => None,
        })
        .collect();
    values.sort();
    assert_eq!(
        values,
        [
            "amount=Some(Decimal(Decimal { integer: 1, fractional: 5000000000000000000 }))",
            "amount=Some(Decimal(Decimal { integer: 2, fractional: 2500000000000000000 }))",
            "amount=Some(Decimal(Decimal { integer: 3, fractional: 0 }))",
            "big=Some(Integer(-9223372036854775808))",
            "big=Some(Integer(9223372036854775807))",
            "flag=Some(Boolean(false))",
            "flag=Some(Boolean(true))",
            "ratio=Some(Double(1.0))",
            "ratio=Some(Double(2.0))",
            "seen=Some(DatetimeTz(DatetimeTz { datetime: Some(Datetime { seconds: 1577829600, nanos: 0 }), \
             timezone: Some(Named(\"UTC\")) }))",
        ]
    );
    let ownerships: usize = items
        .iter()
        .filter_map(|item| if let item::Item::Entity(entity) = item { Some(entity.attributes.len()) } else { None })
        .sum();
    assert_eq!(ownerships, 7 + 3);
    Ok(())
}

#[test]
fn string_values_are_distinct_byte_wise() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "collation",
        "define\n  attribute name value string;\n  attribute tag value string;\n  \
         entity person, owns name, owns tag @card(0..);\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR COLLATE NOCASE, tags UUID[]);
         COMMENT ON COLUMN person.tags IS '@typedb owns tag';
         INSERT INTO person VALUES
             (1, 'Bob', ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002']),
             (2, 'BOB', NULL);",
    )?;
    let items = convert(&dir)?;
    let attributes = items.iter().filter(|item| matches!(item, item::Item::Attribute(_))).count();
    assert_eq!(attributes, 2 + 2, "two names and two tags expected");
    let owned: usize = items
        .iter()
        .filter_map(|item| if let item::Item::Entity(entity) = item { Some(entity.attributes.len()) } else { None })
        .sum();
    assert_eq!(owned, 2 + 1 + 0 + 1);

    Ok(())
}

#[test]
fn inherited_roles_are_scoped_to_their_declaring_relation() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "scoping",
        "define\n  entity person, plays interaction:author;\n  entity post, plays posting:post;\n  \
         relation interaction @abstract, relates subject @card(1), relates author as subject;\n  \
         relation posting, sub interaction, relates post @card(1);\n",
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
    let items = convert(&dir)?;
    let relation = items
        .iter()
        .find_map(|i| if let item::Item::Relation(r) = i { Some(r) } else { None })
        .expect("one relation emitted");
    let mut labels: Vec<&str> = relation.roles.iter().map(|role| role.label.as_str()).collect();
    labels.sort();
    assert_eq!(labels, ["interaction:author", "posting:post"], "roles must carry the declaring relation's scope");

    Ok(())
}

#[test]
fn relations_may_play_in_relations_streamed_before_them() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "forward-reference",
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
    let items = convert(&dir)?;
    assert_file_guarantees(&items);
    let relations: Vec<String> = items
        .into_iter()
        .filter_map(|item| match item {
            item::Item::Relation(relation) => {
                let players = relation.roles.iter().flat_map(|role| role.players.iter().map(|player| &player.id));
                Some(format!("{} <- {}", relation.id, players.cloned().collect::<Vec<_>>().join(" ")))
            }
            _ => None,
        })
        .collect();
    assert_eq!(relations, ["ref:0 <- base:0", "base:0 <- node:0"]);
    Ok(())
}

#[test]
fn composite_foreign_keys_cannot_bind_roles() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "composite-fk",
        "define\n  attribute a value integer;\n  entity node, plays link:end;\n  relation link, relates end, owns a;\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY, a INTEGER, b INTEGER, UNIQUE (a, b));
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES node(a, b));",
    )?;
    assert_error(&validation_errors(&dir), &["column 'a' is part of the composite FOREIGN KEY [\"a\", \"b\"]"]);
    Ok(())
}

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
    Ok(())
}

#[test]
fn values_typedb_cannot_hold_are_found_before_the_conversion() -> Result<(), Box<dyn Error>> {
    let cases = [
        ("happened TIMESTAMP_NS", "'infinity'", "hold a timestamp that is infinite or outside"),
        ("happened TIMESTAMP", "'1500-01-01'::TIMESTAMP - INTERVAL 280000 YEARS", "hold a timestamp that is"),
        (
            "happened TIMESTAMP",
            "(DATE '1970-01-01' + (-96465292))::TIMESTAMP - INTERVAL 1 MICROSECOND",
            "hold a timestamp",
        ),
        ("day DATE", "'-infinity'", "hold a date that is infinite or outside TypeDB's range"),
        ("score DOUBLE[]", "['nan'::DOUBLE, 1.5]", "hold a double that is not finite"),
        ("span INTERVAL", "INTERVAL '1 month -2 days'", "hold an interval with a negative part or a time part"),
        ("span INTERVAL[]", "[INTERVAL '5124096 hours']", "hold an interval with a negative part or a time part"),
        ("span INTERVAL", "INTERVAL '5124095:34:33.709552'", "hold an interval with a negative part or a time part"),
        ("count UBIGINT", "18446744073709551615", "hold an integer that does not fit TypeDB's 64-bit integer"),
    ];
    for (column, value, fragment) in cases {
        let sql = format!(
            "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, {column}); INSERT INTO event VALUES (2, NULL), (7, {value});"
        );
        let dir = scratch("unstorable", EVENTS, &sql)?;
        let attribute = column.split(' ').next().unwrap();
        let context = format!("table 'event': column '{attribute}': 1 row(s) {fragment}");
        assert_error(&validation_errors(&dir), &[&context, "e.g. _iid 7"]);
    }

    // The edges themselves are inside the range, in exact microseconds.
    let dir = scratch(
        "storable-edges",
        EVENTS,
        "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, happened TIMESTAMP, span INTERVAL);
         INSERT INTO event VALUES
           (1, (DATE '1970-01-01' + 95026236)::TIMESTAMP + INTERVAL '23:59:59.999999', INTERVAL '5124095:34:33.709551'),
           (2, (DATE '1970-01-01' + (-96465292))::TIMESTAMP, INTERVAL '0 seconds');",
    )?;
    assert_eq!(convert(&dir)?.len(), 2 + 4 + 2);
    Ok(())
}

#[test]
fn decimal_integer_parts_beyond_typedb_are_found_before_the_conversion() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "decimal-integer-part",
        "define\n  attribute amount value decimal;\n  entity item, owns amount;\n",
        "CREATE TABLE item (_iid UBIGINT PRIMARY KEY, amount DECIMAL(38,2));
         INSERT INTO item VALUES (1, 99999999999999999999.5), (2, 1.5), (3, 9223372036854775807.99),
                                (4, -9223372036854775808.00), (5, -9223372036854775808.01);",
    )?;
    let errors = validation_errors(&dir);
    let fragment = "column 'amount': 2 row(s) hold a decimal whose integer part is beyond TypeDB's 64-bit range";
    assert_error(&errors, &[fragment, "_iid 1, 5"]);

    // Wide integers and decimals feeding doubles convert when a double holds them exactly.
    let dir = scratch(
        "wide-to-double",
        "define\n  attribute score value double;\n  entity item, owns score @card(0..);\n",
        "CREATE TABLE item (_iid UBIGINT PRIMARY KEY, big UBIGINT, wide DECIMAL(38,19));
         COMMENT ON COLUMN item.big IS '@typedb owns score'; COMMENT ON COLUMN item.wide IS '@typedb owns score';
         INSERT INTO item VALUES (1, 18446744073709551616 - 2048, 4503599627370495.5);",
    )?;
    convert(&dir)?;
    Ok(())
}

#[test]
fn decimal_columns_beyond_typedb_precision_are_rejected() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "decimal-scale",
        "define\n  attribute amount value decimal;\n  entity item, owns amount;\n",
        "CREATE TABLE item (_iid UBIGINT PRIMARY KEY, amount DECIMAL(38,20));
         INSERT INTO item VALUES (1, 1.12345678901234567891);",
    )?;
    assert_error(&validation_errors(&dir), &["'amount' has DECIMAL scale 20, beyond TypeDB's 19"]);
    Ok(())
}

#[test]
fn integers_and_decimals_feeding_doubles_must_be_exact() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "inexact-double",
        EVENTS,
        "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, score BIGINT);
         CREATE TABLE reading (_iid UBIGINT PRIMARY KEY, score DECIMAL(20,3));
         COMMENT ON TABLE reading IS '@typedb entity event';
         INSERT INTO event VALUES (1, 9007199254740993), (2, 9007199254740992);
         INSERT INTO reading VALUES (3, 0.1), (4, 12345678901234567.125);",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["table 'event': column 'score': 1 row(s) hold a value that a double cannot", "_iid 1"]);
    assert_error(&errors, &["table 'reading': column 'score': 1 row(s) hold a value that a double cannot", "_iid 4"]);
    assert_eq!(errors.len(), 2, "{errors:#?}");

    // A decimal reads exactly at its own scale: 0.1 is the double nearest to it.
    let dir = scratch(
        "exact-double",
        EVENTS,
        "CREATE TABLE event (_iid UBIGINT PRIMARY KEY, score BIGINT);
         CREATE TABLE reading (_iid UBIGINT PRIMARY KEY, score DECIMAL(20,3));
         COMMENT ON TABLE reading IS '@typedb entity event';
         INSERT INTO event VALUES (2, 9007199254740992);
         INSERT INTO reading VALUES (3, 0.1);",
    )?;
    let mut doubles: Vec<f64> = convert(&dir)?
        .into_iter()
        .filter_map(|item| match item {
            item::Item::Attribute(attribute) => match attribute.value?.value? {
                migration_value::Value::Double(double) => Some(double),
                _ => None,
            },
            _ => None,
        })
        .collect();
    doubles.sort_by(f64::total_cmp);
    assert_eq!(doubles, [0.1, 9007199254740992.0]);
    Ok(())
}

#[test]
fn schema_constraints_are_left_to_the_server() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "schema-constraints",
        "define\n  attribute code @abstract, value string @regex(\"^[A-Z]+$\");\n  attribute country-code sub code;\n  \
         attribute level value integer @values(1, 2) @range(1..2);\n  \
         entity place, owns country-code @key, owns level @card(2..);\n",
        "CREATE TABLE place (_iid UBIGINT PRIMARY KEY, country_code VARCHAR, level INTEGER);
         INSERT INTO place VALUES (1, 'gb', 3), (2, 'gb', NULL);",
    )?;
    convert(&dir)?;
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
    Ok(())
}

#[test]
fn exact_names_win_over_dashed_readings() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "exact-names",
        "define\n  attribute a-b value string;\n  attribute a_b value string;\n  entity t, owns a-b, owns a_b;\n  \
         entity t_x;\n  entity t-x;\n",
        "CREATE TABLE t (_iid UBIGINT PRIMARY KEY, a_b VARCHAR);
         CREATE TABLE t_x (_iid UBIGINT PRIMARY KEY);
         INSERT INTO t VALUES (1, 'v'); INSERT INTO t_x VALUES (1);",
    )?;
    let items = convert(&dir)?;
    let labels: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            item::Item::Attribute(attribute) => Some(attribute.label.as_str()),
            item::Item::Entity(entity) => Some(entity.label.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(labels, ["a_b", "t", "t_x"]);
    Ok(())
}

#[test]
fn instances_are_numbered_per_type_whatever_their_iid() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "numbering",
        "define\n  attribute name value string;\n  entity person, owns name, plays knows:friend;\n  \
         relation knows, owns name, relates friend @card(1..), plays knows:friend;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         CREATE TABLE contractor (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         COMMENT ON TABLE contractor IS '@typedb entity person';
         CREATE TABLE knows (_iid UBIGINT PRIMARY KEY, name VARCHAR, a UBIGINT REFERENCES person(_iid),
                             b UBIGINT REFERENCES contractor(_iid), c UBIGINT REFERENCES knows(_iid));
         COMMENT ON TABLE knows IS '@typedb relation knows (friend = a)';
         CREATE TABLE alumni (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         COMMENT ON TABLE alumni IS '@typedb entity person';
         CREATE TABLE aknows (_iid UBIGINT PRIMARY KEY, name VARCHAR, a UBIGINT REFERENCES person(_iid),
                              c UBIGINT REFERENCES knows(_iid));
         COMMENT ON TABLE aknows IS '@typedb relation knows (friend = a)';
         INSERT INTO person VALUES (1000, 'Ann'), (5, 'Bob');
         INSERT INTO contractor VALUES (18446744073709551615, 'Dee'), (5, 'Cid');
         INSERT INTO knows VALUES (7, 'k7', 1000, 18446744073709551615, NULL);
         INSERT INTO knows VALUES (3, 'k3', 5, 5, 7);
         INSERT INTO knows VALUES (9, 'k9', NULL, NULL, 3);
         INSERT INTO aknows VALUES (1, 'ak', 1000, 7);",
    )?;
    let items = convert(&dir)?;
    assert_file_guarantees(&items);

    let mut names: HashMap<String, String> = HashMap::new();
    for item in &items {
        let (id, owned) = match item {
            item::Item::Entity(entity) => (&entity.id, &entity.attributes),
            item::Item::Relation(relation) => (&relation.id, &relation.attributes),
            _ => continue,
        };
        names.insert(id.clone(), owned[0].id.trim_start_matches("name:").to_owned());
    }
    // Tables in name order, entity tables first (alumni is empty), rows by _iid.
    let expected = [("person:0", "Cid"), ("person:1", "Dee"), ("person:2", "Bob"), ("person:3", "Ann")];
    let relations = [("knows:0", "ak"), ("knows:1", "k3"), ("knows:2", "k7"), ("knows:3", "k9")];
    for (id, name) in expected.into_iter().chain(relations) {
        assert_eq!(names[id], name, "{id}");
    }
    let mut friends: Vec<String> = Vec::new();
    for item in &items {
        if let item::Item::Relation(relation) = item {
            let players =
                relation.roles.iter().flat_map(|role| role.players.iter().map(|player| names[&player.id].as_str()));
            friends.push(format!("{}: {}", names[&relation.id], players.collect::<Vec<_>>().join(" ")));
        }
    }
    assert_eq!(friends, ["ak: Ann k7", "k3: Bob Cid k7", "k7: Ann Dee", "k9: k3"]);
    Ok(())
}

#[test]
fn scans_longer_than_a_page_keep_their_order() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "pages",
        "define\n  attribute age value integer;\n  entity person, owns age, plays knows:friend;\n  \
         relation knows, relates friend;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, age BIGINT);
         CREATE TABLE knows (_iid UBIGINT PRIMARY KEY, friend_id UBIGINT REFERENCES person(_iid));
         INSERT INTO person SELECT 1000000 - 3 * i, 1000000 - 3 * i FROM range(250000) t(i);
         INSERT INTO knows SELECT i, 1000000 - 3 * i FROM range(250000) t(i);",
    )?;
    let items = convert(&dir)?;
    assert_file_guarantees(&items);
    // The person numbered n has the n-th smallest _iid, which is also its age.
    let age_of_person: HashMap<u64, &str> = items
        .iter()
        .filter_map(|item| match item {
            item::Item::Entity(entity) => Some((entity.label_scoped_id?, entity.attributes[0].id.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(age_of_person.len(), 250_000);
    assert_eq!(age_of_person[&0], "age:250003");
    assert_eq!(age_of_person[&249_999], "age:1000000");
    for item in &items {
        if let item::Item::Relation(relation) = item {
            let friend_iid = 1_000_000 - 3 * relation.label_scoped_id.unwrap();
            let player = &relation.roles[0].players[0];
            assert_eq!(
                age_of_person[&player.player_by_label_scoped_id.as_ref().unwrap().label_scoped_id],
                format!("age:{friend_iid}")
            );
        }
    }
    Ok(())
}

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
    let warnings: Vec<&str> = migration.warnings().iter().map(|warning| warning.as_str()).collect();
    assert!(warnings.iter().any(|w| w.contains("view main.node_view is not loaded")), "{warnings:?}");
    assert!(warnings.iter().any(|w| w.contains("table staging.node is not loaded")), "{warnings:?}");
    Ok(())
}

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
    assert_file_guarantees(&items);
    let attributes = items.iter().filter(|item| matches!(item, item::Item::Attribute(_))).count();
    assert_eq!(attributes, 4, "expected prices 1 and 1.25, weights 2 and 2.5");
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
    let errors = validation_errors(&dir);
    let fragment = "table 'old_event': column 'happened': 1 row(s) hold a timestamp outside the nanosecond range";
    assert_error(&errors, &[fragment, "e.g. _iid 2"]);
    assert_eq!(errors.len(), 1, "{errors:#?}");
    Ok(())
}

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
    assert_file_guarantees(&items);
    let labels: Vec<&str> = items
        .iter()
        .filter_map(|item| match item {
            item::Item::Attribute(attribute) => Some(attribute.label.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(labels, ["weight", "span", "span", "span", "span"]);
    Ok(())
}

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
    assert_file_guarantees(&items);
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
    Ok(())
}

#[test]
fn relation_directive_bindings_are_checked() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "bindings",
        "define\n  attribute name value string;\n  entity node, plays link:a, plays link:b;\n  entity rock;\n  \
         relation link, relates a, relates b, owns name;\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE rock (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, x UBIGINT REFERENCES node(_iid), y UBIGINT REFERENCES node(_iid),
                            r UBIGINT REFERENCES rock(_iid), o UBIGINT REFERENCES node(_iid));
         COMMENT ON TABLE link IS '@typedb relation link (a = x, b = x, b = yy, a = r, b = y, a = _iid)';
         COMMENT ON COLUMN link.y IS '@typedb skip';
         COMMENT ON COLUMN link.o IS '@typedb owns name';",
    )?;
    let errors = validation_errors(&dir);
    assert_error(&errors, &["binds role 'b' to column 'x', which the directive already binds"]);
    assert_error(&errors, &["binds role 'b' to column 'yy', which the table does not have"]);
    assert_error(&errors, &["binds role 'b' to column 'y', which is marked '@typedb skip'"]);
    assert_error(&errors, &["binds role 'a' to column '_iid', which holds the relations' own ids"]);
    assert_error(&errors, &["column 'r' is bound to role 'link:a'", "'rock'", "does not play that role"]);
    assert_error(&errors, &["table 'link': column 'o' holds role players", "cannot own"]);
    Ok(())
}

#[test]
fn directive_bindings_settle_inference_of_the_other_columns() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "partial-directive",
        "define
  entity person, plays mentorship:mentor, plays mentorship:student;
  \
         relation mentorship, relates mentor, relates student;
",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE mentorship (_iid UBIGINT PRIMARY KEY, student_id UBIGINT REFERENCES person(_iid),
                                  mentor_id UBIGINT REFERENCES person(_iid));
         COMMENT ON TABLE mentorship IS '@typedb relation mentorship (mentor = mentor_id)';
         INSERT INTO person VALUES (1), (2);
         INSERT INTO mentorship VALUES (3, 1, 2);",
    )?;
    let items = convert(&dir)?;
    let item::Item::Relation(relation) = &items[items.len() - 2] else { panic!("the relation precedes the checksums") };
    let mut players: Vec<String> =
        relation.roles.iter().map(|role| format!("{}={}", role.label, role.players[0].id)).collect();
    players.sort();
    assert_eq!(players, ["mentorship:mentor=person:1", "mentorship:student=person:0"]);
    Ok(())
}

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
    assert_file_guarantees(&items);
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
    Ok(())
}

#[test]
fn a_column_named_rowid_does_not_disturb_the_scan() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "rowid",
        "define\n  attribute rowid value integer;\n  entity node, owns rowid, plays link:end;\n  \
         relation link, relates end @card(1..2);\n",
        "CREATE TABLE node (_iid UBIGINT PRIMARY KEY, rowid INTEGER);
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, rowid UBIGINT REFERENCES node(_iid),
                            b UBIGINT REFERENCES node(_iid));
         COMMENT ON TABLE link IS '@typedb relation link (end = rowid, end = b)';
         INSERT INTO node VALUES (1, NULL), (2, -1), (3, 200000);
         INSERT INTO link VALUES (1, 1, 2), (2, NULL, 3), (3, 3, 1);",
    )?;
    let items = convert(&dir)?;
    assert_file_guarantees(&items);
    let item::Item::Checksums(checksums) = items.last().unwrap() else { panic!() };
    assert_eq!((checksums.entity_count, checksums.relation_count, checksums.role_count), (3, 3, 5));
    Ok(())
}

#[test]
fn foreign_keys_are_matched_as_duckdb_resolves_them() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "fk-case",
        "define\n  entity person, plays link:end;\n  entity company, plays link:end;\n  relation link, relates end;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE company (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE link (_iid UBIGINT PRIMARY KEY, a UBIGINT REFERENCES Person(_IID), b UBIGINT,
                            FOREIGN KEY (b) REFERENCES person(_iid), FOREIGN KEY (b) REFERENCES company(_iid));
         INSERT INTO person VALUES (1);",
    )?;
    let errors = validation_errors(&dir);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_error(&errors, &["column 'b' is in several FOREIGN KEYs"]);
    Ok(())
}

#[test]
fn role_inference_does_not_depend_on_column_order() -> Result<(), Box<dyn Error>> {
    let schema = "define\n  entity person, plays r:a, plays r:b;\n  entity company, plays r:b;\n  \
                  relation r, relates a, relates b;\n";
    let tables = "CREATE TABLE person (_iid UBIGINT PRIMARY KEY); CREATE TABLE company (_iid UBIGINT PRIMARY KEY);";
    for (name, columns) in [
        ("order-1", "company_id UBIGINT REFERENCES company(_iid), person_id UBIGINT REFERENCES person(_iid)"),
        ("order-2", "person_id UBIGINT REFERENCES person(_iid), company_id UBIGINT REFERENCES company(_iid)"),
    ] {
        let dir = scratch(name, schema, &format!("{tables} CREATE TABLE r (_iid UBIGINT PRIMARY KEY, {columns});"))?;
        assert_error(&validation_errors(&dir), &["FK column 'person_id' is ambiguous", "(a = <column>, b = <column>)"]);
    }
    Ok(())
}

#[test]
fn directives_take_any_whitespace_and_keyword_case() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "directive-forms",
        "define\n  attribute name value string;\n  entity person, owns name;\n",
        "CREATE TABLE people (_iid UBIGINT PRIMARY KEY, full_name VARCHAR, extra INTEGER);
         COMMENT ON TABLE people IS '@TypeDB\tEntity\tperson';
         COMMENT ON COLUMN people.full_name IS ' @typedb OWNS   name ';
         COMMENT ON COLUMN people.extra IS '@typedb Skip';
         INSERT INTO people VALUES (1, 'Ann', 1);",
    )?;
    let items = convert(&dir)?;
    assert_file_guarantees(&items);
    assert!(items.iter().any(|item| matches!(item, item::Item::Entity(entity) if entity.id == "person:0")));
    Ok(())
}

#[test]
fn the_source_database_is_left_untouched() -> Result<(), Box<dyn Error>> {
    let dir = scratch(
        "read-only",
        "define\n  entity node;\n",
        "PRAGMA disable_checkpoint_on_shutdown;
         CREATE TABLE node (_iid UBIGINT PRIMARY KEY); INSERT INTO node VALUES (1);",
    )?;
    let db = dir.0.join("fixture.duckdb");
    let wal = dir.0.join("fixture.duckdb.wal");
    let before = (fs::read(&db)?, fs::read(&wal)?);
    let entities = convert(&dir)?.into_iter().filter(|item| matches!(item, item::Item::Entity(_))).count();
    assert_eq!(entities, 1);
    assert!(before == (fs::read(&db)?, fs::read(&wal)?), "the source database files changed");
    Ok(())
}

#[test]
fn relations_without_players_are_rejected() -> Result<(), Box<dyn Error>> {
    let schema = "define\n  entity person, plays friendship:friend;\n  relation friendship, relates friend;\n";
    let dir = scratch(
        "no_role_column",
        schema,
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE friendship (_iid UBIGINT PRIMARY KEY);
         COMMENT ON TABLE friendship IS '@typedb relation friendship';",
    )?;
    assert_error(&validation_errors(&dir), &["relation table 'friendship' fills no role"]);

    let dir = scratch(
        "skipped_role_column",
        schema,
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE friendship (_iid UBIGINT PRIMARY KEY, a UBIGINT REFERENCES person(_iid));
         COMMENT ON TABLE friendship IS '@typedb relation friendship';
         COMMENT ON COLUMN friendship.a IS '@typedb skip';",
    )?;
    assert_error(&validation_errors(&dir), &["relation table 'friendship' fills no role"]);

    let dir = scratch(
        "playerless",
        schema,
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY);
         CREATE TABLE friendship (_iid UBIGINT PRIMARY KEY, a UBIGINT REFERENCES person(_iid),
                                  b UBIGINT REFERENCES person(_iid));
         COMMENT ON TABLE friendship IS '@typedb relation friendship (friend = a, friend = b)';
         INSERT INTO person VALUES (1);
         INSERT INTO friendship VALUES (1, 1, NULL), (2, NULL, NULL);",
    )?;
    assert_error(&validation_errors(&dir), &["relation table 'friendship': 1 row(s) fill no role", "e.g. _iid 2"]);
    Ok(())
}

#[test]
fn an_output_error_ends_a_large_conversion() -> Result<(), Box<dyn Error>> {
    // More rows than the pipeline holds in flight, so that the workers outlive the writer's failure.
    let dir = scratch(
        "output-error",
        "define\n  attribute name value string;\n  entity person, owns name;\n",
        "CREATE TABLE person (_iid UBIGINT PRIMARY KEY, name VARCHAR);
         INSERT INTO person SELECT i, 'p' || i FROM range(1500000) t(i);",
    )?;
    struct Failing;
    impl io::Write for Failing {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("disk full"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let result = prepare(&dir)
            .map_err(|err| format!("{err:?}"))
            .and_then(|migration| migration.run(Failing, "3.0.0-test", "duckdb:fixture.duckdb").map(|_| ()));
        let _ = sender.send(result);
    });
    let result = receiver.recv_timeout(Duration::from_secs(120)).expect("the conversion must end, not hang");
    let error = result.expect_err("a failing output must fail the conversion");
    assert!(error.contains("failed to write the output file: disk full"), "{error}");
    Ok(())
}
