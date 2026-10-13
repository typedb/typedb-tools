/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};

use base64::Engine;

use crate::value::TypeDBValue;

// TODO: Heavily hardcoded now. Require them from the incoming data and probably let the server
// just reject an incorrect IID by the storage insert op?

const PREFIX_ENTITY: u8 = 0x1E;
const PREFIX_RELATION: u8 = 0x1F;
const PREFIX_ATTRIBUTE: u8 = 0x20;

const CATEGORY_BOOLEAN: u8 = 0;
const CATEGORY_INTEGER: u8 = 1;
const CATEGORY_DOUBLE: u8 = 2;
const CATEGORY_DECIMAL: u8 = 3;
const CATEGORY_DATE: u8 = 4;
const CATEGORY_DATETIME: u8 = 5;
const CATEGORY_DATETIME_TZ: u8 = 6;
const CATEGORY_DURATION: u8 = 7;
const CATEGORY_STRING: u8 = 8;

/// TypeDB's frozen IANA timezone table id for UTC (append-only by upstream policy).
const TIMEZONE_ID_UTC: u32 = 591;

const STRING_INLINE_LENGTH: usize = 16;
const STRING_HASHED_PREFIX_LENGTH: usize = 8;
const STRING_HASHED_FLAG: u8 = 0x80;
const STRING_MAX_DISAMBIGUATOR: u8 = 0x7F;

fn base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub(crate) fn entity_iid(type_id: u16, instance_id: u64) -> String {
    object_iid(PREFIX_ENTITY, type_id, instance_id)
}

pub(crate) fn relation_iid(type_id: u16, instance_id: u64) -> String {
    object_iid(PREFIX_RELATION, type_id, instance_id)
}

/// `[prefix][type id: u16 BE][object id: u64 BE]` — 11 bytes.
fn object_iid(prefix: u8, type_id: u16, instance_id: u64) -> String {
    let mut bytes = Vec::with_capacity(11);
    bytes.push(prefix);
    bytes.extend_from_slice(&type_id.to_be_bytes());
    bytes.extend_from_slice(&instance_id.to_be_bytes());
    base64(&bytes)
}

/// `[0x20][type id: u16 BE][value category][value encoding: 8 or 17 bytes]`.
pub(crate) fn attribute_iid(type_id: u16, value: &TypeDBValue, disambiguators: &StringDisambiguators) -> String {
    let (category, value_bytes) = encode_value(type_id, value, disambiguators);
    let mut bytes = Vec::with_capacity(4 + value_bytes.len());
    bytes.push(PREFIX_ATTRIBUTE);
    bytes.extend_from_slice(&type_id.to_be_bytes());
    bytes.push(category);
    bytes.extend_from_slice(&value_bytes);
    base64(&bytes)
}

/// A long string is identified by its prefix and hash, plus a disambiguator that tells
/// colliding values apart. The server gives each new colliding value the lowest free
/// disambiguator, so in a fresh database they follow creation order: the order of the
/// attribute stream. Collisions are rare enough that only the non-zero ones are kept.
#[derive(Debug, Default)]
pub(crate) struct StringDisambiguators {
    collided: HashMap<(u16, String), u8>,
}

impl StringDisambiguators {
    /// The hash identifying a string value, unless the value is short enough to be inlined.
    pub(crate) fn hash(value: &[u8]) -> Option<u64> {
        (value.len() > STRING_INLINE_LENGTH).then(|| seahash::hash(value))
    }

    /// The hashes occurring more than once; usually none.
    pub(crate) fn colliding_hashes(mut hashes: Vec<u64>) -> HashSet<u64> {
        hashes.sort_unstable();
        hashes.windows(2).filter(|pair| pair[0] == pair[1]).map(|pair| pair[0]).collect()
    }

    /// Assigns disambiguators to the values of one attribute type, in stream order, for
    /// those whose hash is among `colliding_hashes`.
    pub(crate) fn assign(
        &mut self,
        type_id: u16,
        colliding_hashes: &HashSet<u64>,
        next: &mut HashMap<([u8; STRING_HASHED_PREFIX_LENGTH], u64), u8>,
        value: &str,
    ) -> Result<(), String> {
        let bytes = value.as_bytes();
        let Some(hash) = Self::hash(bytes).filter(|hash| colliding_hashes.contains(hash)) else { return Ok(()) };
        let prefix = bytes[..STRING_HASHED_PREFIX_LENGTH].try_into().expect("length checked above");
        self.record(type_id, value, next.entry((prefix, hash)).or_insert(0))
    }

    fn record(&mut self, type_id: u16, value: &str, next_disambiguator: &mut u8) -> Result<(), String> {
        if *next_disambiguator > STRING_MAX_DISAMBIGUATOR {
            return Err(format!("more than {} string values share one hash", STRING_MAX_DISAMBIGUATOR as u32 + 1));
        }
        if *next_disambiguator > 0 {
            self.collided.insert((type_id, value.to_owned()), *next_disambiguator);
        }
        *next_disambiguator += 1;
        Ok(())
    }

    fn get(&self, type_id: u16, value: &str) -> u8 {
        if self.collided.is_empty() {
            return 0;
        }
        self.collided.get(&(type_id, value.to_owned())).copied().unwrap_or(0)
    }
}

fn encode_value(type_id: u16, value: &TypeDBValue, disambiguators: &StringDisambiguators) -> (u8, Vec<u8>) {
    match value {
        TypeDBValue::Boolean(value) => {
            let mut bytes = vec![0u8; 8];
            bytes[0] = *value as u8;
            (CATEGORY_BOOLEAN, bytes)
        }
        TypeDBValue::Integer(value) => (CATEGORY_INTEGER, sign_flipped_i64(*value).to_vec()),
        TypeDBValue::Double(value) => (CATEGORY_DOUBLE, order_preserving_f64(*value).to_vec()),
        TypeDBValue::Decimal { integer, fractional } => {
            let mut bytes = Vec::with_capacity(17);
            bytes.extend_from_slice(&sign_flipped_i64(*integer));
            bytes.extend_from_slice(&fractional.to_be_bytes());
            bytes.push(0);
            (CATEGORY_DECIMAL, bytes)
        }
        TypeDBValue::Date { days_since_ce } => {
            let mut bytes = vec![0u8; 8];
            bytes[..4].copy_from_slice(&sign_flipped_i32(*days_since_ce));
            (CATEGORY_DATE, bytes)
        }
        TypeDBValue::Datetime { seconds, nanos } => (CATEGORY_DATETIME, datetime_bytes(*seconds, *nanos, &[0u8; 5])),
        TypeDBValue::DatetimeTz { seconds, nanos } => {
            let mut tail = [0u8; 5];
            tail[..4].copy_from_slice(&TIMEZONE_ID_UTC.to_be_bytes());
            (CATEGORY_DATETIME_TZ, datetime_bytes(*seconds, *nanos, &tail))
        }
        TypeDBValue::Duration { months, days, nanos } => {
            let mut bytes = Vec::with_capacity(17);
            bytes.extend_from_slice(&months.to_be_bytes());
            bytes.extend_from_slice(&days.to_be_bytes());
            bytes.extend_from_slice(&nanos.to_be_bytes());
            bytes.push(0);
            (CATEGORY_DURATION, bytes)
        }
        TypeDBValue::String(value) => {
            let string_bytes = value.as_bytes();
            let mut bytes = Vec::with_capacity(17);
            if string_bytes.len() <= STRING_INLINE_LENGTH {
                bytes.extend_from_slice(string_bytes);
                bytes.resize(STRING_INLINE_LENGTH, 0);
                bytes.push(string_bytes.len() as u8);
            } else {
                bytes.extend_from_slice(&string_bytes[..STRING_HASHED_PREFIX_LENGTH]);
                bytes.extend_from_slice(&seahash::hash(string_bytes).to_be_bytes());
                bytes.push(STRING_HASHED_FLAG | disambiguators.get(type_id, value));
            }
            (CATEGORY_STRING, bytes)
        }
    }
}

fn datetime_bytes(seconds: i64, nanos: u32, tail: &[u8; 5]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(17);
    bytes.extend_from_slice(&sign_flipped_i64(seconds));
    bytes.extend_from_slice(&nanos.to_be_bytes());
    bytes.extend_from_slice(tail);
    bytes
}

fn sign_flipped_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

fn sign_flipped_i32(value: i32) -> [u8; 4] {
    ((value as u32) ^ 0x8000_0000).to_be_bytes()
}

/// Order-preserving f64 encoding: flip the sign bit for positives, all bits for negatives.
fn order_preserving_f64(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let mapped = if bits & 0x8000_0000_0000_0000 == 0 { bits ^ 0x8000_0000_0000_0000 } else { !bits };
    mapped.to_be_bytes()
}

#[cfg(test)]
mod test {
    //! Golden ids captured from a real `typedb export` (TypeDB 3.13.0) of values imported
    //! with the schema type ids used below. If any of these fail after an upstream
    //! encoding change, the synthesised format no longer matches that server version.

    use super::*;
    use crate::value::EPOCH_DAYS_FROM_CE;

    fn golden_attribute_iid(type_id: u16, value: &TypeDBValue) -> String {
        attribute_iid(type_id, value, &StringDisambiguators::default())
    }

    #[test]
    fn golden_object_iids() {
        assert_eq!(entity_iid(0, 0), "HgAAAAAAAAAAAAA=");
        assert_eq!(entity_iid(1, 0), "HgABAAAAAAAAAAA=");
        assert_eq!(relation_iid(0, 0), "HwAAAAAAAAAAAAA=");
    }

    #[test]
    fn golden_integer_boolean_double() {
        assert_eq!(golden_attribute_iid(2, &TypeDBValue::Integer(30)), "IAACAYAAAAAAAAAe");
        assert_eq!(golden_attribute_iid(5, &TypeDBValue::Boolean(true)), "IAAFAAEAAAAAAAAA");
        assert_eq!(golden_attribute_iid(3, &TypeDBValue::Double(1.75)), "IAADAr/8AAAAAAAA");
    }

    #[test]
    fn golden_strings_inline_and_hashed() {
        assert_eq!(golden_attribute_iid(0, &TypeDBValue::String("Alice".to_owned())), "IAAACEFsaWNlAAAAAAAAAAAAAAAF");
        // 36 chars: 8-byte prefix + seahash + hashed flag
        assert_eq!(
            golden_attribute_iid(11, &TypeDBValue::String("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".to_owned())),
            "IAALCGEwZWViYzk5BUUB2QjWkGqA"
        );
    }

    #[test]
    fn golden_decimal_date_datetime_tz() {
        // 1234.5678
        assert_eq!(
            golden_attribute_iid(4, &TypeDBValue::Decimal { integer: 1234, fractional: 5678 * 10u64.pow(15) }),
            "IAAEA4AAAAAAAATSTsxO6qqrAAAA"
        );
        // 1995-01-15
        assert_eq!(
            golden_attribute_iid(6, &TypeDBValue::Date { days_since_ce: 9145 + EPOCH_DAYS_FROM_CE }),
            "IAAGBIALHPQAAAAA"
        );
        // 2024-01-01T12:00:00
        assert_eq!(
            golden_attribute_iid(7, &TypeDBValue::Datetime { seconds: 1704110400, nanos: 0 }),
            "IAAHBYAAAABlkqlAAAAAAAAAAAAA"
        );
        // 2024-01-01T10:00:00 UTC
        assert_eq!(
            golden_attribute_iid(8, &TypeDBValue::DatetimeTz { seconds: 1704103200, nanos: 0 }),
            "IAAIBoAAAABlko0gAAAAAAAAAk8A"
        );
    }

    #[test]
    fn colliding_string_hashes_are_disambiguated_in_stream_order() {
        // Real seahash collisions are infeasible to construct: record two values as colliding.
        let (first, second) = ("https://example.com/first", "https://example.com/second");
        let mut disambiguators = StringDisambiguators::default();
        let mut next_disambiguator = 0;
        disambiguators.record(0, first, &mut next_disambiguator).unwrap();
        disambiguators.record(0, second, &mut next_disambiguator).unwrap();

        let decode = |iid: String| base64::engine::general_purpose::STANDARD.decode(iid).unwrap();
        let first_iid = decode(attribute_iid(0, &TypeDBValue::String(first.to_owned()), &disambiguators));
        let second_iid = decode(attribute_iid(0, &TypeDBValue::String(second.to_owned()), &disambiguators));
        assert_eq!(first_iid.last(), Some(&0x80));
        assert_eq!(second_iid.last(), Some(&0x81));
    }

    #[test]
    fn colliding_hashes_are_found() {
        let values: [&[u8]; 4] =
            [b"a long string value, 1", b"short", b"a long string value, 1", b"a long string value, 2"];
        let hashes = values.iter().filter_map(|value| StringDisambiguators::hash(value)).collect();
        assert_eq!(StringDisambiguators::colliding_hashes(hashes), [seahash::hash(values[0])].into());
    }
}
