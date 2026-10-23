/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use duckdb::types::{TimeUnit, Value, ValueRef};
use typedb_protocol::{
    migration::{MigrationValue, migration_value},
    value as proto_value,
};

use crate::schema::ValueType;

// Days from 0001-01-01 to 1970-01-01.
pub(crate) const EPOCH_DAYS_FROM_CE: i32 = 719_163;
pub(crate) const DECIMAL_FRACTIONAL_DIGITS: u8 = 19;

// chrono's `NaiveDate::MIN..=MAX` and `NaiveDateTime::MIN..=MAX`, from the epoch.
const MIN_EPOCH_DAYS: i64 = -95_746_129 - EPOCH_DAYS_FROM_CE as i64;
const MAX_EPOCH_DAYS: i64 = 95_745_399 - EPOCH_DAYS_FROM_CE as i64;
const MIN_EPOCH_SECONDS: i64 = -8_334_601_228_800;
const MAX_EPOCH_SECONDS: i64 = 8_210_266_876_799;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TypeDBValue {
    Boolean(bool),
    Integer(i64),
    Double(f64),
    Decimal { integer: i64, fractional: u64 },
    String(String),
    Date { days_since_ce: i32 },
    Datetime { seconds: i64, nanos: u32 },
    // DuckDB stores TIMESTAMPTZ as a UTC instant.
    DatetimeTz { seconds: i64, nanos: u32 },
    Duration { months: u32, days: u32, nanos: u64 },
}

impl TypeDBValue {
    pub(crate) fn into_proto(self) -> MigrationValue {
        use migration_value::Value as V;
        let value = match self {
            TypeDBValue::Boolean(value) => V::Boolean(value),
            TypeDBValue::Integer(value) => V::Integer(value),
            TypeDBValue::Double(value) => V::Double(value),
            TypeDBValue::Decimal { integer, fractional } => V::Decimal(proto_value::Decimal { integer, fractional }),
            TypeDBValue::String(value) => V::String(value),
            TypeDBValue::Date { days_since_ce } => V::Date(proto_value::Date { num_days_since_ce: days_since_ce }),
            TypeDBValue::Datetime { seconds, nanos } => V::Datetime(proto_value::Datetime { seconds, nanos }),
            TypeDBValue::DatetimeTz { seconds, nanos } => V::DatetimeTz(proto_value::DatetimeTz {
                datetime: Some(proto_value::Datetime { seconds, nanos }),
                timezone: Some(proto_value::datetime_tz::Timezone::Named("UTC".to_owned())),
            }),
            TypeDBValue::Duration { months, days, nanos } => V::Duration(proto_value::Duration { months, days, nanos }),
        };
        MigrationValue { value: Some(value) }
    }
}

// The text identifies the value: it makes the attributes' ids.
impl fmt::Display for TypeDBValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeDBValue::Boolean(value) => write!(f, "{value}"),
            TypeDBValue::Integer(value) => write!(f, "{value}"),
            TypeDBValue::Double(value) => write!(f, "{value:?}"),
            // Stored as a floor integer and a non-negative fraction: -0.5 is (-1, 0.5).
            TypeDBValue::Decimal { integer, fractional } if *integer < 0 && *fractional > 0 => {
                write!(f, "-{}.{:019}", -(integer + 1), 10u64.pow(DECIMAL_FRACTIONAL_DIGITS.into()) - fractional)
            }
            TypeDBValue::Decimal { integer, fractional } => write!(f, "{integer}.{fractional:019}"),
            TypeDBValue::String(value) => f.write_str(value),
            TypeDBValue::Date { days_since_ce } => write!(f, "{days_since_ce}"),
            TypeDBValue::Datetime { seconds, nanos } | TypeDBValue::DatetimeTz { seconds, nanos } => {
                write!(f, "{seconds}.{nanos:09}")
            }
            TypeDBValue::Duration { months, days, nanos } => write!(f, "{months}m{days}d{nanos}n"),
        }
    }
}

pub(crate) fn convert_cell(cell: ValueRef<'_>, target: ValueType) -> Result<Option<TypeDBValue>, String> {
    let converted = match (cell, target) {
        (ValueRef::Null, _) => return Ok(None),
        (ValueRef::Boolean(value), ValueType::Boolean) => TypeDBValue::Boolean(value),

        (ValueRef::TinyInt(value), ValueType::Integer) => TypeDBValue::Integer(value.into()),
        (ValueRef::SmallInt(value), ValueType::Integer) => TypeDBValue::Integer(value.into()),
        (ValueRef::Int(value), ValueType::Integer) => TypeDBValue::Integer(value.into()),
        (ValueRef::BigInt(value), ValueType::Integer) => TypeDBValue::Integer(value),
        (ValueRef::UTinyInt(value), ValueType::Integer) => TypeDBValue::Integer(value.into()),
        (ValueRef::USmallInt(value), ValueType::Integer) => TypeDBValue::Integer(value.into()),
        (ValueRef::UInt(value), ValueType::Integer) => TypeDBValue::Integer(value.into()),
        (ValueRef::UBigInt(value), ValueType::Integer) => TypeDBValue::Integer(
            i64::try_from(value).map_err(|_| format!("UBIGINT value {value} does not fit TypeDB's 64-bit integer"))?,
        ),
        (ValueRef::HugeInt(value), ValueType::Integer) => TypeDBValue::Integer(
            i64::try_from(value).map_err(|_| format!("HUGEINT value {value} does not fit TypeDB's 64-bit integer"))?,
        ),

        (ValueRef::Float(value), ValueType::Double) => double(value.into())?,
        (ValueRef::Double(value), ValueType::Double) => double(value)?,
        (ValueRef::Decimal(value), ValueType::Decimal) => decimal(value.value(), value.scale() as u32)?,

        (ValueRef::Text(bytes), ValueType::String) => TypeDBValue::String(
            String::from_utf8(bytes.to_vec()).map_err(|_| "invalid UTF-8 in VARCHAR value".to_owned())?,
        ),

        (ValueRef::Date32(days), ValueType::Date) => date(days)?,
        (ValueRef::Timestamp(unit, raw), ValueType::Datetime) => {
            let (seconds, nanos) = timestamp_parts(unit, raw)?;
            TypeDBValue::Datetime { seconds, nanos }
        }
        (ValueRef::Timestamp(unit, raw), ValueType::DatetimeTz) => {
            let (seconds, nanos) = timestamp_parts(unit, raw)?;
            TypeDBValue::DatetimeTz { seconds, nanos }
        }
        (ValueRef::Text(bytes), ValueType::Duration) => {
            duration(std::str::from_utf8(bytes).map_err(|_| "invalid UTF-8 in INTERVAL text".to_owned())?)?
        }

        (cell, target) => {
            return Err(format!("cannot convert a {cell:?} cell to TypeDB value type '{}'", target.name()));
        }
    };
    Ok(Some(converted))
}

// `ValueRef::from` panics on these.
pub(crate) fn convert_list_element(element: Value, target: ValueType) -> Result<Option<TypeDBValue>, String> {
    match element {
        Value::Enum(_) | Value::List(_) | Value::Struct(_) | Value::Map(_) | Value::Array(_) | Value::Union(_) => {
            Err(format!("cannot convert list element {element:?} to value type '{}'", target.name()))
        }
        scalar => convert_cell(ValueRef::from(&scalar), target),
    }
}

// DuckDB does not keep -0.0 and 0.0 apart, while TypeDB would store two attributes.
fn double(value: f64) -> Result<TypeDBValue, String> {
    if !value.is_finite() {
        return Err(format!("{value} is not a finite double, which TypeDB requires"));
    }
    Ok(TypeDBValue::Double(if value == 0.0 { 0.0 } else { value }))
}

fn decimal(unscaled: i128, scale: u32) -> Result<TypeDBValue, String> {
    let pow = 10i128.pow(scale);
    let integer = unscaled.div_euclid(pow);
    let fractional_units = unscaled.rem_euclid(pow) as u64;
    let integer = i64::try_from(integer)
        .map_err(|_| format!("DECIMAL integer part {integer} does not fit TypeDB's decimal range"))?;
    Ok(TypeDBValue::Decimal {
        integer,
        fractional: fractional_units * 10u64.pow(u32::from(DECIMAL_FRACTIONAL_DIGITS) - scale),
    })
}

// `[N year(s)] [N month(s)] [N day(s)] [-]H:MM:SS[.ffffff]`, every part optional and possibly negative.
fn duration(text: &str) -> Result<TypeDBValue, String> {
    let (mut months, mut days, mut micros) = (0i64, 0i64, 0i128);
    let mut tokens = text.split_whitespace();
    while let Some(token) = tokens.next() {
        let invalid = || format!("unexpected INTERVAL text '{text}'");
        if token.contains(':') {
            let (sign, time) = match token.strip_prefix('-') {
                Some(time) => (-1, time),
                None => (1, token),
            };
            let [hours, minutes, seconds] = time.split(':').collect::<Vec<_>>()[..] else { return Err(invalid()) };
            let (seconds, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
            let fraction: i128 = format!("{fraction:0<6}").parse().map_err(|_| invalid())?;
            let whole = |part: &str| part.parse::<i128>().map_err(|_| invalid());
            micros = sign * (((whole(hours)? * 60 + whole(minutes)?) * 60 + whole(seconds)?) * 1_000_000 + fraction);
        } else {
            let count: i64 = token.parse().map_err(|_| invalid())?;
            match tokens.next().map(|unit| unit.trim_end_matches('s')) {
                Some("year") => months += 12 * count,
                Some("month") => months += count,
                Some("day") => days += count,
                _ => return Err(invalid()),
            }
        }
    }
    if months < 0 || days < 0 || micros < 0 {
        return Err(format!("negative INTERVAL '{text}' cannot be a TypeDB duration"));
    }
    let nanos = u64::try_from(micros)
        .ok()
        .and_then(|micros| micros.checked_mul(1000))
        .ok_or_else(|| format!("INTERVAL '{text}' has a time part beyond TypeDB's duration range"))?;
    Ok(TypeDBValue::Duration { months: months as u32, days: days as u32, nanos })
}

// DuckDB encodes ±infinity as ±`i32::MAX` days.
fn date(days_from_epoch: i32) -> Result<TypeDBValue, String> {
    if days_from_epoch.unsigned_abs() == i32::MAX as u32 {
        return Err("infinite DATE cannot be a TypeDB date".to_owned());
    }
    if !(MIN_EPOCH_DAYS..=MAX_EPOCH_DAYS).contains(&days_from_epoch.into()) {
        return Err(format!("DATE {days_from_epoch} days from 1970-01-01 is outside TypeDB's date range"));
    }
    Ok(TypeDBValue::Date { days_since_ce: days_from_epoch + EPOCH_DAYS_FROM_CE })
}

// DuckDB encodes ±infinity as ±`i64::MAX`, whatever the unit.
fn timestamp_parts(unit: TimeUnit, raw: i64) -> Result<(i64, u32), String> {
    if raw.unsigned_abs() == i64::MAX as u64 {
        return Err("infinite TIMESTAMP cannot be a TypeDB datetime".to_owned());
    }
    let units_per_second: i64 = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let seconds = raw.div_euclid(units_per_second);
    if !(MIN_EPOCH_SECONDS..=MAX_EPOCH_SECONDS).contains(&seconds) {
        return Err(format!("timestamp {seconds} seconds from 1970-01-01 is outside TypeDB's datetime range"));
    }
    let nanos = raw.rem_euclid(units_per_second) * (1_000_000_000 / units_per_second);
    Ok((seconds, nanos as u32))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn decimal_conversion_floors_negatives() {
        let TypeDBValue::Decimal { integer, fractional } = decimal(-959, 2).unwrap() else { panic!() };
        assert_eq!(integer, -10);
        assert_eq!(fractional, 41 * 10u64.pow(17));
        assert_eq!(decimal(-959, 2).unwrap().to_string(), "-9.5900000000000000000");
        assert_eq!(decimal(-50, 2).unwrap().to_string(), "-0.5000000000000000000");
        assert_eq!(decimal(-100, 2).unwrap().to_string(), "-1.0000000000000000000");
    }

    #[test]
    fn decimal_conversion_is_exact() {
        let TypeDBValue::Decimal { integer, fractional } = decimal(12345678, 4).unwrap() else { panic!() };
        assert_eq!(integer, 1234);
        assert_eq!(fractional, 5678 * 10u64.pow(15));
        assert!(decimal(i128::MAX, 0).is_err());
    }

    #[test]
    fn timestamps_split_exactly_at_every_unit() {
        assert_eq!(timestamp_parts(TimeUnit::Nanosecond, 1_577_836_800_000_000_001), Ok((1_577_836_800, 1)));
        assert_eq!(timestamp_parts(TimeUnit::Nanosecond, -1), Ok((-1, 999_999_999)));
        assert_eq!(timestamp_parts(TimeUnit::Microsecond, -1_500_000), Ok((-2, 500_000_000)));
        assert_eq!(timestamp_parts(TimeUnit::Millisecond, -1), Ok((-1, 999_000_000)));
        assert_eq!(timestamp_parts(TimeUnit::Second, -1), Ok((-1, 0)));
        assert!(timestamp_parts(TimeUnit::Microsecond, 8_210_266_876_800_000_000).is_err());
        for infinity in [i64::MAX, -i64::MAX] {
            assert!(timestamp_parts(TimeUnit::Nanosecond, infinity).unwrap_err().contains("infinite"));
        }
    }

    #[test]
    fn values_typedb_cannot_hold_are_rejected() {
        assert!(convert_cell(ValueRef::Date32(i32::MAX), ValueType::Date).unwrap_err().contains("infinite"));
        assert!(convert_cell(ValueRef::Date32(-100_000_000), ValueType::Date).unwrap_err().contains("range"));
        assert!(convert_cell(ValueRef::Double(f64::NAN), ValueType::Double).is_err());
        assert!(convert_cell(ValueRef::Double(f64::INFINITY), ValueType::Double).is_err());
        assert!(convert_cell(ValueRef::UBigInt(u64::MAX), ValueType::Integer).is_err());
        assert!(convert_cell(ValueRef::HugeInt(i128::MIN), ValueType::Integer).is_err());
    }

    #[test]
    fn intervals_parse_from_their_text_form() {
        let parse = |text: &str| {
            duration(text).map(|value| match value {
                TypeDBValue::Duration { months, days, nanos } => (months, days, nanos),
                other => panic!("{other:?}"),
            })
        };
        assert_eq!(parse("00:00:00"), Ok((0, 0, 0)));
        assert_eq!(parse("1 year 1 month"), Ok((13, 0, 0)));
        assert_eq!(parse("2 days 02:00:00"), Ok((0, 2, 7_200_000_000_000)));
        assert_eq!(parse("01:30:00.000001"), Ok((0, 0, 5_400_000_001_000)));
        assert_eq!(parse("00:00:01.5"), Ok((0, 0, 1_500_000_000)));
        assert_eq!(parse("2562048:00:00"), Ok((0, 0, 2_562_048 * 3_600_000_000_000)));
        assert!(parse("1 month -2 days 03:00:00").unwrap_err().contains("negative"));
        assert!(parse("-00:00:00.000001").unwrap_err().contains("negative"));
        assert!(parse("-2562047788:00:54.775808").unwrap_err().contains("negative"));
        assert!(parse("5124096:00:00").unwrap_err().contains("beyond"));
        assert!(parse("1 fortnight").unwrap_err().contains("unexpected"));
    }

    #[test]
    fn negative_zero_is_read_as_zero() {
        for negative_zero in [ValueRef::Double(-0.0), ValueRef::Float(-0.0)] {
            let Ok(Some(TypeDBValue::Double(zero))) = convert_cell(negative_zero, ValueType::Double) else { panic!() };
            assert!(zero == 0.0 && zero.is_sign_positive());
        }
    }

    #[test]
    fn list_elements_convert_like_cells() {
        let date = convert_list_element(Value::Date32(0), ValueType::Date).unwrap();
        assert_eq!(date, Some(TypeDBValue::Date { days_since_ce: EPOCH_DAYS_FROM_CE }));
        let interval = Value::Text("1 month 2 days 00:00:00.000003".to_owned());
        let duration = convert_list_element(interval, ValueType::Duration).unwrap();
        assert_eq!(duration, Some(TypeDBValue::Duration { months: 1, days: 2, nanos: 3000 }));
    }
}
