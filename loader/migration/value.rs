/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use duckdb::types::{TimeUnit, Value, ValueRef};
use typedb_protocol::{
    migration::{MigrationValue, migration_value},
    value as proto_value,
};

use crate::schema::ValueType;

/// Days from 0001-01-01 (day 1, Gregorian) to the Unix epoch, per chrono's `num_days_from_ce`.
pub(crate) const EPOCH_DAYS_FROM_CE: i32 = 719_163;
pub(crate) const DECIMAL_FRACTIONAL_DIGITS: u32 = 19;

/// The date range TypeDB can store (chrono's `NaiveDate::MIN..=MAX`), in days from the epoch.
pub(crate) const MIN_EPOCH_DAYS: i64 = -95_746_129 - EPOCH_DAYS_FROM_CE as i64;
pub(crate) const MAX_EPOCH_DAYS: i64 = 95_745_399 - EPOCH_DAYS_FROM_CE as i64;
/// The datetime range TypeDB can store (chrono's `NaiveDateTime::MIN..=MAX`), in seconds
/// from the epoch.
pub(crate) const MIN_EPOCH_SECONDS: i64 = -8_334_601_228_800;
pub(crate) const MAX_EPOCH_SECONDS: i64 = 8_210_266_876_799;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TypeDBValue {
    Boolean(bool),
    Integer(i64),
    Double(f64),
    Decimal {
        integer: i64,
        fractional: u64,
    },
    String(String),
    Date {
        days_since_ce: i32,
    },
    Datetime {
        seconds: i64,
        nanos: u32,
    },
    /// DuckDB stores TIMESTAMPTZ as a UTC instant, so the zone is always UTC.
    DatetimeTz {
        seconds: i64,
        nanos: u32,
    },
    Duration {
        months: u32,
        days: u32,
        nanos: u64,
    },
}

impl TypeDBValue {
    pub(crate) fn to_proto(&self) -> MigrationValue {
        use migration_value::Value as V;
        let value = match self.clone() {
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

pub(crate) fn convert_cell(cell: ValueRef<'_>, target: ValueType) -> Result<Option<TypeDBValue>, String> {
    let converted = match (cell, target) {
        (ValueRef::Null, _) => return Ok(None),

        (ValueRef::Boolean(value), ValueType::Boolean) => TypeDBValue::Boolean(value),

        (ValueRef::TinyInt(value), _) => return convert_integer(value as i64, target),
        (ValueRef::SmallInt(value), _) => return convert_integer(value as i64, target),
        (ValueRef::Int(value), _) => return convert_integer(value as i64, target),
        (ValueRef::BigInt(value), _) => return convert_integer(value, target),
        (ValueRef::UTinyInt(value), _) => return convert_integer(value as i64, target),
        (ValueRef::USmallInt(value), _) => return convert_integer(value as i64, target),
        (ValueRef::UInt(value), _) => return convert_integer(value as i64, target),
        (ValueRef::UBigInt(value), _) => {
            let value = i64::try_from(value)
                .map_err(|_| format!("UBIGINT value {value} does not fit TypeDB's 64-bit integer"))?;
            return convert_integer(value, target);
        }
        (ValueRef::HugeInt(value), _) => {
            let value = i64::try_from(value)
                .map_err(|_| format!("HUGEINT value {value} does not fit TypeDB's 64-bit integer"))?;
            return convert_integer(value, target);
        }

        (ValueRef::Float(value), ValueType::Double) => return convert_double(value as f64),
        (ValueRef::Double(value), ValueType::Double) => return convert_double(value),

        (ValueRef::Decimal(decimal), ValueType::Decimal) => decimal_to_typedb(decimal.value(), decimal.scale() as u32)?,

        (ValueRef::Text(bytes), ValueType::String) => TypeDBValue::String(
            String::from_utf8(bytes.to_vec()).map_err(|_| "invalid UTF-8 in VARCHAR value".to_owned())?,
        ),

        (ValueRef::Date32(days), ValueType::Date) => {
            if !(MIN_EPOCH_DAYS..=MAX_EPOCH_DAYS).contains(&(days as i64)) {
                return Err(format!("DATE {days} days from 1970-01-01 is outside TypeDB's date range"));
            }
            TypeDBValue::Date { days_since_ce: days + EPOCH_DAYS_FROM_CE }
        }

        (ValueRef::Timestamp(unit, raw), ValueType::Datetime) => {
            let (seconds, nanos) = timestamp_parts(unit, raw)?;
            TypeDBValue::Datetime { seconds, nanos }
        }
        (ValueRef::Timestamp(unit, raw), ValueType::DatetimeTz) => {
            let (seconds, nanos) = timestamp_parts(unit, raw)?;
            TypeDBValue::DatetimeTz { seconds, nanos }
        }

        (ValueRef::Interval { months, days, nanos }, ValueType::Duration) => {
            if months < 0 || days < 0 || nanos < 0 {
                return Err(format!(
                    "negative INTERVAL ({months} months, {days} days, {nanos} ns) cannot be a TypeDB duration"
                ));
            }
            TypeDBValue::Duration { months: months as u32, days: days as u32, nanos: nanos as u64 }
        }

        (cell, target) => {
            return Err(format!("cannot convert a {cell:?} cell to TypeDB value type '{}'", target.name()));
        }
    };
    Ok(Some(converted))
}

pub(crate) fn convert_list_element(element: Value, target: ValueType) -> Result<Option<TypeDBValue>, String> {
    match element {
        Value::Enum(_) | Value::List(_) | Value::Struct(_) | Value::Map(_) | Value::Array(_) | Value::Union(_) => {
            Err(format!("cannot convert list element {element:?} to value type '{}'", target.name()))
        }
        scalar => convert_cell(ValueRef::from(&scalar), target),
    }
}

fn convert_integer(value: i64, target: ValueType) -> Result<Option<TypeDBValue>, String> {
    Ok(Some(match target {
        ValueType::Integer => TypeDBValue::Integer(value),
        ValueType::Double => {
            if value as f64 as i128 != value as i128 {
                return Err(format!("integer {value} has no exact double representation"));
            }
            TypeDBValue::Double(value as f64)
        }
        ValueType::Decimal => TypeDBValue::Decimal { integer: value, fractional: 0 },
        other => return Err(format!("cannot convert an integer cell to TypeDB value type '{}'", other.name())),
    }))
}

fn convert_double(value: f64) -> Result<Option<TypeDBValue>, String> {
    if !value.is_finite() {
        return Err(format!("{value} is not a finite double, which TypeDB requires"));
    }
    Ok(Some(TypeDBValue::Double(if value == 0.0 { 0.0 } else { value })))
}

fn timestamp_parts(unit: TimeUnit, raw: i64) -> Result<(i64, u32), String> {
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

fn decimal_to_typedb(unscaled: i128, scale: u32) -> Result<TypeDBValue, String> {
    if scale > DECIMAL_FRACTIONAL_DIGITS {
        return Err(format!("DECIMAL scale {scale} exceeds TypeDB's 19 fractional digits"));
    }
    let pow = 10i128.pow(scale);
    let integer = unscaled.div_euclid(pow);
    let fractional_units = unscaled.rem_euclid(pow) as u64;
    let integer = i64::try_from(integer)
        .map_err(|_| format!("DECIMAL integer part {integer} does not fit TypeDB's decimal range"))?;
    let fractional = fractional_units * 10u64.pow(DECIMAL_FRACTIONAL_DIGITS - scale);
    Ok(TypeDBValue::Decimal { integer, fractional })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn decimal_conversion_floors_negatives() {
        // -9.59 == (-10, 0.41): TypeDB decimals hold a floor integer and a non-negative fraction
        let TypeDBValue::Decimal { integer, fractional } = decimal_to_typedb(-959, 2).unwrap() else { panic!() };
        assert_eq!(integer, -10);
        assert_eq!(fractional, 41 * 10u64.pow(17));
    }

    #[test]
    fn decimal_conversion_is_exact() {
        let TypeDBValue::Decimal { integer, fractional } = decimal_to_typedb(12345678, 4).unwrap() else { panic!() };
        assert_eq!(integer, 1234);
        assert_eq!(fractional, 5678 * 10u64.pow(15));
    }

    #[test]
    fn timestamps_split_exactly_at_every_unit() {
        assert_eq!(timestamp_parts(TimeUnit::Nanosecond, 1_577_836_800_000_000_001), Ok((1_577_836_800, 1)));
        assert_eq!(timestamp_parts(TimeUnit::Nanosecond, -1), Ok((-1, 999_999_999)));
        assert_eq!(timestamp_parts(TimeUnit::Microsecond, -1_500_000), Ok((-2, 500_000_000)));
        assert_eq!(timestamp_parts(TimeUnit::Millisecond, -1), Ok((-1, 999_000_000)));
        assert_eq!(timestamp_parts(TimeUnit::Second, -1), Ok((-1, 0)));
        assert!(timestamp_parts(TimeUnit::Microsecond, i64::MAX).is_err());
    }

    #[test]
    fn out_of_range_and_inexact_values_are_rejected() {
        assert!(convert_cell(ValueRef::Date32(i32::MAX), ValueType::Date).is_err());
        assert!(convert_cell(ValueRef::Double(f64::NAN), ValueType::Double).is_err());
        assert!(convert_cell(ValueRef::Double(f64::INFINITY), ValueType::Double).is_err());
        assert!(convert_cell(ValueRef::BigInt(9_007_199_254_740_993), ValueType::Double).is_err());
        assert!(convert_cell(ValueRef::BigInt(i64::MAX), ValueType::Double).is_err());
        for negative_zero in [ValueRef::Double(-0.0), ValueRef::Float(-0.0)] {
            let Ok(Some(TypeDBValue::Double(zero))) = convert_cell(negative_zero, ValueType::Double) else { panic!() };
            assert!(zero == 0.0 && zero.is_sign_positive());
        }
        assert_eq!(
            convert_cell(ValueRef::BigInt(9_007_199_254_740_992), ValueType::Double),
            Ok(Some(TypeDBValue::Double(9_007_199_254_740_992.0)))
        );
    }

    #[test]
    fn list_elements_convert_like_cells() {
        let date = convert_list_element(Value::Date32(0), ValueType::Date).unwrap();
        assert_eq!(date, Some(TypeDBValue::Date { days_since_ce: EPOCH_DAYS_FROM_CE }));
        let interval = Value::Interval { months: 1, days: 2, nanos: 3 };
        let duration = convert_list_element(interval, ValueType::Duration).unwrap();
        assert_eq!(duration, Some(TypeDBValue::Duration { months: 1, days: 2, nanos: 3 }));
    }

    #[test]
    fn decimal_scale_and_range_are_checked() {
        assert!(decimal_to_typedb(1, 20).is_err());
        assert!(decimal_to_typedb(i128::MAX, 0).is_err());
    }
}
