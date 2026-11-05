/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub const SECONDS_IN_MINUTE: u64 = 60;
pub const MINUTES_IN_HOUR: u64 = 60;
pub const SECONDS_IN_HOUR: u64 = SECONDS_IN_MINUTE * MINUTES_IN_HOUR;
pub const MONTHS_IN_YEAR: u64 = 12;

pub const MILLIS_IN_SECOND: u64 = 1_000;
pub const MICROS_IN_SECOND: u64 = 1_000 * MILLIS_IN_SECOND;
pub const NANOS_IN_SECOND: u64 = 1_000 * MICROS_IN_SECOND;
pub const NANOS_IN_MICRO: u64 = NANOS_IN_SECOND / MICROS_IN_SECOND;

pub const KB: u64 = 1024;
pub const MB: u64 = 1024 * KB;
pub const GB: u64 = 1024 * MB;
pub const TB: u64 = 1024 * GB;
