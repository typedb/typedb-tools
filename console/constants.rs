/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::time::Duration;

use crate::constants::common::{SECONDS_IN_HOUR, SECONDS_IN_MINUTE};

pub mod common {
    pub const SECONDS_IN_MINUTE: u64 = 60;
    pub const MINUTES_IN_HOUR: u64 = 60;
    pub const SECONDS_IN_HOUR: u64 = SECONDS_IN_MINUTE * MINUTES_IN_HOUR;
}

pub const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(1 * SECONDS_IN_HOUR);
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10 * SECONDS_IN_MINUTE);

pub const FORCE_FLAG: &str = "--force";
pub const FORCE_FLAG_SHORT: &str = "-f";
