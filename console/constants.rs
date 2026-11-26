/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::time::Duration;

use typedb_cli_common::constants::{SECONDS_IN_HOUR, SECONDS_IN_MINUTE};

pub const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(1 * SECONDS_IN_HOUR);
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10 * SECONDS_IN_MINUTE);
