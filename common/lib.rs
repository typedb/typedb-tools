/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Connection helpers shared between the typedb-console and typedb-loader binaries.
//!
//! These functions are intentionally infrastructure-only: they parse user-supplied strings and
//! build driver config objects, but never decide how a caller should report errors or exit.

mod addresses;
pub mod printer;
mod tls;

pub use addresses::{parse_address_translation, parse_addresses};
pub use tls::build_tls_config;
