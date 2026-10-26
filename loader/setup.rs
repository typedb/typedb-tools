/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs::read_to_string, path::Path};

use typedb_cli_common::{build_tls_config, parse_addresses};
use typedb_driver::{Credentials, DriverOptions, TransactionType, TypeDBDriver};

use crate::{ExitCode, fatal, fatal_with, params::Params};

/// Establishes the database connection ready for the load: connects, optionally creates the
/// target database, and optionally applies the schema file. The create/apply steps are skipped
/// on resume — the database already exists and the original schema is assumed in place.
pub(crate) async fn connect_and_initialize(params: &Params, password: &str, resuming: bool) -> TypeDBDriver {
    let driver = connect(params, password).await;
    if !resuming && params.create_db {
        let created = create_database_if_missing(&driver, &params.database).await;
        if created && params.schema_file.is_none() {
            eprintln!(
                "warning: created database '{}' without a schema; rows will be rejected unless a schema is applied before loading (see --schema-file)",
                params.database
            );
        }
    }
    if !resuming {
        if let Some(path) = params.schema_file.as_deref() {
            let schema =
                read_to_string(path).unwrap_or_else(|err| fatal(format!("failed to read schema file '{path}': {err}")));
            apply_schema(&driver, &params.database, schema).await;
        }
    }
    driver
}

/// Connects to the cluster using the params and password supplied, exiting with
/// `ConnectionError` on failure. TLS config is derived from `--tls-disabled` / `--tls-root-ca`.
async fn connect(params: &Params, password: &str) -> TypeDBDriver {
    let addresses = parse_addresses(&params.addresses).unwrap_or_else(|err| fatal_with(ExitCode::UserInputError, err));
    let tls_config = build_tls_config(params.tls_disabled, params.tls_root_ca.as_deref().map(Path::new))
        .unwrap_or_else(|err| fatal_with(ExitCode::UserInputError, err));
    TypeDBDriver::new(addresses, Credentials::new(&params.username, password), DriverOptions::new(tls_config))
        .await
        .unwrap_or_else(|err| fatal_with(ExitCode::ConnectionError, format!("failed to connect to TypeDB: {err}")))
}

/// Creates `database` if it does not already exist; returns whether it was created.
/// No-op when it already exists.
async fn create_database_if_missing(driver: &TypeDBDriver, database: &str) -> bool {
    let exists = driver
        .databases()
        .contains(database.to_owned())
        .await
        .unwrap_or_else(|err| fatal(format!("failed to check if database '{database}' exists: {err}")));
    if !exists {
        driver
            .databases()
            .create(database.to_owned())
            .await
            .unwrap_or_else(|err| fatal(format!("failed to create database '{database}': {err}")));
    }
    !exists
}

/// Runs the supplied schema text in a schema transaction and commits it.
async fn apply_schema(driver: &TypeDBDriver, database: &str, schema: String) {
    let schema_tx = driver
        .transaction(database.to_owned(), TransactionType::Schema)
        .await
        .unwrap_or_else(|err| fatal(format!("failed to open schema transaction on '{database}': {err}")));
    schema_tx.query(schema).await.unwrap_or_else(|err| fatal(format!("schema query failed: {err}")));
    schema_tx.commit().await.unwrap_or_else(|err| fatal(format!("failed to commit schema transaction: {err}")));
}
