/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use glob::glob;
use typedb_driver::TypeDBDriver;

use crate::{BackgroundRuntime, constants::FORCE_FLAG, repl::command::InputCompleterFn};

pub(crate) fn database_name_completer_fn(
    driver: Arc<TypeDBDriver>,
    runtime: BackgroundRuntime,
) -> Box<InputCompleterFn> {
    // we have to do an annoying hack to let auto-complete function with a live database connection...
    Box::new(move |input| database_name_completer(driver.clone(), runtime.clone(), input))
}

pub(crate) fn database_name_completer(
    driver: Arc<TypeDBDriver>,
    runtime: BackgroundRuntime,
    input: &str,
) -> Vec<String> {
    match runtime.run(async move { driver.databases().all().await }) {
        Ok(dbs) => dbs.iter().map(|db| db.name().to_owned()).filter(|db_name| db_name.starts_with(input)).collect(),
        Err(_) => Vec::with_capacity(0),
    }
}

pub(crate) fn file_completer(input: &str) -> Vec<String> {
    match glob(&format!("{}*", input)) {
        Ok(paths) => paths.filter_map(Result::ok).map(|path| path.to_string_lossy().into_owned()).collect(),
        Err(_) => Vec::new(),
    }
}

pub(crate) fn force_flag_completer(input: &str) -> Vec<String> {
    if FORCE_FLAG.starts_with(input) { vec![FORCE_FLAG.to_owned()] } else { Vec::with_capacity(0) }
}
