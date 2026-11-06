/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, error::Error, fs::read_to_string, path::PathBuf, process::exit, rc::Rc, sync::Arc};

use futures::stream::StreamExt;
use itertools::Itertools;
use sha2::Digest;
use typedb_driver::{
    AvailableServer, Database, Server, ServerRouting, ServerVersion, TransactionOptions, TransactionType, TypeDBDriver,
    answer::{QueryAnswer, QueryType},
};
use ureq;

use crate::{
    ConsoleContext,
    constants::{DEFAULT_TRANSACTION_TIMEOUT, FORCE_FLAG, FORCE_FLAG_SHORT},
    printer::{print_document, print_row, print_servers_table},
    repl::{
        command::{CommandResult, ReplError},
        line_reader::LineReaderVisible,
        parser::{parse_one_query, parse_server_routing},
    },
    transaction_repl,
};

pub(crate) fn server_version(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let routing = parse_server_routing(input)?;
    let server_version =
        context.background_runtime.run(async move { execute_server_version(&driver, routing).await })?;
    println!("{}", server_version);
    Ok(())
}

pub(crate) fn server_list(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let routing = parse_server_routing(input)?;
    let servers = context.background_runtime.run(async move { execute_servers(&driver, routing).await })?;
    if servers.is_empty() {
        println!("No servers are present.");
    } else {
        print_servers_table(servers);
    }
    Ok(())
}

pub(crate) fn server_primary(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let routing = parse_server_routing(input)?;
    let primary_server =
        context.background_runtime.run(async move { execute_primary_server(&driver, routing).await })?;
    if let Some(primary_server) = primary_server {
        println!("{}", primary_server.address());
    } else {
        println!("No primary server is present.");
    }
    Ok(())
}

pub(crate) fn database_list(context: &mut ConsoleContext, _input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let databases = context.background_runtime.run(async move { execute_databases_all(&driver).await })?;
    if databases.is_empty() {
        println!("No databases are present on the server.");
    } else {
        databases.into_iter().map(|db| db.name().to_string()).sorted().for_each(|name| {
            println!("{name}");
        });
    }
    Ok(())
}

pub(crate) fn database_create(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = input[0].clone();
    context.background_runtime.run(async move { execute_databases_create(&driver, db_name).await })?;
    println!("Successfully created database.");
    Ok(())
}

pub(crate) fn database_create_init(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let db_name = &input[0..1];
    let mut schema_args = vec![input[1].clone()];
    let mut data_args = vec![input[2].clone()];
    schema_args.extend(input.get(3).cloned());
    data_args.extend(input.get(4).cloned());

    database_create(context, db_name)?;
    println!("Successfully created database.");

    println!("Initializing schema...");
    transaction_schema(context, db_name)?;
    transaction_source(context, &schema_args)?;
    transaction_commit(context, &[])?;
    println!("Successfully initialized schema.");

    println!("Loading data... (this can take a while, please wait)");
    transaction_write(context, db_name)?;
    transaction_source(context, &data_args)?;
    transaction_commit(context, &[])?;
    println!("Successfully loaded data. Database is ready.");
    Ok(())
}

pub(crate) fn database_import(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = input[0].clone();
    let schema =
        read_to_string(context.convert_path(&input[1])).map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;
    let data_file_path: PathBuf = context.convert_path(&input[2]);
    context
        .background_runtime
        .run(async move { execute_databases_import(&driver, db_name, schema, data_file_path).await })?;
    println!("Successfully imported database.");
    Ok(())
}

pub(crate) fn database_export(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = input[0].clone();
    let schema_file_path: PathBuf = context.convert_path(&input[1]);
    let data_file_path: PathBuf = context.convert_path(&input[2]);
    context
        .background_runtime
        .run(async move { execute_database_export(&driver, db_name, schema_file_path, data_file_path).await })?;
    println!("Successfully exported database.");
    Ok(())
}

pub(crate) fn database_delete(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = input[0].clone();
    let force = match input.get(1) {
        None => false,
        Some(flag) if flag == FORCE_FLAG || flag == FORCE_FLAG_SHORT => true,
        Some(other) => {
            return Err(Box::new(ReplError {
                message: format!("Unrecognised argument '{}'. Expected '{}'.", other, FORCE_FLAG),
            }));
        }
    };
    if !force && context.can_prompt() && !confirm_database_delete(&db_name)? {
        println!("Cancelled: database '{}' was not deleted.", db_name);
        return Ok(());
    }
    context.background_runtime.run(async move { execute_database_delete(&driver, db_name).await })?;
    println!("Successfully deleted database.");
    Ok(())
}

// irreversible, so require the name to be re-typed rather than a keystroke that is easy to agree to by reflex
fn confirm_database_delete(db_name: &str) -> CommandResult<bool> {
    println!("Deleting database '{}' permanently removes all of its schema and data. This cannot be undone.", db_name);
    let entered = LineReaderVisible::new()
        .readline("  Type the database name to confirm: ")
        .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;
    Ok(entered == db_name)
}

pub(crate) fn database_schema(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = input[0].clone();
    let schema = context.background_runtime.run(async move { execute_database_schema(&driver, db_name).await })?;
    println!("{}", schema);
    Ok(())
}

pub(crate) fn user_list(context: &mut ConsoleContext, _input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let users = context.background_runtime.run(async move { execute_users_all(&driver).await })?;
    if users.is_empty() {
        println!("No users are present.");
    } else {
        users.into_iter().map(|user| user.name().to_string()).sorted().for_each(|name| {
            println!("{name}");
        });
    }
    Ok(())
}

pub(crate) fn user_create(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let username = input[0].clone();
    let password = input[1].clone();
    context.background_runtime.run(async move { execute_users_create(&driver, username, password).await })?;
    println!("Successfully created user.");
    Ok(())
}

pub(crate) fn user_delete(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let username = input[0].clone();
    context.background_runtime.run(async move { execute_user_delete(&driver, username).await })?;
    println!("Successfully deleted user.");
    Ok(())
}

pub(crate) fn user_update_password(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let username = input[0].clone();
    let new_password = input[1].clone();
    let updated_current_user = context
        .background_runtime
        .run(async move { execute_user_update_password(&driver, username, new_password).await })?;
    if updated_current_user {
        println!(
            "Successfully updated current user's password, exiting console. Please log in with the updated credentials."
        );
        exit(0);
    } else {
        println!("Successfully updated user password.");
    }
    Ok(())
}

pub(crate) fn transaction_read(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = &input[0];
    let db_name_owned = db_name.clone();
    let options = default_transaction_options();
    let transaction = context
        .background_runtime
        .run(async move { driver.transaction_with_options(db_name_owned, TransactionType::Read, options).await })
        .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;
    context.transaction = Some((transaction, false));
    let repl = transaction_repl(db_name, TransactionType::Read);
    context.repl_stack.push(Rc::new(repl));
    Ok(())
}

pub(crate) fn transaction_write(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = &input[0];
    let db_name_owned = db_name.clone();
    let transaction = context
        .background_runtime
        .run(async move {
            driver.transaction_with_options(db_name_owned, TransactionType::Write, default_transaction_options()).await
        })
        .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;
    context.transaction = Some((transaction, false));
    let repl = transaction_repl(db_name, TransactionType::Write);
    context.repl_stack.push(Rc::new(repl));
    Ok(())
}

pub(crate) fn transaction_schema(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let driver = context.driver.clone();
    let db_name = &input[0];
    let db_name_owned = db_name.clone();
    let transaction = context
        .background_runtime
        .run(async move {
            driver.transaction_with_options(db_name_owned, TransactionType::Schema, default_transaction_options()).await
        })
        .map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;
    context.transaction = Some((transaction, false));
    let repl = transaction_repl(db_name, TransactionType::Schema);
    context.repl_stack.push(Rc::new(repl));
    Ok(())
}

pub(crate) fn transaction_commit(context: &mut ConsoleContext, _input: &[String]) -> CommandResult {
    let (transaction, _) = context.transaction.take().unwrap();
    match context.background_runtime.run(transaction.commit()) {
        Ok(_) => {
            println!("Successfully committed transaction.");
            context.repl_stack.pop().unwrap().finished(context);
            Ok(())
        }
        Err(err) => {
            context.repl_stack.pop().unwrap().finished(context);
            Err(Box::new(err) as Box<dyn Error + Send>)
        }
    }
}

pub(crate) fn transaction_close(context: &mut ConsoleContext, _input: &[String]) -> CommandResult {
    let (transaction, _) = context.transaction.take().unwrap(); // drop
    let message = match transaction.type_() {
        TransactionType::Read => "Transaction closed",
        TransactionType::Write | TransactionType::Schema => "Transaction closed without committing changes.",
    };
    context.repl_stack.pop().unwrap().finished(context);
    println!("{}", message);
    Ok(())
}

pub(crate) fn transaction_rollback(context: &mut ConsoleContext, _input: &[String]) -> CommandResult {
    let (transaction, _has_writes) = context.transaction.take().unwrap();
    let (transaction, result) = context.background_runtime.run(async move {
        let result = transaction.rollback().await;
        (transaction, result)
    });
    match result {
        Ok(_) => {
            context.transaction = Some((transaction, false));
            println!("Transaction changes rolled back.");
            Ok(())
        }
        Err(err) => {
            // drop transaction, end repl
            context.repl_stack.pop();
            Err(Box::new(err))
        }
    }
}

pub(crate) fn transaction_source(context: &mut ConsoleContext, input: &[String]) -> CommandResult {
    let file_str = &input[0];
    let file_sha256 = input.get(1);
    let resource = FileResource::read(context, file_str)?;
    if let Some(file_sha) = file_sha256 {
        resource.matches_sha256(file_sha)?
    }

    let mut input: &str = &resource.file_content;
    let mut query_count = 0;
    while let Some(next_query_index) = parse_one_query(&input) {
        let query = &input[0..next_query_index];
        if query.trim().is_empty() {
            input = &input[next_query_index..];
            continue;
        }
        match execute_query(context, query.to_owned(), false) {
            Err(err) => {
                return Err(Box::new(ReplError {
                    message: format!(
                        "{}\n### Stopped executing sourced file '{}' at query {}: {}",
                        err.message(),
                        file_str,
                        query_count + 1,
                        query
                    ),
                }) as Box<dyn Error + Send>);
            }
            Ok(_) => {
                input = &input[next_query_index..];
                query_count += 1;
            }
        }
        if query_count % 1_000 == 0 {
            println!("In progress: executed {} queries.", query_count);
        }
    }
    if !input.trim().is_empty() {
        match execute_query(context, input.to_owned(), false) {
            Err(err) => {
                return Err(Box::new(ReplError {
                    message: format!(
                        "{}\n### Stopped executing sourced file '{}' at query {}: {}",
                        err.message(),
                        file_str,
                        query_count + 1,
                        input
                    ),
                }) as Box<dyn Error + Send>);
            }
            Ok(_) => query_count += 1,
        }
    }
    println!("Finished executing {} queries.", query_count);
    Ok(())
}

pub(crate) fn transaction_query(context: &mut ConsoleContext, input: &[impl AsRef<str>]) -> CommandResult {
    let query = input[0].as_ref().to_owned();
    if query.trim().is_empty() {
        return Ok(());
    } else {
        execute_query(context, query, true).map_err(|err| Box::new(err) as Box<dyn Error + Send>)
    }
}

fn default_transaction_options() -> TransactionOptions {
    TransactionOptions::new().transaction_timeout(DEFAULT_TRANSACTION_TIMEOUT)
}

async fn execute_server_version(driver: &TypeDBDriver, routing: ServerRouting) -> CommandResult<ServerVersion> {
    driver.server_version_with_routing(routing).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_servers(driver: &TypeDBDriver, routing: ServerRouting) -> CommandResult<HashSet<Server>> {
    driver.servers_with_routing(routing).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_primary_server(
    driver: &TypeDBDriver,
    routing: ServerRouting,
) -> CommandResult<Option<AvailableServer>> {
    driver.primary_server_with_routing(routing).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_databases_all(driver: &TypeDBDriver) -> CommandResult<Vec<Arc<Database>>> {
    driver.databases().all().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_databases_get(driver: &TypeDBDriver, db_name: String) -> CommandResult<Arc<Database>> {
    driver.databases().get(db_name).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_databases_create(driver: &TypeDBDriver, db_name: String) -> CommandResult {
    driver.databases().create(db_name).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_database_delete(driver: &TypeDBDriver, db_name: String) -> CommandResult {
    let db = execute_databases_get(driver, db_name).await?;
    db.delete().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_database_schema(driver: &TypeDBDriver, db_name: String) -> CommandResult<String> {
    let db = execute_databases_get(driver, db_name).await?;
    db.schema().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_database_type_schema(driver: &TypeDBDriver, db_name: String) -> CommandResult<String> {
    let db = execute_databases_get(driver, db_name).await?;
    db.type_schema().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_database_export(
    driver: &TypeDBDriver,
    db_name: String,
    schema_file_path: PathBuf,
    data_file_path: PathBuf,
) -> CommandResult {
    let db = execute_databases_get(driver, db_name).await?;
    db.export_to_file(schema_file_path, data_file_path).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_databases_import(
    driver: &TypeDBDriver,
    db_name: String,
    schema: String,
    data_file_path: PathBuf,
) -> CommandResult {
    driver
        .databases()
        .import_from_file(db_name, schema, data_file_path)
        .await
        .map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_users_all(driver: &TypeDBDriver) -> CommandResult<Vec<typedb_driver::User>> {
    driver.users().all().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_users_get(driver: &TypeDBDriver, username: String) -> CommandResult<typedb_driver::User> {
    driver.users().get(&username).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)?.ok_or_else(|| {
        Box::new(ReplError { message: format!("User {} not found.", username) }) as Box<dyn Error + Send>
    })
}

async fn execute_users_get_current(driver: &TypeDBDriver) -> CommandResult<typedb_driver::User> {
    driver.users().get_current().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)?.ok_or_else(|| {
        Box::new(ReplError { message: "Could not fetch currently logged in user.".to_string() })
            as Box<dyn Error + Send>
    })
}

async fn execute_users_create(driver: &TypeDBDriver, username: String, password: String) -> CommandResult {
    driver.users().create(username, password).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_user_delete(driver: &TypeDBDriver, username: String) -> CommandResult {
    let user = execute_users_get(driver, username.clone()).await?;
    user.delete().await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)
}

async fn execute_user_update_password(
    driver: &TypeDBDriver,
    username: String,
    password: String,
) -> CommandResult<bool> {
    let user = execute_users_get(driver, username.clone()).await?;
    let current_user = execute_users_get_current(driver).await?;
    user.update_password(password).await.map_err(|err| Box::new(err) as Box<dyn Error + Send>)?;
    Ok(current_user.name() == username)
}

struct FileResource {
    file_source: String,
    file_content: String,
}

impl FileResource {
    fn read(context: &mut ConsoleContext, string: &str) -> Result<Self, Box<dyn Error + Send>> {
        let file_content = if string.starts_with("http://") || string.starts_with("https://") {
            let url = string.to_owned();
            let response = ureq::get(&url).call().map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to fetch file from {}: {}", url, e),
                )) as Box<dyn Error + Send>
            })?;
            response.into_string().map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read file content from {}: {}", url, e),
                )) as Box<dyn Error + Send>
            })?
        } else {
            let path = context.convert_path(&string);
            if !path.exists() {
                return Err(Box::new(ReplError { message: format!("File not found: {}", path.to_string_lossy()) })
                    as Box<dyn Error + Send>);
            } else if path.is_dir() {
                return Err(
                    Box::new(ReplError { message: format!("Path must be a file: {}", path.to_string_lossy()) })
                        as Box<dyn Error + Send>,
                );
            }
            read_to_string(path).map_err(|err| Box::new(err) as Box<dyn Error + Send>)?
        };
        Ok(Self { file_source: string.to_owned(), file_content })
    }

    fn matches_sha256(&self, expected_sha256: &str) -> Result<(), Box<dyn Error + Send>> {
        let expected_sha256 = expected_sha256.to_lowercase();
        let expected_sha256 = expected_sha256.trim_start_matches("sha256:");
        let computed_hash_string = format!("{:x}", sha2::Sha256::digest(&self.file_content)).to_lowercase();
        if expected_sha256 != computed_hash_string {
            Err(Box::new(ReplError {
                message: format!(
                    "Expected '{}' to have sha256 '{expected_sha256}', but calculated '{computed_hash_string}',
                ",
                    &self.file_source
                ),
            }) as Box<dyn Error + Send>)
        } else {
            println!("Successfully verified sha256 checksum of {}", &self.file_source);
            Ok(())
        }
    }
}

const QUERY_TYPE_TEMPLATE: &'static str = "<QUERY TYPE>";
const QUERY_COMPILATION_SUCCESS: &'static str = "Finished <QUERY TYPE> query validation and compilation...";
const QUERY_WRITE_FINISHED_STREAMING_ROWS: &'static str = "Finished writes. Streaming rows...";
const QUERY_WRITE_FINISHED_STREAMING_DOCUMENTS: &'static str = "Finished writes. Streaming rows...";
const QUERY_STREAMING_ROWS: &'static str = "Streaming rows...";
const QUERY_STREAMING_DOCUMENTS: &'static str = "Streaming documents...";
const ANSWER_COUNT_TEMPLATE: &'static str = "<ANSWER COUNT>";
const QUERY_FINISHED_COUNT: &'static str = "Finished. Total answers: <ANSWER COUNT>";

fn query_type_str(query_type: QueryType) -> &'static str {
    match query_type {
        QueryType::ReadQuery => "read",
        QueryType::WriteQuery => "write",
        QueryType::SchemaQuery => "schema",
    }
}

fn execute_query(context: &mut ConsoleContext, query: String, logging: bool) -> Result<(), typedb_driver::Error> {
    let (transaction, has_writes) =
        context.transaction.take().expect("Transaction query run without active transaction.");
    let (transaction, result, write_succes) = context.background_runtime.run(async move {
        let result = transaction.query(query).await;
        if logging {
            // note: print results in the async block so we don't have to collect first
            match result {
                Ok(answer) => {
                    match answer {
                        QueryAnswer::Ok(query_type) => {
                            println!("Finished {} query.", query_type_str(query_type));
                            let write_query = !matches!(query_type, QueryType::ReadQuery);
                            (transaction, Ok(()), write_query)
                        }
                        QueryAnswer::ConceptRowStream(header, mut rows_stream) => {
                            println!(
                                "{}",
                                QUERY_COMPILATION_SUCCESS
                                    .replace(QUERY_TYPE_TEMPLATE, query_type_str(header.query_type))
                            );
                            let write_query = if matches!(header.query_type, QueryType::WriteQuery) {
                                println!("{}", QUERY_WRITE_FINISHED_STREAMING_ROWS);
                                true
                            } else {
                                println!("{}", QUERY_STREAMING_ROWS);
                                false
                            };
                            let has_columns = !header.column_names.is_empty();
                            if !has_columns {
                                println!("\nNo columns to show.\n");
                            }
                            let mut count = 0;
                            while let Some(result) = rows_stream.next().await {
                                match result {
                                    Ok(row) => {
                                        if has_columns {
                                            print_row(row, count == 0);
                                        }
                                        count += 1;
                                    }
                                    Err(err) => return (transaction, Err(err), false),
                                }
                            }
                            println!("{}", QUERY_FINISHED_COUNT.replace(ANSWER_COUNT_TEMPLATE, &count.to_string()));
                            (transaction, Ok(()), write_query)
                        }
                        QueryAnswer::ConceptDocumentStream(header, mut documents_stream) => {
                            println!(
                                "{}",
                                QUERY_COMPILATION_SUCCESS
                                    .replace(QUERY_TYPE_TEMPLATE, query_type_str(header.query_type))
                            );
                            let write_query = if matches!(header.query_type, QueryType::WriteQuery) {
                                println!("{}", QUERY_WRITE_FINISHED_STREAMING_DOCUMENTS);
                                true
                            } else {
                                println!("{}", QUERY_STREAMING_DOCUMENTS);
                                false
                            };

                            let mut count = 0;
                            while let Some(result) = documents_stream.next().await {
                                match result {
                                    Ok(document) => {
                                        print_document(document);
                                        count += 1;
                                    }
                                    // Note: we don't necessarily have to terminate the transaction when we get an error
                                    // but the signalling isn't in place to do this canonically either!
                                    Err(err) => return (transaction, Err(err), false),
                                }
                            }
                            println!("{}", QUERY_FINISHED_COUNT.replace(ANSWER_COUNT_TEMPLATE, &count.to_string()));
                            (transaction, Ok(()), write_query)
                        }
                    }
                }
                Err(err) => (transaction, Err(err), false),
            }
        } else {
            match result {
                Ok(answer) => {
                    let write_query = !matches!(answer.get_query_type(), QueryType::ReadQuery);
                    (transaction, Ok(()), write_query)
                }
                Err(err) => (transaction, Err(err), false),
            }
        }
    });
    if !transaction.is_open() {
        // drop transaction
        // TODO: would be better to return a repl END type. In other places, return repl START(repl)
        context.repl_stack.pop();
    } else {
        context.transaction = Some((transaction, has_writes || write_succes));
    };
    result
}
