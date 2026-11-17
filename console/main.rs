/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    env,
    env::temp_dir,
    error::Error,
    fmt::{Debug, Display, Formatter},
    fs::File,
    io,
    io::BufRead,
    path::{Path, PathBuf},
    process::exit,
    rc::Rc,
    sync::Arc,
};

use clap::Parser;
use home::home_dir;
use rustyline::error::ReadlineError;
use sentry::ClientOptions;
use typedb_cli_common::{build_tls_config, format_argument, parse_address_translation, parse_addresses, println_error};
use typedb_driver::{Credentials, DriverOptions, Transaction, TransactionType, TypeDBDriver};

use crate::{
    cli::{Args, USERNAME_VALUE_NAME},
    completions::{database_name_completer_fn, file_completer},
    constants::DEFAULT_REQUEST_TIMEOUT,
    operations::{
        database_create, database_create_init, database_delete, database_export, database_import, database_list,
        database_schema, server_list, server_primary, server_version, transaction_close, transaction_commit,
        transaction_query, transaction_read, transaction_rollback, transaction_schema, transaction_source,
        transaction_write, user_create, user_delete, user_list, user_update_password,
    },
    repl::{
        Repl, ReplContext,
        command::{CommandInput, CommandLeaf, Subcommand},
        line_reader::LineReaderHidden,
        parser::{get_word, parse_one_query},
    },
    runtime::BackgroundRuntime,
};

mod cli;
mod completions;
mod constants;
mod operations;
mod printer;
mod repl;
mod runtime;

pub const VERSION: &str = include_str!("../VERSION");

const PROMPT: &'static str = ">> ";
const ENTRY_REPL_HISTORY: &'static str = ".typedb_console_repl_history";
const TRANSACTION_REPL_HISTORY: &'static str = "typedb_console_transaction_repl_history";
const DIAGNOSTICS_REPORTING_URI: &'static str =
    "https://7f0ccb67b03abfccbacd7369d1f4ac6b@o4506315929812992.ingest.sentry.io/4506355433537536";

#[derive(Debug, Copy, Clone)]
enum ExitCode {
    Success = 0,
    GeneralError = 1,
    CommandError = 2,
    ConnectionError = 3,
    UserInputError = 4,
    QueryError = 5,
}

fn exit_with_error(err: &(dyn std::error::Error + 'static)) -> ! {
    use crate::repl::command::ReplError;
    if let Some(repl_err) = err.downcast_ref::<ReplError>() {
        println_error!("Error: {}", repl_err);
        exit(ExitCode::UserInputError as i32);
    } else if let Some(io_err) = err.downcast_ref::<io::Error>() {
        println_error!("I/O Error: {}", io_err);
        exit(ExitCode::GeneralError as i32);
    } else if let Some(driver_err) = err.downcast_ref::<typedb_driver::Error>() {
        println_error!("TypeDB Error: {}", driver_err);
        exit(ExitCode::QueryError as i32);
    } else if let Some(command_error) = err.downcast_ref::<CommandError>() {
        println_error!("Command Error: {}", command_error);
        exit(ExitCode::CommandError as i32);
    } else {
        println_error!("Error: {}", err);
        exit(ExitCode::GeneralError as i32);
    }
}

struct ConsoleContext {
    invocation_dir: PathBuf,
    repl_stack: Vec<Rc<Repl<ConsoleContext>>>,
    background_runtime: BackgroundRuntime,
    driver: Arc<TypeDBDriver>,
    transaction: Option<(Transaction, bool)>,
    script_dir: Option<String>,
}

impl ConsoleContext {
    fn convert_path(&self, path: &str) -> PathBuf {
        let path = Path::new(path);
        if !path.is_absolute() {
            match self.script_dir.as_ref() {
                None => self.invocation_dir.join(path),
                Some(dir) => PathBuf::from(dir).join(path),
            }
        } else {
            path.to_path_buf()
        }
    }

    fn has_changes(&self) -> bool {
        self.transaction.as_ref().is_some_and(|(_, has_writes)| *has_writes)
    }
}

impl ReplContext for ConsoleContext {
    fn current_repl(&self) -> &Repl<Self> {
        self.repl_stack.last().unwrap()
    }
}

fn main() {
    let mut args = Args::parse();
    if args.version {
        println!("{}", VERSION);
        exit(ExitCode::Success as i32);
    }
    let addresses = parse_addresses_from_args(&args);
    let username = match args.username {
        Some(username) => username,
        None => {
            println_error!(
                "username is required for connection authentication ('{}').",
                format_argument!("--username <{USERNAME_VALUE_NAME}>")
            );
            exit(ExitCode::UserInputError as i32);
        }
    };
    if args.password.is_none() {
        args.password = Some(LineReaderHidden::new().readline(&format!("password for '{username}': ")));
    }
    if !args.diagnostics_disabled {
        init_diagnostics()
    }
    let tls_root_ca_path = args.tls_root_ca.as_ref().map(|value| Path::new(value));
    let runtime = BackgroundRuntime::new();
    let driver_tls_config = match build_tls_config(args.tls_disabled, tls_root_ca_path) {
        Ok(config) => config,
        Err(err) => {
            println_error!("{err}");
            exit(ExitCode::UserInputError as i32);
        }
    };
    let driver_options = DriverOptions::new(driver_tls_config).request_timeout(DEFAULT_REQUEST_TIMEOUT);
    let driver = match runtime.run(TypeDBDriver::new(
        addresses,
        Credentials::new(&username, args.password.as_ref().unwrap()),
        driver_options,
    )) {
        Ok(driver) => Arc::new(driver),
        Err(err) => {
            let tls_error =
                if args.tls_disabled { "" } else { "\nVerify that the server is also configured with TLS encryption." };
            println_error!("Failed to create driver connection to server. {err}{tls_error}");
            exit(ExitCode::ConnectionError as i32);
        }
    };

    let repl = entry_repl(driver.clone(), runtime.clone());
    let invocation_dir = PathBuf::from(env::current_dir().unwrap());
    let mut context = ConsoleContext {
        invocation_dir,
        repl_stack: vec![Rc::new(repl)],
        background_runtime: runtime,
        driver,
        transaction: None,
        script_dir: None,
    };

    if !args.command.is_empty() && !args.script.is_empty() {
        println_error!("cannot specify both commands and files");
        exit(ExitCode::UserInputError as i32);
    } else if !args.command.is_empty() {
        if let Err(err) = execute_command_list(&mut context, &args.command) {
            exit_with_error(&*err);
        }
    } else if !args.script.is_empty() {
        if let Err(err) = execute_scripts(&mut context, &args.script) {
            exit_with_error(&*err);
        }
    } else {
        execute_interactive(&mut context);
    }
}

fn execute_scripts(context: &mut ConsoleContext, files: &[String]) -> Result<(), Box<dyn Error>> {
    for file_path in files {
        let path = context.convert_path(file_path);
        if let Ok(file) = File::open(&file_path) {
            execute_script(context, path, io::BufReader::new(file).lines())?;
        } else {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Error opening file: {}", path.to_string_lossy()),
            )));
        }
    }
    Ok(())
}

fn execute_script(
    context: &mut ConsoleContext,
    file_path: PathBuf,
    inputs: impl Iterator<Item = Result<String, io::Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut combined_input = String::new();
    context.script_dir = Some(file_path.parent().unwrap().to_string_lossy().to_string());
    for (index, input) in inputs.enumerate() {
        match input {
            Ok(line) => {
                combined_input.push('\n');
                combined_input.push_str(&line);
            }
            Err(_) => {
                println!("### Error reading file '{}' line: {}", file_path.to_string_lossy(), index + 1);
                return Err(Box::new(io::Error::new(io::ErrorKind::Other, "Error reading file")));
            }
        }
    }
    // we could choose to implement this as line-by-line instead of as an interactive-compatible script
    let result = execute_commands(context, &combined_input, true);
    context.script_dir = None;
    result.map_err(|err| Box::new(err) as Box<dyn Error>)
}

fn execute_command_list(context: &mut ConsoleContext, commands: &[String]) -> Result<(), Box<dyn Error>> {
    for command in commands {
        if let Err(err) = execute_commands(context, command, true) {
            println_error!("### Stopped executing at command: {}", command);
            return Err(Box::new(err));
        }
    }
    Ok(())
}

fn execute_interactive(context: &mut ConsoleContext) {
    println!("\nWelcome to TypeDB Console!\n");
    while !context.repl_stack.is_empty() {
        let repl_index = context.repl_stack.len() - 1;
        let current_repl = context.repl_stack[repl_index].clone();
        let (result, interrupt_input_empty) = current_repl.get_input(context.has_changes());
        match result {
            Ok(input) => {
                if !input.trim().is_empty() {
                    let _ = execute_commands(context, &input, false);
                } else {
                    continue;
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                let exit_once = interrupt_input_empty.unwrap_or(true);
                if exit_once && context.repl_stack.len() == repl_index + 1 {
                    // TODO: extra way to eliminate the current repl...
                    //  ideally, every command would signal with a new Repl or to pop one off, so stack manipulation is these control loops
                    let last = context.repl_stack.pop();
                    last.unwrap().finished(context);
                } else if !exit_once {
                    // do nothing
                } else {
                    // this is unexpected... quit
                    exit(ExitCode::GeneralError as i32);
                }
            }
            Err(err) => {
                println!("{}", err);
            }
        }
    }
}

fn execute_commands(context: &mut ConsoleContext, mut input: &str, must_log_command: bool) -> Result<(), CommandError> {
    let mut multiple_commands = None;
    while !context.repl_stack.is_empty() && !input.trim().is_empty() {
        let repl_index = context.repl_stack.len() - 1;
        let current_repl = context.repl_stack[repl_index].clone();

        input = match current_repl.match_first_command(input) {
            Ok(None) => {
                let message = format!("Unrecognised command: {}", input);
                println_error!("{}", message);
                return Err(CommandError { message });
            }
            Ok(Some((command, arguments, next_command_index))) => {
                let command_string = &input[0..next_command_index];
                if multiple_commands.is_none() && !input[next_command_index..].trim().is_empty() {
                    multiple_commands = Some(true);
                }

                if must_log_command || multiple_commands.is_some_and(|b| b) {
                    println!("{} {}", "+".repeat(repl_index + 1), command_string.trim());
                }
                match command.execute(context, arguments) {
                    Ok(_) => &input[next_command_index..],
                    Err(err) => {
                        let message =
                            format!("**Error executing command**\n{}\n--> Error\n{}", command_string.trim(), err);
                        println_error!("{}", message);
                        return Err(CommandError { message });
                    }
                }
            }
            Err(err) => {
                println_error!("{}", err);
                return Err(CommandError { message: err.to_string() });
            }
        };
        input = input.trim_start();
    }
    Ok(())
}

fn entry_repl(driver: Arc<TypeDBDriver>, runtime: BackgroundRuntime) -> Repl<ConsoleContext> {
    let server_commands = Subcommand::new("server")
        .add(CommandLeaf::new_with_input(
            "version",
            "Retrieve server version. Optionally specify a server address to route the request to.",
            CommandInput::new_optional("address", get_word, None),
            server_version,
        ))
        .add(CommandLeaf::new_with_input(
            "list",
            "List servers. Optionally specify a server address to route the request to.",
            CommandInput::new_optional("address", get_word, None),
            server_list,
        ))
        .add(CommandLeaf::new_with_input(
            "primary",
            "Get current primary server. Optionally specify a server address to route the request to.",
            CommandInput::new_optional("address", get_word, None),
            server_primary,
        ));

    let database_commands = Subcommand::new("database")
        .add(CommandLeaf::new("list", "List databases on the server.", database_list))
        .add(CommandLeaf::new_with_input(
            "create",
            "Create a new database with the given name.",
            CommandInput::new_required("db", get_word, None),
            database_create,
        ))
        .add(CommandLeaf::new_with_inputs(
            "create-init",
            "Create a new database with the given name and load schema and data from files. Files may be HTTP-hosted files, or absolute or relative paths. File contents are treated identically to 'transaction source' commands run explicitly, and may contain multiple queries separated by 'end;' markers. File sha256 sums maybe provided.",
            vec![
                CommandInput::new_required("db", get_word, None),
                CommandInput::new_required("schema file", get_word, None),
                CommandInput::new_required("data file", get_word, None),
                CommandInput::new_optional("schema file sha256 (hex or sha256:hex)", get_word, None),
                CommandInput::new_optional("data file sha256 (hex or sha256:hex)", get_word, None),
            ],
            database_create_init,
        ))
        .add(CommandLeaf::new_with_input(
            "delete",
            "Delete the database with the given name.",
            CommandInput::new_required("db", get_word, Some(database_name_completer_fn(driver.clone(), runtime.clone()))),
            database_delete,
        ))
        .add(CommandLeaf::new_with_input(
            "schema",
            "Retrieve the TypeQL representation of a database's schema.",
            CommandInput::new_required("db", get_word, Some(database_name_completer_fn(driver.clone(), runtime.clone()))),
            database_schema,
        ))
        .add(CommandLeaf::new_with_inputs(
            "import",
            "Create a database with the given name based on another previously exported database. File paths must be absolute or relative paths on the local machine.",
            vec![
                CommandInput::new_required("db", get_word, None),
                CommandInput::new_required("schema file path", get_word, None),
                CommandInput::new_required("data file path", get_word, None),
            ],
            database_import,
        ))
        .add(CommandLeaf::new_with_inputs(
            "export",
            "Export a database into a schema definition and a data files.",
            vec![
                CommandInput::new_required(
                    "db",
                    get_word,
                    Some(database_name_completer_fn(driver.clone(), runtime.clone())),
                ),
                CommandInput::new_required("schema file path", get_word, None),
                CommandInput::new_required("data file path", get_word, None),
            ],
            database_export,
        ));

    let user_commands = Subcommand::new("user")
        .add(CommandLeaf::new("list", "List users.", user_list))
        .add(CommandLeaf::new_with_inputs(
            "create",
            "Create new user.",
            vec![
                CommandInput::new_required("name", get_word, None),
                CommandInput::new_hidden("password", get_word, get_word, None),
            ],
            user_create,
        ))
        .add(CommandLeaf::new_with_input(
            "delete",
            "Delete existing user.",
            CommandInput::new_required("name", get_word, None),
            user_delete,
        ))
        .add(CommandLeaf::new_with_inputs(
            "update-password",
            "Set existing user's password.",
            vec![
                CommandInput::new_required("name", get_word, None),
                CommandInput::new_hidden("new password", get_word, get_word, None),
            ],
            user_update_password,
        ));

    let transaction_commands = Subcommand::new("transaction")
        .add(CommandLeaf::new_with_input(
            "read",
            "Open read transaction.",
            CommandInput::new_required(
                "db",
                get_word,
                Some(database_name_completer_fn(driver.clone(), runtime.clone())),
            ),
            transaction_read,
        ))
        .add(CommandLeaf::new_with_input(
            "write",
            "Open write transaction.",
            CommandInput::new_required(
                "db",
                get_word,
                Some(database_name_completer_fn(driver.clone(), runtime.clone())),
            ),
            transaction_write,
        ))
        .add(CommandLeaf::new_with_input(
            "schema",
            "Open schema transaction.",
            CommandInput::new_required(
                "db",
                get_word,
                Some(database_name_completer_fn(driver.clone(), runtime.clone())),
            ),
            transaction_schema,
        ));

    let history_path = home_dir().unwrap_or_else(|| temp_dir()).join(ENTRY_REPL_HISTORY);

    let repl = Repl::new(PROMPT.to_owned(), history_path, false, None)
        .add(server_commands)
        .add(database_commands)
        .add(user_commands)
        .add(transaction_commands);

    repl
}

fn transaction_repl(database: &str, transaction_type: TransactionType) -> Repl<ConsoleContext> {
    let db_prompt = format!("{}::{}{}", database, transaction_type_str(transaction_type), PROMPT);
    let history_path = home_dir().unwrap_or_else(|| temp_dir()).join(TRANSACTION_REPL_HISTORY);
    let repl = Repl::new(db_prompt, history_path, true, Some(on_transaction_repl_finished))
        .add(CommandLeaf::new(
            "commit",
            "Commit the current transaction.",
            transaction_commit,
        ))
        .add(CommandLeaf::new(
            "rollback",
            "Roll back the current transaction to the initial snapshot state.",
            transaction_rollback,
        ))
        .add(CommandLeaf::new(
            "close",
            "Close the current transaction.",
            transaction_close,
        ))
        .add(CommandLeaf::new_with_inputs(
            "source",
            "Synchronously execute a file containing a sequence of TypeQL queries with full validation. Queries can be explicitly separated with 'end;' if required. May be a HTTP-hosted file, an absolute path, or a relative path. Relative paths are relative to the invoking script (if there is one) or else a path relative to the current working directory. A sha256 may be optionally provided.",
            vec![
                CommandInput::new_required("file", get_word, Some(Box::new(file_completer))),
                CommandInput::new_optional("file sha256 (hex or sha256:hex)", get_word, None),
            ],
            transaction_source,
        ))
        // default: no token
        .add(CommandLeaf::new_with_input_multiline(
            "",
            "Execute query string.",
            CommandInput::new_required("query", parse_one_query, None),
            transaction_query,
        ));
    repl
}

fn on_transaction_repl_finished(context: &mut ConsoleContext) {
    context.transaction.take(); // drop
}

fn transaction_type_str(transaction_type: TransactionType) -> &'static str {
    match transaction_type {
        TransactionType::Read => "read",
        TransactionType::Write => "write",
        TransactionType::Schema => "schema",
    }
}

fn parse_addresses_from_args(args: &Args) -> typedb_driver::Addresses {
    let result = if let Some(addresses) = &args.addresses {
        parse_addresses(addresses)
    } else if let Some(translation) = &args.address_translation {
        parse_address_translation(translation)
    } else {
        println_error!(
            "missing server address (at least one of --address, --addresses, or --address-translation must be provided)."
        );
        exit(ExitCode::UserInputError as i32);
    };
    match result {
        Ok(addresses) => addresses,
        Err(err) => {
            println_error!("{err}");
            exit(ExitCode::UserInputError as i32);
        }
    }
}

fn init_diagnostics() {
    let _ = sentry::init((
        DIAGNOSTICS_REPORTING_URI,
        ClientOptions { release: Some(VERSION.into()), ..Default::default() },
    ));
}

struct CommandError {
    message: String,
}

impl Debug for CommandError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for CommandError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for CommandError {}
