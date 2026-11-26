# TypeDB Console

## Running TypeDB Console

Go to the directory where you have your `typedb-all` or `typedb-console` distribution unarchived, and run `./typedb console`
```bash
cd <your_typedb_console_dir>/
./typedb console
```

To build and run from Cargo, use:
```bash
cargo run -p typedb-console -- --username=<username> --address=<address>
```

Or to use bazel, use:
```bash
bazel run //console:console-native -- --username=<username> --address=<address>
```

TypeDB console binaries are platform-specific, so cannot be moved across platforms - please use the correct
platform-specific distribution.

## Command line arguments

You can provide several command arguments when running console in the terminal, of which the typically-used ones are:

- `--username=<username>` : TypeDB server username to log in with (mandatory).
- `--address=<address>` : TypeDB server address to which the console will connect to (mandatory)
- `--script=<path>` : Run commands in the script file in non-interactive mode.
- `--command=<command1> --command=<command2> ...` : Run commands in non-interactive mode.
- `-V, --version` : Print version information and exit.
- `-h, --help` : Show help message.

TypeDB Console will by default prompt you for your password in a safe way. If you must,
you are still able to pass in the password with `--password=<password>`.

**By default, TLS encryption is enabled to ensure passwords are not sent over the network in plaintext**

For development or local work, you can disable this with:

`--tls-disabled`

For TypeDB Cloud deployments, there is **no reason to use this setting** as they can only operate with network TLS encryption.

Alternatively, you may securely connect by managing your own certificates for both the client-side and server-side,
and provide your certificate to the console with:
`--tls-root-ca=<path>`

## Console commands

TypeDB Console provides two levels of interaction: server-level commands and transaction-level commands.
To enter the transaction command mode, open a transaction, using a `transaction` command.

Console offers command completion, accessible with a `tab` keypress. Some non-keyword data, such as database and usernames,
will also be autocompleted, while others, such as queries, will not.

### Database-level commands

- `database create <db>` : Create a database with name `<db>` on the server.
  ```
  > database create my-typedb-database
  Successfully created database.
  ```
- `database list` : List the databases on the server.
  ```
  > database list
  my-typedb-database
  ```
- `database delete <db>` : Delete a database with name `<db>` on the server.
  ```
  > database delete my-typedb-database
  Successfully deleted database.
  ```
- `transaction read|write|schema <db>` : Start a `read`, `write`, or `schema` transaction to database `<db>`.
  ```
  > transaction schema my-typedb-database
  my-typedb-database::schema>
  ```
  This will then take you to the transaction-level interface, i.e. the second-level REPL.

**IMPORTANT: For TypeDB Console 2.x users, this command has changed order! It used to be `transaction <db> read|write|schema`**. Now, the `<db>` and `read|write|schema` order is reversed!

- `clear` : Clear the console
- `help` : Print help menu
- `exit` : Exit console

### Transaction-level commands

- `<query>` : Once you're in the transaction REPL, the terminal immediately accepts a multi-line TypeQL query, and will execute it when you **hit enter twice (submit an empty newline) or use an explicit "end;" stage**.
  ```
  my-typedb-database::schema>> define
                              attribute name, value string;
                              entity person, owns name;
  
  Finished schema query.
  >>
  ```
- `source <file>` : Run TypeQL queries in a file (_not a general console script_), which you can refer to using relative or absolute path. Queries must be terminated by an empty newline or an explicit "end;" stage.
  ```
  my-typedb-database::schema> source ./schema.tql
  Successfully executed 1 queries.
  ```
- `commit` : Commit the transaction changes and close transaction. For example:
  ```
  my-typedb-database::schema> commit
  Successfully committed transaction.
  ```
- `rollback` : Will remove any uncommitted changes you've made in the transaction, while leaving transaction open. For example:
  ```
  my-typedb-database::schema> rollback
  Transaction changes rolled back.
  ```
- `close` : Close the transaction without committing changes, and takes you back to the database-level interface, i.e. first-level REPL. For example:
  ```
  my-typedb-database::schema> close
  Transaction closed without committing changes
  ```
- `clear` : Clear the console
- `help` : Print this help menu
- `exit` : Exit console

### Interactive scripts

It's possible to paste console scripts directly into any level of REPL:

```
>> database create test
transaction schema test
    define entity person;
    
    commit
transaction write test
    insert $x isa person;
    
    commit
database create test
transaction read test
match $x isa person;

close
```

This will produce the following output:
```
+ database create test
Successfully created database.
+ transaction schema test
++ define entity person;
Finished schema query.
++ commit
Successfully committed transaction.
+ transaction write test
++ insert $x isa person;
Finished write query validation and compilation...
Finished writes. Streaming rows...
   --------
    $x | isa person, iid 0x1e00000000000000000000
   --------
Finished. Total answers: 1
++ commit
Successfully committed transaction.
+ database create test
Successfully created database.
+ transaction read test
++ match $x isa person;
Finished read query validation and compilation...
Streaming rows...
   --------
    $x | isa person, iid 0x1e00000000000000000000
   --------
Finished. Total answers: 1
++ close
Transaction closed
```

Queries will be parsed maximally and newlines **are** allowed.
If you wish to split a query pipeline into two queries, or skip the usual empty newline after a query,
use the query "end;" marker:
```
>> database create test
transaction schema test
    define 
      entity person;  # newlines are allowed in pasted scripts:
       
      attribute name, value string;
      
      person owns name; 
   
    commit
transaction write test
    insert 
      $x isa person;
      
      $x has name "bob";
    end; # this is required to avoid creating a single "insert-match" query pipeline!
    
    match $x isa person, has name "bob";  # this is treated as a new query
    
    commit
```

### Non-interactive modes

#### Scripts

We can define a script file that contains the list of commands to run, then invoke console with `./typedb console --script=<path>`.

Script files take exactly the same format as scripts pasted directly in the REPL.
Importantly, this means that:
- **Ambiguous query pipelines may need to be split explicitly up with an "end;" marker**.
- **A query is that is not ended with the "end;" marker must still be terminated with an empty newline**

For example, a script `commands.tqls`
```
database create test
transaction schema test
    define entity person;
    
    commit
transaction write test
    insert $x isa person;
    
    commit
transaction read test
    match $x isa person;
    
    close
database delete test
```

Will produce the following output:
```
./typedb console --username="user" --password="password" --script=commands.tqls     
+ database create test
Successfully created database.
+ transaction schema test
++ define entity person;
Finished schema query.
++ commit
Successfully committed transaction.
+ transaction write test
++ insert $x isa person;
Finished write query validation and compilation...
Finished writes. Streaming rows...
   --------
    $x | isa person, iid 0x1e00000000000000000000
   --------
Finished. Total answers: 1
++ commit
Successfully committed transaction.
+ transaction read test
++ match $x isa person;
Finished read query validation and compilation...
Streaming rows...
   --------
    $x | isa person, iid 0x1e00000000000000000000
   --------
Finished. Total answers: 1
++ close
Transaction closed
+ database delete test
Successfully deleted database.
```

The indentation in the script file are only for visual guide and will be ignored by the console.

#### Commands

Instead of producing a script file, you can also specify ordered commands to run directly from the command line:
`./typedb console --command=<command1> --command=<command2> ...`.

These commands will be executed as full commands, one at a time.
