/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    io,
    path::PathBuf,
    process::{Child, Command},
};

use tempdir::TempDir;

use crate::util::{get_archive_file, unarchive};

pub struct TypeDBBinaryRunner {
    temp_dir: TempDir, // retain to prevent deletion of the directory
    distribution: PathBuf,
    subcommand: String,
}

impl TypeDBBinaryRunner {
    pub fn new(archive_env_var: &str, typedb_subcommand: &str) -> io::Result<Self> {
        println!("Constructing runner for {}", typedb_subcommand);
        println!("Extracting {} distribution archive.", typedb_subcommand);

        let (temp_dir, distribution) = unarchive(get_archive_file(archive_env_var).unwrap()).unwrap();
        println!("{} distribution extracted, at: {:?}", typedb_subcommand, distribution);

        println!("{} runner constructed", typedb_subcommand);

        Ok(TypeDBBinaryRunner { temp_dir, distribution, subcommand: typedb_subcommand.to_owned() })
    }

    pub fn run(&self, options: &[impl AsRef<str>]) -> io::Result<Child> {
        let mut command = self.typedb_command();
        command.arg(&self.subcommand).args(options.iter().map(AsRef::as_ref));
        command.stdout(std::process::Stdio::inherit()).stderr(std::process::Stdio::inherit()).spawn()
    }

    // Windows runs the distribution's batch file through cmd.exe.
    fn typedb_command(&self) -> Command {
        if cfg!(windows) {
            let mut command = Command::new("cmd.exe");
            command.arg("/c").arg(self.distribution.join("typedb.bat"));
            command
        } else {
            Command::new(self.distribution.join("typedb"))
        }
    }

    pub fn distribution_path(&self) -> &PathBuf {
        &self.distribution
    }
}
