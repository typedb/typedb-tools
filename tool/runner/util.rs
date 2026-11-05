/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{env, error::Error, fs, path::PathBuf, process::Command};

use tempdir::TempDir;

const TAR_GZ: &str = ".tar.gz";
const ZIP: &str = ".zip";

pub fn get_archive_file(archive_env_var: &str) -> Result<PathBuf, Box<dyn Error>> {
    let archive_file: String = env::var(archive_env_var)?;
    Ok(PathBuf::from(archive_file))
}

pub fn unarchive(archive: PathBuf) -> Result<(TempDir, PathBuf), Box<dyn Error>> {
    let runner_dir = TempDir::new("unarchived")?;
    let current_dir = env::current_dir()?;

    if !archive.is_file() {
        return Err(format!("No distribution archive at {:?}", std::path::absolute(&archive)?).into());
    }
    let destination = runner_dir.path().to_str().unwrap();
    let mut command = match archive.to_str() {
        Some(path) if path.ends_with(TAR_GZ) => {
            let mut command = Command::new("tar");
            command.args(["-xf", path, "-C", destination]);
            command
        }
        // Windows has no unzip, and its own tar reads zip files where the GNU tar a PATH may put first does not.
        Some(path) if path.ends_with(ZIP) && cfg!(windows) => {
            let mut command = Command::new(PathBuf::from(env::var("SystemRoot")?).join("System32").join("tar.exe"));
            command.args(["-xf", path, "-C", destination]);
            command
        }
        Some(path) if path.ends_with(ZIP) => {
            let mut command = Command::new("unzip");
            command.args(["-q", path, "-d", destination]);
            command
        }
        _ => return Err(format!("The distribution archive format must be either {} or {}", TAR_GZ, ZIP).into()),
    };
    let output = command.current_dir(&current_dir).output()?;
    if !output.status.success() {
        return Err(format!("Failed to extract {:?}: {}", archive, String::from_utf8_lossy(&output.stderr)).into());
    }

    // Get the first (and only) directory entry
    let extracted_dir = fs::read_dir(runner_dir.path())?.next().ok_or("No files found in extracted directory")??.path();

    Ok((runner_dir, extracted_dir))
}
