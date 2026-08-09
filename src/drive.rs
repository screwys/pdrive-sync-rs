// SPDX-License-Identifier: MIT

use crate::Config;
use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultValue<T> {
    pub ok: bool,
    pub value: Option<T>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteNode {
    pub uid: String,
    pub name: ResultValue<String>,
    #[serde(rename = "type")]
    pub kind: String,
    pub total_storage_size: Option<u64>,
    pub active_revision: Option<RemoteRevision>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteRevision {
    pub uid: String,
    pub storage_size: u64,
    pub claimed_size: u64,
    pub claimed_modification_time: Option<String>,
    pub claimed_digests: RemoteDigests,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDigests {
    pub sha1: String,
    pub sha1_verified: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct RemoteFile {
    pub uid: String,
    pub sha1: String,
    pub claimed_size: u64,
}

#[derive(Clone, Debug)]
pub struct UploadFailure {
    pub name: String,
    pub error: String,
}

#[derive(Clone, Debug, Default)]
pub struct UploadBatchResult {
    pub transferred_items: usize,
    pub skipped_items: usize,
    pub transferred_bytes: u64,
    pub failures: Vec<UploadFailure>,
}

#[derive(Clone, Debug)]
pub struct TrashTarget {
    pub remote_path: String,
    pub uid: String,
}

#[derive(Clone, Debug, Default)]
pub struct TrashBatchResult {
    pub succeeded_uids: Vec<String>,
    pub failed_uids: Vec<String>,
}

pub trait DriveClient {
    fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>>;
    fn info(&mut self, remote_path: &str) -> Result<Option<RemoteNode>>;
    fn create_folder(&mut self, parent_path: &str, name: &str) -> Result<()>;
    fn upload_many(
        &mut self,
        local_paths: &[PathBuf],
        remote_parent: &str,
    ) -> Result<UploadBatchResult>;
    fn download(&mut self, remote_path: &str, local_parent: &Path) -> Result<()>;
    fn trash_many(&mut self, targets: &[TrashTarget]) -> Result<TrashBatchResult>;
    fn release_session(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct CliDrive {
    binary: PathBuf,
    session: Option<ReplSession>,
}

impl CliDrive {
    pub fn new(binary: PathBuf) -> Self {
        Self {
            binary,
            session: None,
        }
    }

    fn session(&mut self) -> Result<&mut ReplSession> {
        if self.session.is_none() {
            self.session = Some(ReplSession::start(&self.binary)?);
        }
        Ok(self.session.as_mut().expect("session was initialized"))
    }

    fn read_json(&mut self, args: &[&str]) -> Result<Vec<u8>> {
        const ATTEMPTS: usize = 3;

        for attempt in 1..=ATTEMPTS {
            let response = if repl_arguments_supported(args) {
                self.session()?.command(args)?
            } else {
                self.one_shot(args)?
            };
            if !response.output.is_empty() {
                return Ok(response.output);
            }
            if attempt < ATTEMPTS && transient_read_failure(&response.error) {
                eprintln!(
                    "[pdrive-sync] Proton Drive read failed transiently; retrying ({attempt}/{ATTEMPTS})"
                );
                thread::sleep(Duration::from_secs(2));
                continue;
            }
            bail!("Proton Drive CLI command failed: {}", response.error);
        }
        unreachable!()
    }

    fn write_json(&mut self, args: &[&str]) -> Result<Vec<u8>> {
        let response = if repl_arguments_supported(args) {
            self.session()?.command(args)?
        } else {
            self.one_shot(args)?
        };
        if response.output.is_empty() {
            bail!("Proton Drive CLI command failed: {}", response.error);
        }
        Ok(response.output)
    }

    fn one_shot(&mut self, args: &[&str]) -> Result<ReplResponse> {
        drop(self.session.take());
        let output = Command::new(&self.binary)
            .args(args)
            .output()
            .with_context(|| format!("failed to run {}", self.binary.display()))?;
        Ok(ReplResponse {
            output: output.stdout,
            error: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }
}

impl DriveClient for CliDrive {
    fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>> {
        let output = self.read_json(&["filesystem", "list", "-j", remote_path])?;
        serde_json::from_slice(&output).context("invalid JSON from Proton Drive list")
    }

    fn info(&mut self, remote_path: &str) -> Result<Option<RemoteNode>> {
        const ATTEMPTS: usize = 3;

        for attempt in 1..=ATTEMPTS {
            let args = ["filesystem", "info", "-j", remote_path];
            let response = if repl_arguments_supported(&args) {
                self.session()?.command(&args)?
            } else {
                self.one_shot(&args)?
            };
            if !response.output.is_empty() {
                return serde_json::from_slice(&response.output)
                    .context("invalid JSON from Proton Drive info")
                    .map(Some);
            }

            let message = response.error;
            if message.starts_with("Node not found:") {
                return Ok(None);
            }
            if attempt < ATTEMPTS && transient_read_failure(&message) {
                eprintln!(
                    "[pdrive-sync] Proton Drive read failed transiently; retrying ({attempt}/{ATTEMPTS})"
                );
                thread::sleep(Duration::from_secs(2));
                continue;
            }
            bail!("Proton Drive CLI command failed: {message}");
        }
        unreachable!()
    }

    fn create_folder(&mut self, parent_path: &str, name: &str) -> Result<()> {
        let output = self.write_json(&["filesystem", "create-folder", "-j", parent_path, name])?;
        serde_json::from_slice::<RemoteNode>(&output)
            .context("invalid JSON from Proton Drive create-folder")?;
        Ok(())
    }

    fn upload_many(
        &mut self,
        local_paths: &[PathBuf],
        remote_parent: &str,
    ) -> Result<UploadBatchResult> {
        if local_paths.is_empty() {
            return Ok(UploadBatchResult::default());
        }
        let mut args = vec![
            "filesystem".to_owned(),
            "upload".to_owned(),
            "-j".to_owned(),
            "--file-conflict-strategy".to_owned(),
            "replace".to_owned(),
            "--skip-thumbnails".to_owned(),
        ];
        let mut expected_names = BTreeSet::new();
        for local_path in local_paths {
            args.push(
                local_path
                    .to_str()
                    .context("local upload path is not valid UTF-8")?
                    .to_owned(),
            );
            let name = local_path
                .file_name()
                .and_then(|name| name.to_str())
                .context("local upload path has no UTF-8 file name")?;
            if !expected_names.insert(name.to_owned()) {
                bail!("upload batch contains duplicate file name {name:?}");
            }
        }
        args.push(remote_parent.to_owned());
        let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
        let output = self.write_json(&arg_refs)?;
        let summary: TransferSummary =
            serde_json::from_slice(&output).context("invalid JSON from Proton Drive upload")?;
        if summary.failed_items != summary.failures.len()
            || summary.transferred_items + summary.skipped_items + summary.failed_items
                != local_paths.len()
        {
            bail!(
                "Proton Drive upload returned inconsistent counts for {} files: transferred={} skipped={} failed={} failure_details={}",
                local_paths.len(),
                summary.transferred_items,
                summary.skipped_items,
                summary.failed_items,
                summary.failures.len()
            );
        }
        let mut failure_names = BTreeSet::new();
        for failure in &summary.failures {
            if !expected_names.contains(&failure.name) {
                bail!(
                    "Proton Drive upload reported failure for unknown file {:?}",
                    failure.name
                );
            }
            if !failure_names.insert(failure.name.clone()) {
                bail!(
                    "Proton Drive upload reported duplicate failure for {:?}",
                    failure.name
                );
            }
        }
        Ok(UploadBatchResult {
            transferred_items: summary.transferred_items,
            skipped_items: summary.skipped_items,
            transferred_bytes: summary.transferred_bytes,
            failures: summary
                .failures
                .into_iter()
                .map(|failure| UploadFailure {
                    name: failure.name,
                    error: failure.error,
                })
                .collect(),
        })
    }

    fn download(&mut self, remote_path: &str, local_parent: &Path) -> Result<()> {
        let local_parent = local_parent
            .to_str()
            .context("local download path is not valid UTF-8")?;
        let output = self.write_json(&[
            "filesystem",
            "download",
            "-j",
            "--file-conflict-strategy",
            "replace",
            remote_path,
            local_parent,
        ])?;
        let summary: TransferSummary =
            serde_json::from_slice(&output).context("invalid JSON from Proton Drive download")?;
        if summary.failed_items != 0 || summary.transferred_items != 1 {
            bail!(
                "Proton Drive download reported transferred={} failed={}",
                summary.transferred_items,
                summary.failed_items
            );
        }
        Ok(())
    }

    fn trash_many(&mut self, targets: &[TrashTarget]) -> Result<TrashBatchResult> {
        if targets.is_empty() {
            return Ok(TrashBatchResult::default());
        }
        let mut args = vec!["filesystem".to_owned(), "trash".to_owned(), "-j".to_owned()];
        let mut expected_uids = BTreeSet::new();
        for target in targets {
            args.push(target.remote_path.clone());
            if !expected_uids.insert(target.uid.clone()) {
                bail!("trash batch contains duplicate node UID");
            }
        }
        let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
        let output = self.write_json(&arg_refs)?;
        let results: Vec<OperationResult> =
            serde_json::from_slice(&output).context("invalid JSON from Proton Drive trash")?;
        if results.len() != targets.len() {
            bail!(
                "Proton Drive trash returned {} results for {} targets",
                results.len(),
                targets.len()
            );
        }
        let mut returned_uids = BTreeSet::new();
        let mut outcome = TrashBatchResult::default();
        for result in results {
            if !expected_uids.contains(&result.uid) {
                bail!("Proton Drive trash returned an unknown node UID");
            }
            if !returned_uids.insert(result.uid.clone()) {
                bail!("Proton Drive trash returned a duplicate node UID");
            }
            if result.ok {
                outcome.succeeded_uids.push(result.uid);
            } else {
                outcome.failed_uids.push(result.uid);
            }
        }
        Ok(outcome)
    }

    fn release_session(&mut self) -> Result<()> {
        drop(self.session.take());
        release_cli_cache_pages();
        Ok(())
    }
}

pub fn optimize_cli_cache(config: &Config) -> Result<usize> {
    if !config.optimize_cli_cache {
        return Ok(0);
    }
    let cache_dir = proton_cli_cache_dir()?;
    optimize_cli_cache_dir(&cache_dir)
}

fn proton_cli_cache_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("PROTON_DRIVE_CACHE_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(path).join("proton-drive-cli"));
    }
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".cache").join("proton-drive-cli"))
}

fn release_cli_cache_pages() {
    #[cfg(target_os = "linux")]
    if let Ok(cache_dir) = proton_cli_cache_dir() {
        for name in [
            "cache-entities.sqlite",
            "cache-entities.sqlite-wal",
            "cache-crypto.sqlite",
            "cache-crypto.sqlite-wal",
        ] {
            let Ok(file) = File::open(cache_dir.join(name)) else {
                continue;
            };
            use std::os::fd::AsRawFd;
            unsafe {
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
}

pub fn optimize_cli_cache_dir(cache_dir: &Path) -> Result<usize> {
    let mut optimized = 0;
    for name in ["cache-entities.sqlite", "cache-crypto.sqlite"] {
        let path = cache_dir.join(name);
        if !path.is_file() {
            continue;
        }
        let connection = Connection::open(&path)
            .with_context(|| format!("failed to open Proton Drive cache {}", path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .with_context(|| format!("failed to enable WAL for {}", path.display()))?;
        if !mode.eq_ignore_ascii_case("wal") {
            bail!(
                "Proton Drive cache {} rejected WAL mode: {mode}",
                path.display()
            );
        }
        optimized += 1;
    }
    Ok(optimized)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferSummary {
    transferred_items: usize,
    #[serde(default)]
    transferred_bytes: u64,
    #[serde(default)]
    skipped_items: usize,
    failed_items: usize,
    #[serde(default)]
    failures: Vec<TransferFailure>,
}

#[derive(Debug, Deserialize)]
struct TransferFailure {
    name: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct OperationResult {
    uid: String,
    ok: bool,
}

struct ReplResponse {
    output: Vec<u8>,
    error: String,
}

struct ReplSession {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    errors: Receiver<String>,
}

impl ReplSession {
    const PROMPT: &'static [u8] = b"proton-drive> ";

    fn start(binary: &Path) -> Result<Self> {
        let mut child = Command::new(binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {} REPL", binary.display()))?;
        let input = child
            .stdin
            .take()
            .context("Proton Drive REPL has no stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("Proton Drive REPL has no stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("Proton Drive REPL has no stderr")?;
        let (error_sender, errors) = mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().map_while(std::result::Result::ok) {
                let _ = error_sender.send(line);
            }
        });

        let mut session = Self {
            child,
            input,
            output: BufReader::new(stdout),
            errors,
        };
        let startup = session.read_to_prompt()?;
        if !startup.is_empty() {
            bail!(
                "unexpected output while starting Proton Drive REPL: {}",
                String::from_utf8_lossy(&startup).trim()
            );
        }
        Ok(session)
    }

    fn command(&mut self, args: &[&str]) -> Result<ReplResponse> {
        while self.errors.try_recv().is_ok() {}
        reject_repl_newlines(args)?;
        let command = args
            .iter()
            .map(|argument| quote_repl_argument(argument))
            .collect::<Vec<_>>()
            .join(" ");
        self.input.write_all(command.as_bytes())?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;

        let output = self.read_to_prompt()?;
        let mut error_lines = Vec::new();
        if output.is_empty()
            && let Ok(line) = self.errors.recv_timeout(Duration::from_millis(100))
        {
            error_lines.push(line);
        }
        error_lines.extend(self.errors.try_iter());
        Ok(ReplResponse {
            output,
            error: error_lines.join("\n"),
        })
    }

    fn read_to_prompt(&mut self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        loop {
            let available = self.output.fill_buf()?;
            if available.is_empty() {
                let status = self.child.try_wait()?;
                let error = self.errors.try_iter().collect::<Vec<_>>().join("\n");
                bail!(
                    "Proton Drive REPL closed unexpectedly{}{}",
                    status.map_or_else(String::new, |value| format!(" with {value}")),
                    if error.is_empty() {
                        String::new()
                    } else {
                        format!(": {error}")
                    }
                );
            }
            let byte = available[0];
            self.output.consume(1);
            bytes.push(byte);

            if bytes == Self::PROMPT {
                bytes.clear();
                return Ok(bytes);
            }
            if bytes.ends_with(Self::PROMPT)
                && bytes.get(bytes.len().saturating_sub(Self::PROMPT.len() + 1)) == Some(&b'\n')
            {
                bytes.truncate(bytes.len() - Self::PROMPT.len() - 1);
                return Ok(bytes);
            }
        }
    }
}

impl Drop for ReplSession {
    fn drop(&mut self) {
        let _ = self.input.write_all(b"exit\n");
        let _ = self.input.flush();
        for _ in 0..20 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(10)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(crate) fn reject_repl_newlines(args: &[&str]) -> Result<()> {
    if !repl_arguments_supported(args) {
        bail!("Proton Drive REPL arguments cannot contain newlines");
    }
    Ok(())
}

fn repl_arguments_supported(args: &[&str]) -> bool {
    !args.iter().any(|argument| argument.contains(['\n', '\r']))
}

pub(crate) fn quote_repl_argument(argument: &str) -> String {
    format!(
        "\"{}\"",
        argument.replace('\\', "\\\\").replace('"', "\\\"")
    )
}

fn transient_read_failure(message: &str) -> bool {
    message.contains("You need to login first")
        || message.contains("SQLITE_BUSY")
        || message.contains("database is locked")
}
