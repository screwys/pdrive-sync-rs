// SPDX-License-Identifier: GPL-3.0-or-later

use anyhow::{Context, Result, bail};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;
use std::time::UNIX_EPOCH;

const CHECKPOINT_BATCH_SIZE: usize = 256;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_proton_drive_bin")]
    pub proton_drive_bin: PathBuf,
    #[serde(default = "default_optimize_cli_cache")]
    pub optimize_cli_cache: bool,
    pub state_db: Option<PathBuf>,
    pub success_file: Option<PathBuf>,
    #[serde(rename = "sync")]
    pub syncs: Vec<SyncConfig>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SyncMode {
    #[default]
    Push,
    Pull,
    TwoWay,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeletePolicy {
    #[default]
    Keep,
    Trash,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConflictPolicy {
    #[default]
    Fail,
    LocalWins,
    RemoteWins,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncConfig {
    pub name: String,
    #[serde(default)]
    pub mode: SyncMode,
    pub local: PathBuf,
    pub remote: String,
    #[serde(default)]
    pub ready_marker: Option<PathBuf>,
    #[serde(default)]
    pub delete: DeletePolicy,
    #[serde(default)]
    pub conflict: ConflictPolicy,
    #[serde(default)]
    pub exclude: Vec<String>,
}

fn default_proton_drive_bin() -> PathBuf {
    PathBuf::from("proton-drive")
}

fn default_optimize_cli_cache() -> bool {
    true
}

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
    pub active_revision: Option<ResultValue<RemoteRevision>>,
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
    pub sha1: String,
    pub claimed_size: u64,
}

pub trait DriveClient {
    fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>>;
    fn info(&mut self, remote_path: &str) -> Result<Option<RemoteNode>>;
    fn create_folder(&mut self, parent_path: &str, name: &str) -> Result<()>;
    fn upload(&mut self, local_path: &Path, remote_parent: &str) -> Result<()>;
    fn download(&mut self, remote_path: &str, local_parent: &Path) -> Result<()>;
    fn trash(&mut self, remote_path: &str) -> Result<()>;
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
        let stdout = if output.status.success() {
            output.stdout
        } else {
            Vec::new()
        };
        Ok(ReplResponse {
            output: stdout,
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

    fn upload(&mut self, local_path: &Path, remote_parent: &str) -> Result<()> {
        let local_path = local_path
            .to_str()
            .context("local upload path is not valid UTF-8")?;
        let output = self.write_json(&[
            "filesystem",
            "upload",
            "-j",
            "--file-conflict-strategy",
            "replace",
            "--skip-thumbnails",
            local_path,
            remote_parent,
        ])?;
        let summary: TransferSummary =
            serde_json::from_slice(&output).context("invalid JSON from Proton Drive upload")?;
        if summary.failed_items != 0 || summary.transferred_items != 1 {
            bail!(
                "Proton Drive upload reported transferred={} failed={}",
                summary.transferred_items,
                summary.failed_items
            );
        }
        Ok(())
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

    fn trash(&mut self, remote_path: &str) -> Result<()> {
        let output = self.write_json(&["filesystem", "trash", "-j", remote_path])?;
        let results: Vec<OperationResult> =
            serde_json::from_slice(&output).context("invalid JSON from Proton Drive trash")?;
        if results.len() != 1 || !results[0].ok {
            bail!("Proton Drive trash operation did not succeed");
        }
        Ok(())
    }

    fn release_session(&mut self) -> Result<()> {
        drop(self.session.take());
        release_cli_cache_pages();
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferSummary {
    transferred_items: usize,
    failed_items: usize,
}

#[derive(Debug, Deserialize)]
struct OperationResult {
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

fn reject_repl_newlines(args: &[&str]) -> Result<()> {
    if !repl_arguments_supported(args) {
        bail!("Proton Drive REPL arguments cannot contain newlines");
    }
    Ok(())
}

fn repl_arguments_supported(args: &[&str]) -> bool {
    !args.iter().any(|argument| argument.contains(['\n', '\r']))
}

fn quote_repl_argument(argument: &str) -> String {
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

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SyncSummary {
    pub scanned: usize,
    pub unchanged: usize,
    pub matched_remote: usize,
    pub uploaded: usize,
    pub downloaded: usize,
    pub trashed: usize,
    pub trashed_local: usize,
    pub skipped_symlinks: usize,
}

#[derive(Clone, Debug)]
struct LocalFile {
    relative: String,
    absolute: PathBuf,
    size: u64,
    mtime_ns: i64,
}

#[derive(Clone, Debug)]
struct FileState {
    size: u64,
    mtime_ns: i64,
    sha1: String,
}

#[derive(Default)]
struct RemoteTree {
    files: HashMap<String, RemoteFile>,
    directories: HashSet<String>,
}

pub fn default_state_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path).join("pdrive-sync"));
    }
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home)
        .join(".local")
        .join("state")
        .join("pdrive-sync"))
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

pub fn validate_config(config: &Config) -> Result<()> {
    if config.syncs.is_empty() {
        bail!("configuration has no [[sync]] entries");
    }

    let mut names = HashSet::new();
    for sync in &config.syncs {
        if sync.name.trim().is_empty() {
            bail!("sync name cannot be empty");
        }
        if !names.insert(sync.name.clone()) {
            bail!("duplicate sync name: {}", sync.name);
        }
        if !sync.remote.starts_with('/') || sync.remote == "/" {
            bail!(
                "sync {} remote path must be an absolute non-root path",
                sync.name
            );
        }
        if sync
            .ready_marker
            .as_ref()
            .is_some_and(|marker| marker.is_absolute())
        {
            bail!("sync {} ready_marker must be relative", sync.name);
        }
        build_excludes(sync)?;
    }
    Ok(())
}

pub fn open_database(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let connection =
        Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    connection.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS files (
            mirror TEXT NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            sha1 TEXT NOT NULL,
            PRIMARY KEY (mirror, path)
        );
        CREATE TABLE IF NOT EXISTS remote_directories (
            mirror TEXT NOT NULL,
            path TEXT NOT NULL,
            PRIMARY KEY (mirror, path)
        );
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        ",
    )?;
    Ok(connection)
}

pub fn sync_all(
    config: &Config,
    connection: &Connection,
    drive: &mut dyn DriveClient,
) -> Result<Vec<(String, SyncSummary)>> {
    validate_config(config)?;
    let mut summaries = Vec::new();
    for sync in &config.syncs {
        let summary = match sync.mode {
            SyncMode::Push => sync_push(sync, connection, drive),
            SyncMode::Pull => sync_pull(sync, connection, drive),
            SyncMode::TwoWay => sync_two_way(sync, connection, drive),
        }
        .with_context(|| format!("sync {} failed", sync.name))?;
        summaries.push((sync.name.clone(), summary));
    }
    Ok(summaries)
}

pub fn sync_push(
    mirror: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
) -> Result<SyncSummary> {
    require_ready(mirror)?;

    let excludes = build_excludes(mirror)?;
    let (files, skipped_symlinks) = scan_local_files(mirror, &excludes)?;
    let baseline_key = format!("baseline:{}", mirror.name);
    let baseline_complete = metadata_value(connection, &baseline_key)?.as_deref() == Some("1");
    let mut remote_tree = if baseline_complete {
        None
    } else {
        let mut tree = RemoteTree::default();
        tree.directories.insert(String::new());
        let mut visited = HashSet::new();
        load_remote_tree(drive, &mirror.remote, "", &mut visited, &mut tree)?;
        eprintln!(
            "[pdrive-sync] {}: remote baseline listed {} files in {} directories",
            mirror.name,
            tree.files.len(),
            tree.directories.len()
        );
        replace_remote_directories(connection, &mirror.name, &tree.directories)?;
        drive.release_session()?;
        Some(tree)
    };

    let mut summary = SyncSummary {
        scanned: files.len(),
        skipped_symlinks,
        ..SyncSummary::default()
    };
    let mut seen = HashSet::with_capacity(files.len());
    let mut checkpoints = CheckpointBatch::new(connection);

    for local in files {
        seen.insert(local.relative.clone());
        if let Some(previous) = file_state(connection, &mirror.name, &local.relative)? {
            if previous.size == local.size && previous.mtime_ns == local.mtime_ns {
                summary.unchanged += 1;
                continue;
            }

            let digest = sha1_file(&local.absolute)?;
            if previous.sha1 == digest {
                checkpoints.push(
                    &mirror.name,
                    &local.relative,
                    local.size,
                    local.mtime_ns,
                    &digest,
                )?;
                summary.unchanged += 1;
                continue;
            }

            if remote_matches(
                mirror,
                drive,
                remote_tree.as_ref(),
                &local.relative,
                local.size,
                &digest,
            )? {
                checkpoints.push(
                    &mirror.name,
                    &local.relative,
                    local.size,
                    local.mtime_ns,
                    &digest,
                )?;
                summary.matched_remote += 1;
                continue;
            }

            upload_changed_file(mirror, connection, drive, &local)?;
            checkpoints.push(
                &mirror.name,
                &local.relative,
                local.size,
                local.mtime_ns,
                &digest,
            )?;
            summary.uploaded += 1;
            continue;
        }

        let digest = sha1_file(&local.absolute)?;
        let remote_match = remote_matches(
            mirror,
            drive,
            remote_tree.as_ref(),
            &local.relative,
            local.size,
            &digest,
        )?;

        if remote_match {
            checkpoints.push(
                &mirror.name,
                &local.relative,
                local.size,
                local.mtime_ns,
                &digest,
            )?;
            summary.matched_remote += 1;
        } else {
            upload_changed_file(mirror, connection, drive, &local)?;
            checkpoints.push(
                &mirror.name,
                &local.relative,
                local.size,
                local.mtime_ns,
                &digest,
            )?;
            summary.uploaded += 1;
        }
    }
    checkpoints.flush()?;

    let stale = stale_paths(connection, &mirror.name, &seen)?;
    for path in stale {
        if excludes.is_match(&path) {
            continue;
        }
        if mirror.delete == DeletePolicy::Trash {
            let remote = remote_path(&mirror.remote, &path);
            if drive.info(&remote)?.is_some() {
                drive.trash(&remote)?;
                summary.trashed += 1;
            }
        }
        delete_file_state(connection, &mirror.name, &path)?;
    }

    if mirror.delete == DeletePolicy::Trash
        && let Some(tree) = remote_tree.as_ref()
    {
        for path in tree
            .files
            .keys()
            .filter(|path| !seen.contains(*path) && !excludes.is_match(*path))
        {
            drive.trash(&remote_path(&mirror.remote, path))?;
            summary.trashed += 1;
        }
    }
    set_metadata(connection, &baseline_key, "1")?;
    drop(remote_tree.take());
    Ok(summary)
}

pub fn sync_pull(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
) -> Result<SyncSummary> {
    require_ready(sync)?;
    let excludes = build_excludes(sync)?;
    let (local_files, skipped_symlinks) = scan_local_files(sync, &excludes)?;
    let local_files = local_files
        .into_iter()
        .map(|file| (file.relative.clone(), file))
        .collect::<HashMap<_, _>>();
    let mut tree = RemoteTree::default();
    tree.directories.insert(String::new());
    let mut visited = HashSet::new();
    load_remote_tree(drive, &sync.remote, "", &mut visited, &mut tree)?;
    replace_remote_directories(connection, &sync.name, &tree.directories)?;
    drive.release_session()?;

    let mut summary = SyncSummary {
        scanned: local_files.len(),
        skipped_symlinks,
        ..SyncSummary::default()
    };
    let mut remote_paths = tree
        .files
        .keys()
        .filter(|path| !excludes.is_match(*path))
        .cloned()
        .collect::<Vec<_>>();
    remote_paths.sort();
    let mut checkpoints = CheckpointBatch::new(connection);

    for path in &remote_paths {
        let remote = tree.files.get(path).expect("remote path came from map");
        if let Some(local) = local_files.get(path) {
            let previous = file_state(connection, &sync.name, path)?;
            let matches = if previous.as_ref().is_some_and(|state| {
                state.size == local.size
                    && state.mtime_ns == local.mtime_ns
                    && state.sha1.eq_ignore_ascii_case(&remote.sha1)
                    && state.size == remote.claimed_size
            }) {
                true
            } else {
                let digest = sha1_file(&local.absolute)?;
                digest.eq_ignore_ascii_case(&remote.sha1) && local.size == remote.claimed_size
            };
            if matches {
                checkpoints.push(&sync.name, path, local.size, local.mtime_ns, &remote.sha1)?;
                summary.unchanged += 1;
                continue;
            }
        }

        let local = download_remote_file(sync, drive, path, remote)?;
        checkpoints.push(&sync.name, path, local.size, local.mtime_ns, &remote.sha1)?;
        summary.downloaded += 1;
    }
    checkpoints.flush()?;

    let remote_set = remote_paths.into_iter().collect::<HashSet<_>>();
    let mut local_only = local_files
        .keys()
        .filter(|path| !remote_set.contains(*path))
        .cloned()
        .collect::<Vec<_>>();
    local_only.sort();
    for path in local_only {
        if sync.delete == DeletePolicy::Trash {
            trash::delete(&local_files[&path].absolute)
                .with_context(|| format!("failed to trash local path {path}"))?;
            summary.trashed_local += 1;
        }
        delete_file_state(connection, &sync.name, &path)?;
    }
    Ok(summary)
}

#[derive(Debug, Eq, PartialEq)]
enum TwoWayAction {
    Checkpoint { path: String, sha1: String },
    Upload { path: String, sha1: String },
    Download { path: String },
    TrashRemote { path: String },
    TrashLocal { path: String },
}

struct LocalSnapshot {
    file: LocalFile,
    sha1: String,
}

pub fn sync_two_way(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
) -> Result<SyncSummary> {
    require_ready(sync)?;
    let states = all_file_states(connection, &sync.name)?;
    let excludes = build_excludes(sync)?;
    let (local_files, skipped_symlinks) = scan_local_files(sync, &excludes)?;
    let mut local = HashMap::with_capacity(local_files.len());
    for file in local_files {
        let sha1 = if states
            .get(&file.relative)
            .is_some_and(|state| state.size == file.size && state.mtime_ns == file.mtime_ns)
        {
            states[&file.relative].sha1.clone()
        } else {
            sha1_file(&file.absolute)?
        };
        local.insert(file.relative.clone(), LocalSnapshot { file, sha1 });
    }

    let mut remote = RemoteTree::default();
    remote.directories.insert(String::new());
    let mut visited = HashSet::new();
    load_remote_tree(drive, &sync.remote, "", &mut visited, &mut remote)?;
    replace_remote_directories(connection, &sync.name, &remote.directories)?;
    drive.release_session()?;
    remote.files.retain(|path, _| !excludes.is_match(path));
    let actions = plan_two_way(sync, &local, &remote.files, &states)?;

    let mut summary = SyncSummary {
        scanned: local.len(),
        skipped_symlinks,
        ..SyncSummary::default()
    };
    let mut checkpoints = CheckpointBatch::new(connection);
    for action in actions
        .iter()
        .filter(|action| matches!(action, TwoWayAction::Checkpoint { .. }))
    {
        let TwoWayAction::Checkpoint { path, sha1 } = action else {
            unreachable!()
        };
        let file = &local[path].file;
        checkpoints.push(&sync.name, path, file.size, file.mtime_ns, sha1)?;
        summary.unchanged += 1;
    }

    for action in actions.iter().filter(|action| {
        matches!(
            action,
            TwoWayAction::Upload { .. } | TwoWayAction::Download { .. }
        )
    }) {
        match action {
            TwoWayAction::Upload { path, sha1 } => {
                let file = &local[path].file;
                upload_changed_file(sync, connection, drive, file)?;
                checkpoints.push(&sync.name, path, file.size, file.mtime_ns, sha1)?;
                summary.uploaded += 1;
            }
            TwoWayAction::Download { path } => {
                let remote_file = &remote.files[path];
                let file = download_remote_file(sync, drive, path, remote_file)?;
                checkpoints.push(
                    &sync.name,
                    path,
                    file.size,
                    file.mtime_ns,
                    &remote_file.sha1,
                )?;
                summary.downloaded += 1;
            }
            _ => unreachable!(),
        }
    }
    checkpoints.flush()?;

    for action in actions.iter().filter(|action| {
        matches!(
            action,
            TwoWayAction::TrashRemote { .. } | TwoWayAction::TrashLocal { .. }
        )
    }) {
        match action {
            TwoWayAction::TrashRemote { path } => {
                drive.trash(&remote_path(&sync.remote, path))?;
                delete_file_state(connection, &sync.name, path)?;
                summary.trashed += 1;
            }
            TwoWayAction::TrashLocal { path } => {
                trash::delete(&local[path].file.absolute)
                    .with_context(|| format!("failed to trash local path {path}"))?;
                delete_file_state(connection, &sync.name, path)?;
                summary.trashed_local += 1;
            }
            _ => unreachable!(),
        }
    }
    Ok(summary)
}

fn plan_two_way(
    sync: &SyncConfig,
    local: &HashMap<String, LocalSnapshot>,
    remote: &HashMap<String, RemoteFile>,
    states: &HashMap<String, FileState>,
) -> Result<Vec<TwoWayAction>> {
    let mut paths = local
        .keys()
        .chain(remote.keys())
        .chain(states.keys())
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    paths.sort();
    let mut actions = Vec::new();

    for path in paths {
        let local_file = local.get(&path);
        let remote_file = remote.get(&path);
        let state = states.get(&path);
        let action = match (local_file, remote_file, state) {
            (Some(local), Some(remote), _) if same_content(local, remote) => {
                Some(TwoWayAction::Checkpoint {
                    path,
                    sha1: local.sha1.clone(),
                })
            }
            (Some(local), Some(remote), Some(state)) => {
                let local_changed = !local.sha1.eq_ignore_ascii_case(&state.sha1);
                let remote_changed = !remote.sha1.eq_ignore_ascii_case(&state.sha1);
                match (local_changed, remote_changed) {
                    (true, false) => Some(TwoWayAction::Upload {
                        path,
                        sha1: local.sha1.clone(),
                    }),
                    (false, true) => Some(TwoWayAction::Download { path }),
                    _ => Some(resolve_two_way_conflict(
                        sync,
                        path,
                        Some(local),
                        Some(remote),
                    )?),
                }
            }
            (Some(local), Some(remote), None) => Some(resolve_two_way_conflict(
                sync,
                path,
                Some(local),
                Some(remote),
            )?),
            (Some(local), None, None) => Some(TwoWayAction::Upload {
                path,
                sha1: local.sha1.clone(),
            }),
            (None, Some(_), None) => Some(TwoWayAction::Download { path }),
            (Some(local), None, Some(state)) => {
                if sync.delete == DeletePolicy::Keep {
                    Some(TwoWayAction::Upload {
                        path,
                        sha1: local.sha1.clone(),
                    })
                } else if local.sha1.eq_ignore_ascii_case(&state.sha1) {
                    Some(TwoWayAction::TrashLocal { path })
                } else {
                    Some(resolve_two_way_conflict(sync, path, Some(local), None)?)
                }
            }
            (None, Some(remote), Some(state)) => {
                if sync.delete == DeletePolicy::Keep {
                    Some(TwoWayAction::Download { path })
                } else if remote.sha1.eq_ignore_ascii_case(&state.sha1) {
                    Some(TwoWayAction::TrashRemote { path })
                } else {
                    Some(resolve_two_way_conflict(sync, path, None, Some(remote))?)
                }
            }
            (None, None, Some(_)) => None,
            (None, None, None) => unreachable!(),
        };
        if let Some(action) = action {
            actions.push(action);
        }
    }
    Ok(actions)
}

fn same_content(local: &LocalSnapshot, remote: &RemoteFile) -> bool {
    local.file.size == remote.claimed_size && local.sha1.eq_ignore_ascii_case(&remote.sha1)
}

fn resolve_two_way_conflict(
    sync: &SyncConfig,
    path: String,
    local: Option<&LocalSnapshot>,
    remote: Option<&RemoteFile>,
) -> Result<TwoWayAction> {
    match sync.conflict {
        ConflictPolicy::Fail => bail!("two-way conflict at {path}"),
        ConflictPolicy::LocalWins => match local {
            Some(local) => Ok(TwoWayAction::Upload {
                path,
                sha1: local.sha1.clone(),
            }),
            None if sync.delete == DeletePolicy::Trash => Ok(TwoWayAction::TrashRemote { path }),
            None => Ok(TwoWayAction::Download { path }),
        },
        ConflictPolicy::RemoteWins => match remote {
            Some(_) => Ok(TwoWayAction::Download { path }),
            None if sync.delete == DeletePolicy::Trash => Ok(TwoWayAction::TrashLocal { path }),
            None => {
                let local = local.context("two-way conflict has neither side")?;
                Ok(TwoWayAction::Upload {
                    path,
                    sha1: local.sha1.clone(),
                })
            }
        },
    }
}

fn download_remote_file(
    sync: &SyncConfig,
    drive: &mut dyn DriveClient,
    relative: &str,
    remote: &RemoteFile,
) -> Result<LocalFile> {
    let target = sync.local.join(relative);
    let parent = target
        .parent()
        .context("local download target has no parent")?;
    fs::create_dir_all(parent)?;
    let staging = tempfile::Builder::new()
        .prefix(".pdrive-sync-download-")
        .tempdir_in(parent)?;
    drive.download(&remote_path(&sync.remote, relative), staging.path())?;
    let name = target
        .file_name()
        .context("local download target has no name")?;
    let staged = staging.path().join(name);
    let metadata = fs::metadata(&staged)
        .with_context(|| format!("download did not create {}", staged.display()))?;
    if metadata.len() != remote.claimed_size {
        bail!(
            "downloaded size mismatch for {relative}: expected {}, got {}",
            remote.claimed_size,
            metadata.len()
        );
    }
    let digest = sha1_file(&staged)?;
    if !digest.eq_ignore_ascii_case(&remote.sha1) {
        bail!("downloaded SHA-1 mismatch for {relative}");
    }
    fs::rename(&staged, &target)
        .with_context(|| format!("failed to install downloaded file {relative}"))?;
    local_file(&sync.local, target)
}

fn remote_matches(
    mirror: &SyncConfig,
    drive: &mut dyn DriveClient,
    remote_tree: Option<&RemoteTree>,
    relative: &str,
    size: u64,
    digest: &str,
) -> Result<bool> {
    let remote = if let Some(tree) = remote_tree {
        tree.files.get(relative).cloned()
    } else {
        remote_file(drive.info(&remote_path(&mirror.remote, relative))?)
    };
    Ok(remote
        .is_some_and(|file| file.sha1.eq_ignore_ascii_case(digest) && file.claimed_size == size))
}

fn upload_changed_file(
    mirror: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    local: &LocalFile,
) -> Result<()> {
    let parent = relative_parent(&local.relative);
    ensure_remote_directory(mirror, connection, drive, parent)?;
    drive.upload(&local.absolute, &remote_path(&mirror.remote, parent))
}

fn require_ready(mirror: &SyncConfig) -> Result<()> {
    if !mirror.local.is_dir() {
        bail!("local root is missing: {}", mirror.local.display());
    }
    if let Some(ready_marker) = &mirror.ready_marker {
        let marker = mirror.local.join(ready_marker);
        if !marker.is_file() {
            bail!("readiness marker is missing: {}", marker.display());
        }
    }
    Ok(())
}

fn build_excludes(sync: &SyncConfig) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in &sync.exclude {
        let glob = Glob::new(pattern).with_context(|| {
            format!("sync {} has invalid exclude pattern {pattern:?}", sync.name)
        })?;
        builder.add(glob);
    }
    builder
        .build()
        .with_context(|| format!("failed to build excludes for sync {}", sync.name))
}

fn scan_local_files(mirror: &SyncConfig, excludes: &GlobSet) -> Result<(Vec<LocalFile>, usize)> {
    let mut files = Vec::new();
    let mut skipped_symlinks = 0;
    scan_directory(
        &mirror.local,
        &mirror.local,
        mirror.ready_marker.as_deref(),
        excludes,
        &mut files,
        &mut skipped_symlinks,
    )?;
    files.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok((files, skipped_symlinks))
}

fn scan_directory(
    root: &Path,
    directory: &Path,
    ready_marker: Option<&Path>,
    excludes: &GlobSet,
    files: &mut Vec<LocalFile>,
    skipped_symlinks: &mut usize,
) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .with_context(|| format!("failed to read {}", directory.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        let relative_path = path.strip_prefix(root)?;
        let relative = relative_path
            .to_str()
            .context("local path is not valid UTF-8")?
            .replace(std::path::MAIN_SEPARATOR, "/");
        if excludes.is_match(&relative) {
            continue;
        }
        if file_type.is_symlink() {
            *skipped_symlinks += 1;
            continue;
        }
        if file_type.is_dir() {
            scan_directory(root, &path, ready_marker, excludes, files, skipped_symlinks)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        if ready_marker.is_some_and(|marker| relative_path == marker) {
            continue;
        }
        files.push(local_file(root, path)?);
    }
    Ok(())
}

fn local_file(root: &Path, path: PathBuf) -> Result<LocalFile> {
    let relative = path
        .strip_prefix(root)?
        .to_str()
        .context("local path is not valid UTF-8")?
        .replace(std::path::MAIN_SEPARATOR, "/");
    let metadata = fs::metadata(&path)?;
    let modified = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .context("file modification time is before the Unix epoch")?;
    let mtime_ns = i64::try_from(modified.as_nanos())
        .context("file modification time does not fit in SQLite")?;
    Ok(LocalFile {
        relative,
        absolute: path,
        size: metadata.len(),
        mtime_ns,
    })
}

fn sha1_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        // Hashing is a one-pass scan. Avoid keeping tens of gigabytes of source
        // data charged to the oneshot service's cgroup after it has been read.
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
    }
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Sha1::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    #[cfg(target_os = "linux")]
    let mut advised_offset: libc::off_t = 0;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            // Drop each completed range instead of retaining a multi-gigabyte
            // file in the service cgroup until its hash has finished.
            unsafe {
                libc::posix_fadvise(
                    reader.get_ref().as_raw_fd(),
                    advised_offset,
                    read as libc::off_t,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
            advised_offset += read as libc::off_t;
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn load_remote_tree(
    drive: &mut dyn DriveClient,
    remote_root: &str,
    relative_root: &str,
    visited: &mut HashSet<String>,
    tree: &mut RemoteTree,
) -> Result<()> {
    for node in drive.list(&remote_path(remote_root, relative_root))? {
        if !visited.insert(node.uid.clone()) {
            continue;
        }
        if !node.name.ok {
            continue;
        }
        let Some(name) = node.name.value.as_deref() else {
            continue;
        };
        if name.contains('/') {
            continue;
        }
        let relative = join_relative(relative_root, name);
        match node.kind.as_str() {
            "file" => {
                if let Some(file) = remote_file(Some(node)) {
                    tree.files.insert(relative, file);
                }
            }
            "folder" => {
                tree.directories.insert(relative.clone());
                if tree.directories.len().is_multiple_of(250) {
                    eprintln!(
                        "[pdrive-sync] remote baseline: {} directories listed",
                        tree.directories.len()
                    );
                }
                load_remote_tree(drive, remote_root, &relative, visited, tree)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn remote_file(node: Option<RemoteNode>) -> Option<RemoteFile> {
    let node = node?;
    if node.kind != "file" {
        return None;
    }
    let revision = node.active_revision?.value?;
    Some(RemoteFile {
        sha1: revision.claimed_digests.sha1,
        claimed_size: revision.claimed_size,
    })
}

fn ensure_remote_directory(
    mirror: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    relative: &str,
) -> Result<()> {
    if relative.is_empty() || remote_directory_known(connection, &mirror.name, relative)? {
        return Ok(());
    }

    let parent = relative_parent(relative);
    ensure_remote_directory(mirror, connection, drive, parent)?;
    let remote = remote_path(&mirror.remote, relative);
    match drive.info(&remote)? {
        Some(node) if node.kind == "folder" => {}
        Some(_) => bail!("remote path exists but is not a folder: {remote}"),
        None => {
            let name = relative.rsplit('/').next().unwrap_or(relative);
            drive.create_folder(&remote_path(&mirror.remote, parent), name)?;
        }
    }
    save_remote_directory(connection, &mirror.name, relative)
}

fn remote_path(root: &str, relative: &str) -> String {
    if relative.is_empty() {
        return root.trim_end_matches('/').to_string();
    }
    let escaped = relative
        .split('/')
        .map(escape_remote_segment)
        .collect::<Vec<_>>()
        .join("/");
    format!("{}/{}", root.trim_end_matches('/'), escaped)
}

fn escape_remote_segment(segment: &str) -> String {
    segment.replace('\\', "\\\\").replace('/', "\\/")
}

fn join_relative(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn relative_parent(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

fn file_state(connection: &Connection, mirror: &str, path: &str) -> Result<Option<FileState>> {
    connection
        .query_row(
            "SELECT size, mtime_ns, sha1 FROM files WHERE mirror = ?1 AND path = ?2",
            params![mirror, path],
            |row| {
                Ok(FileState {
                    size: row.get(0)?,
                    mtime_ns: row.get(1)?,
                    sha1: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn all_file_states(connection: &Connection, mirror: &str) -> Result<HashMap<String, FileState>> {
    let mut statement = connection
        .prepare("SELECT path, size, mtime_ns, sha1 FROM files WHERE mirror = ?1 ORDER BY path")?;
    let rows = statement.query_map([mirror], |row| {
        Ok((
            row.get::<_, String>(0)?,
            FileState {
                size: row.get(1)?,
                mtime_ns: row.get(2)?,
                sha1: row.get(3)?,
            },
        ))
    })?;
    let mut states = HashMap::new();
    for row in rows {
        let (path, state) = row?;
        states.insert(path, state);
    }
    Ok(states)
}

fn save_file_state(
    connection: &Connection,
    mirror: &str,
    path: &str,
    size: u64,
    mtime_ns: i64,
    sha1: &str,
) -> Result<()> {
    connection.execute(
        "
        INSERT INTO files (mirror, path, size, mtime_ns, sha1)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT (mirror, path) DO UPDATE SET
            size = excluded.size,
            mtime_ns = excluded.mtime_ns,
            sha1 = excluded.sha1
        ",
        params![mirror, path, size, mtime_ns, sha1],
    )?;
    Ok(())
}

#[derive(Debug)]
struct FileCheckpoint {
    mirror: String,
    path: String,
    size: u64,
    mtime_ns: i64,
    sha1: String,
}

struct CheckpointBatch<'connection> {
    connection: &'connection Connection,
    pending: Vec<FileCheckpoint>,
    #[cfg(test)]
    commits: usize,
}

impl<'connection> CheckpointBatch<'connection> {
    fn new(connection: &'connection Connection) -> Self {
        Self {
            connection,
            pending: Vec::with_capacity(CHECKPOINT_BATCH_SIZE),
            #[cfg(test)]
            commits: 0,
        }
    }

    fn push(
        &mut self,
        mirror: &str,
        path: &str,
        size: u64,
        mtime_ns: i64,
        sha1: &str,
    ) -> Result<()> {
        self.pending.push(FileCheckpoint {
            mirror: mirror.to_owned(),
            path: path.to_owned(),
            size,
            mtime_ns,
            sha1: sha1.to_owned(),
        });
        if self.pending.len() >= CHECKPOINT_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let transaction = self.connection.unchecked_transaction()?;
        for checkpoint in &self.pending {
            save_file_state(
                &transaction,
                &checkpoint.mirror,
                &checkpoint.path,
                checkpoint.size,
                checkpoint.mtime_ns,
                &checkpoint.sha1,
            )?;
        }
        transaction.commit()?;
        self.pending.clear();
        #[cfg(test)]
        {
            self.commits += 1;
        }
        Ok(())
    }
}

fn delete_file_state(connection: &Connection, mirror: &str, path: &str) -> Result<()> {
    connection.execute(
        "DELETE FROM files WHERE mirror = ?1 AND path = ?2",
        params![mirror, path],
    )?;
    Ok(())
}

fn stale_paths(
    connection: &Connection,
    mirror: &str,
    seen: &HashSet<String>,
) -> Result<Vec<String>> {
    let mut statement =
        connection.prepare("SELECT path FROM files WHERE mirror = ?1 ORDER BY path")?;
    let rows = statement.query_map([mirror], |row| row.get::<_, String>(0))?;
    let mut stale = Vec::new();
    for row in rows {
        let path = row?;
        if !seen.contains(&path) {
            stale.push(path);
        }
    }
    Ok(stale)
}

fn metadata_value(connection: &Connection, key: &str) -> Result<Option<String>> {
    connection
        .query_row("SELECT value FROM metadata WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(Into::into)
}

fn set_metadata(connection: &Connection, key: &str, value: &str) -> Result<()> {
    connection.execute(
        "
        INSERT INTO metadata (key, value) VALUES (?1, ?2)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value
        ",
        params![key, value],
    )?;
    Ok(())
}

fn replace_remote_directories(
    connection: &Connection,
    mirror: &str,
    directories: &HashSet<String>,
) -> Result<()> {
    let transaction = connection.unchecked_transaction()?;
    transaction.execute("DELETE FROM remote_directories WHERE mirror = ?1", [mirror])?;
    for path in directories {
        transaction.execute(
            "INSERT INTO remote_directories (mirror, path) VALUES (?1, ?2)",
            params![mirror, path],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn remote_directory_known(connection: &Connection, mirror: &str, path: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT 1 FROM remote_directories WHERE mirror = ?1 AND path = ?2",
            params![mirror, path],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(Into::into)
}

fn save_remote_directory(connection: &Connection, mirror: &str, path: &str) -> Result<()> {
    connection.execute(
        "INSERT OR IGNORE INTO remote_directories (mirror, path) VALUES (?1, ?2)",
        params![mirror, path],
    )?;
    Ok(())
}

pub fn write_success_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let timestamp = chrono::Utc::now()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        .into_bytes();
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&temporary, timestamp)?;
    fs::rename(&temporary, path)?;
    Ok(())
}

pub fn resolved_state_paths(config: &Config) -> Result<(PathBuf, PathBuf)> {
    let state_dir = default_state_dir()?;
    let database = config
        .state_db
        .clone()
        .unwrap_or_else(|| state_dir.join("state.sqlite3"));
    let success = config
        .success_file
        .clone()
        .unwrap_or_else(|| state_dir.join("last-success"));
    Ok((database, success))
}

pub fn load_config(path: &Path) -> Result<Config> {
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let config: Config =
        toml::from_str(&text).with_context(|| format!("invalid TOML in {}", path.display()))?;
    validate_config(&config)?;
    Ok(config)
}

pub fn default_config_path() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("PDRIVE_SYNC_CONFIG") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("pdrive-sync")
        .join("config.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[derive(Default)]
    struct MockDrive {
        files: BTreeMap<String, RemoteFile>,
        contents: BTreeMap<String, Vec<u8>>,
        directories: HashSet<String>,
        uploads: Vec<String>,
        downloads: Vec<String>,
        trashed: Vec<String>,
        fail_upload: bool,
        fail_after_upload: bool,
        released_sessions: usize,
    }

    impl MockDrive {
        fn with_root(root: &str) -> Self {
            Self {
                directories: HashSet::from([root.to_string()]),
                ..Self::default()
            }
        }

        fn file_node(path: &str, file: &RemoteFile) -> RemoteNode {
            RemoteNode {
                uid: format!("uid:{path}"),
                name: ResultValue {
                    ok: true,
                    value: Some(path.rsplit('/').next().unwrap().to_string()),
                },
                kind: "file".to_string(),
                total_storage_size: Some(file.claimed_size),
                active_revision: Some(ResultValue {
                    ok: true,
                    value: Some(RemoteRevision {
                        uid: format!("revision:{path}"),
                        storage_size: file.claimed_size,
                        claimed_size: file.claimed_size,
                        claimed_modification_time: None,
                        claimed_digests: RemoteDigests {
                            sha1: file.sha1.clone(),
                            sha1_verified: Some(true),
                        },
                    }),
                }),
            }
        }

        fn folder_node(path: &str) -> RemoteNode {
            RemoteNode {
                uid: format!("uid:{path}"),
                name: ResultValue {
                    ok: true,
                    value: Some(path.rsplit('/').next().unwrap().to_string()),
                },
                kind: "folder".to_string(),
                total_storage_size: None,
                active_revision: None,
            }
        }

        fn insert_file(&mut self, path: String, content: &[u8]) {
            let mut hasher = Sha1::new();
            hasher.update(content);
            self.files.insert(
                path.clone(),
                RemoteFile {
                    sha1: format!("{:x}", hasher.finalize()),
                    claimed_size: content.len() as u64,
                },
            );
            self.contents.insert(path, content.to_vec());
        }
    }

    impl DriveClient for MockDrive {
        fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>> {
            let prefix = format!("{}/", remote_path.trim_end_matches('/'));
            let mut nodes = Vec::new();
            for directory in self.directories.clone() {
                if directory != remote_path
                    && directory.starts_with(&prefix)
                    && !directory[prefix.len()..].contains('/')
                {
                    nodes.push(Self::folder_node(&directory));
                }
            }
            for (path, file) in &self.files {
                if path.starts_with(&prefix) && !path[prefix.len()..].contains('/') {
                    nodes.push(Self::file_node(path, file));
                }
            }
            Ok(nodes)
        }

        fn info(&mut self, remote_path: &str) -> Result<Option<RemoteNode>> {
            if let Some(file) = self.files.get(remote_path) {
                return Ok(Some(Self::file_node(remote_path, file)));
            }
            if self.directories.contains(remote_path) {
                return Ok(Some(Self::folder_node(remote_path)));
            }
            Ok(None)
        }

        fn create_folder(&mut self, parent_path: &str, name: &str) -> Result<()> {
            self.directories
                .insert(format!("{}/{}", parent_path.trim_end_matches('/'), name));
            Ok(())
        }

        fn upload(&mut self, local_path: &Path, remote_parent: &str) -> Result<()> {
            if self.fail_upload {
                bail!("simulated upload failure");
            }
            let name = local_path.file_name().unwrap().to_string_lossy();
            let path = format!("{}/{}", remote_parent.trim_end_matches('/'), name);
            let metadata = fs::metadata(local_path)?;
            self.files.insert(
                path.clone(),
                RemoteFile {
                    sha1: sha1_file(local_path)?,
                    claimed_size: metadata.len(),
                },
            );
            self.contents.insert(path.clone(), fs::read(local_path)?);
            self.uploads.push(path);
            if self.fail_after_upload {
                bail!("simulated failure after accepted upload");
            }
            Ok(())
        }

        fn download(&mut self, remote_path: &str, local_parent: &Path) -> Result<()> {
            let content = self
                .contents
                .get(remote_path)
                .context("mock remote content is missing")?;
            let name = remote_path
                .rsplit('/')
                .next()
                .context("mock path has no name")?;
            fs::write(local_parent.join(name), content)?;
            self.downloads.push(remote_path.to_string());
            Ok(())
        }

        fn trash(&mut self, remote_path: &str) -> Result<()> {
            self.files.remove(remote_path);
            self.contents.remove(remote_path);
            self.trashed.push(remote_path.to_string());
            Ok(())
        }

        fn release_session(&mut self) -> Result<()> {
            self.released_sessions += 1;
            Ok(())
        }
    }

    struct Fixture {
        _temp: TempDir,
        local: PathBuf,
        connection: Connection,
        mirror: SyncConfig,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = TempDir::new().unwrap();
            let local = temp.path().join("stuff");
            fs::create_dir(&local).unwrap();
            fs::write(local.join(".ready"), "").unwrap();
            let connection = open_database(&temp.path().join("state.sqlite3")).unwrap();
            let mirror = SyncConfig {
                name: "stuff".to_string(),
                mode: SyncMode::Push,
                local: local.clone(),
                remote: "/my-files/Desktop/stuff".to_string(),
                ready_marker: Some(PathBuf::from(".ready")),
                delete: DeletePolicy::Keep,
                conflict: ConflictPolicy::Fail,
                exclude: Vec::new(),
            };
            Self {
                _temp: temp,
                local,
                connection,
                mirror,
            }
        }

        fn write(&self, relative: &str, content: &str) -> PathBuf {
            let path = self.local.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&path, content).unwrap();
            path
        }
    }

    #[test]
    fn matching_remote_file_is_checkpointed_without_upload() {
        let fixture = Fixture::new();
        let path = fixture.write("already-there.txt", "same content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive.files.insert(
            format!("{}/already-there.txt", fixture.mirror.remote),
            RemoteFile {
                sha1: sha1_file(&path).unwrap(),
                claimed_size: fs::metadata(path).unwrap().len(),
            },
        );

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.matched_remote, 1);
        assert_eq!(summary.uploaded, 0);
        assert!(drive.uploads.is_empty());
        assert_eq!(drive.released_sessions, 1);
    }

    #[test]
    fn unchanged_file_uploads_only_once() {
        let fixture = Fixture::new();
        fixture.write("new.txt", "content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);

        let first = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        let second = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(first.uploaded, 1);
        assert_eq!(second.unchanged, 1);
        assert_eq!(drive.uploads.len(), 1);
    }

    #[test]
    fn changed_metadata_with_same_digest_does_not_upload() {
        let fixture = Fixture::new();
        let path = fixture.write("touched.txt", "same");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        let before = drive.uploads.len();

        let original = fs::read(&path).unwrap();
        fs::write(&path, original).unwrap();
        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.unchanged, 1);
        assert_eq!(drive.uploads.len(), before);
    }

    #[test]
    fn changed_content_uploads_one_new_revision() {
        let fixture = Fixture::new();
        let path = fixture.write("changed.txt", "before");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        fs::write(path, "after with different size").unwrap();
        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.uploaded, 1);
        assert_eq!(drive.uploads.len(), 2);
    }

    #[test]
    fn local_deletion_moves_managed_remote_file_to_trash() {
        let mut fixture = Fixture::new();
        fixture.mirror.delete = DeletePolicy::Trash;
        let path = fixture.write("remove.txt", "content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        fs::remove_file(path).unwrap();

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.trashed, 1);
        assert_eq!(
            drive.trashed,
            vec![format!("{}/remove.txt", fixture.mirror.remote)]
        );
    }

    #[test]
    fn unknown_remote_file_is_not_trashed() {
        let fixture = Fixture::new();
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive.files.insert(
            format!("{}/remote-only.txt", fixture.mirror.remote),
            RemoteFile {
                sha1: "unknown".to_string(),
                claimed_size: 7,
            },
        );

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.trashed, 0);
        assert!(drive.trashed.is_empty());
    }

    #[test]
    fn exact_mirror_trashes_untracked_remote_file_during_baseline() {
        let mut fixture = Fixture::new();
        fixture.mirror.delete = DeletePolicy::Trash;
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive.files.insert(
            format!("{}/remote-only.txt", fixture.mirror.remote),
            RemoteFile {
                sha1: "unknown".to_string(),
                claimed_size: 7,
            },
        );

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.trashed, 1);
        assert_eq!(
            drive.trashed,
            vec![format!("{}/remote-only.txt", fixture.mirror.remote)]
        );
    }

    #[test]
    fn failed_upload_is_not_checkpointed() {
        let fixture = Fixture::new();
        fixture.write("retry.txt", "content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive.fail_upload = true;

        assert!(sync_push(&fixture.mirror, &fixture.connection, &mut drive).is_err());
        assert!(
            file_state(&fixture.connection, &fixture.mirror.name, "retry.txt")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn accepted_upload_with_failed_command_is_not_uploaded_again() {
        let fixture = Fixture::new();
        let path = fixture.write("ambiguous.txt", "before");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        fs::write(path, "after with different size").unwrap();
        drive.fail_after_upload = true;
        assert!(sync_push(&fixture.mirror, &fixture.connection, &mut drive).is_err());
        assert_eq!(drive.uploads.len(), 2);

        drive.fail_after_upload = false;
        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.matched_remote, 1);
        assert_eq!(summary.uploaded, 0);
        assert_eq!(drive.uploads.len(), 2);
    }

    #[test]
    fn pull_downloads_remote_change_then_uses_checkpoint() {
        let mut fixture = Fixture::new();
        fixture.mirror.mode = SyncMode::Pull;
        let path = fixture.write("pulled.txt", "local old");
        let remote_path = format!("{}/pulled.txt", fixture.mirror.remote);
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive.insert_file(remote_path.clone(), b"remote current");

        let first = sync_pull(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        let second = sync_pull(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(fs::read_to_string(path).unwrap(), "remote current");
        assert_eq!(first.downloaded, 1);
        assert_eq!(second.unchanged, 1);
        assert_eq!(drive.downloads, vec![remote_path]);
    }

    #[test]
    fn two_way_transfers_only_the_side_changed_since_checkpoint() {
        let mut fixture = Fixture::new();
        fixture.mirror.mode = SyncMode::TwoWay;
        let path = fixture.write("shared.txt", "initial");
        let remote_path = format!("{}/shared.txt", fixture.mirror.remote);
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        drive.insert_file(remote_path.clone(), b"remote edit");
        let pulled = sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        assert_eq!(pulled.downloaded, 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), "remote edit");

        fs::write(&path, "local edit").unwrap();
        let pushed = sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        assert_eq!(pushed.uploaded, 1);
        assert_eq!(drive.contents[&remote_path], b"local edit");
    }

    #[test]
    fn two_way_detects_conflict_before_transfer() {
        let mut fixture = Fixture::new();
        fixture.mirror.mode = SyncMode::TwoWay;
        let path = fixture.write("conflict.txt", "initial");
        let remote_path = format!("{}/conflict.txt", fixture.mirror.remote);
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        let uploads_before = drive.uploads.len();

        fs::write(path, "local edit").unwrap();
        drive.insert_file(remote_path, b"remote edit");
        let error = sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap_err();

        assert!(error.to_string().contains("two-way conflict"));
        assert_eq!(drive.uploads.len(), uploads_before);
        assert!(drive.downloads.is_empty());
    }

    #[test]
    fn two_way_local_deletion_moves_remote_to_trash() {
        let mut fixture = Fixture::new();
        fixture.mirror.mode = SyncMode::TwoWay;
        fixture.mirror.delete = DeletePolicy::Trash;
        let path = fixture.write("deleted-locally.txt", "initial");
        let remote_path = format!("{}/deleted-locally.txt", fixture.mirror.remote);
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        fs::remove_file(path).unwrap();

        let summary = sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.trashed, 1);
        assert_eq!(drive.trashed, vec![remote_path]);
    }

    #[test]
    fn two_way_remote_deletion_plans_local_trash() {
        let mut fixture = Fixture::new();
        fixture.mirror.mode = SyncMode::TwoWay;
        fixture.mirror.delete = DeletePolicy::Trash;
        let path = fixture.write("deleted-remotely.txt", "initial");
        let remote_path = format!("{}/deleted-remotely.txt", fixture.mirror.remote);
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        drive.files.remove(&remote_path);
        drive.contents.remove(&remote_path);

        let state = all_file_states(&fixture.connection, &fixture.mirror.name).unwrap();
        let file = local_file(&fixture.local, path).unwrap();
        let digest = sha1_file(&file.absolute).unwrap();
        let local = HashMap::from([(file.relative.clone(), LocalSnapshot { file, sha1: digest })]);
        let actions = plan_two_way(&fixture.mirror, &local, &HashMap::new(), &state).unwrap();

        assert_eq!(
            actions,
            vec![TwoWayAction::TrashLocal {
                path: "deleted-remotely.txt".to_string()
            }]
        );
    }

    #[test]
    fn missing_ready_marker_blocks_sync_before_remote_changes() {
        let fixture = Fixture::new();
        fs::remove_file(fixture.local.join(".ready")).unwrap();
        fixture.write("local.txt", "content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);

        assert!(sync_push(&fixture.mirror, &fixture.connection, &mut drive).is_err());
        assert!(drive.uploads.is_empty());
        assert!(drive.trashed.is_empty());
    }

    #[test]
    fn excluded_paths_are_not_read_or_trashed() {
        let mut fixture = Fixture::new();
        fixture.mirror.delete = DeletePolicy::Trash;
        fixture.mirror.exclude = vec!["private/**".to_owned()];
        fixture.write("private/secret.img", "secret");
        let remote_path = format!("{}/private/secret.img", fixture.mirror.remote);
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive
            .directories
            .insert(format!("{}/private", fixture.mirror.remote));
        drive.insert_file(remote_path.clone(), b"remote secret");

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.scanned, 0);
        assert!(!drive.trashed.contains(&remote_path));
    }

    #[test]
    fn invalid_exclude_pattern_is_rejected() {
        let mut fixture = Fixture::new();
        fixture.mirror.exclude = vec!["[".to_owned()];
        let config = Config {
            proton_drive_bin: PathBuf::from("proton-drive"),
            optimize_cli_cache: true,
            state_db: None,
            success_file: None,
            syncs: vec![fixture.mirror],
        };

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn checkpoints_use_bounded_transactions() {
        let connection = open_database(Path::new(":memory:")).unwrap();
        let mut checkpoints = CheckpointBatch::new(&connection);

        for index in 0..(CHECKPOINT_BATCH_SIZE * 2 + 1) {
            checkpoints
                .push(
                    "stuff",
                    &format!("file-{index}"),
                    index as u64,
                    index as i64,
                    "digest",
                )
                .unwrap();
        }
        checkpoints.flush().unwrap();

        let count: usize = connection
            .query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, CHECKPOINT_BATCH_SIZE * 2 + 1);
        assert_eq!(checkpoints.commits, 3);
    }

    #[test]
    fn example_config_names_every_sync_operation() {
        let config: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        validate_config(&config).unwrap();

        assert_eq!(
            config
                .syncs
                .iter()
                .map(|sync| sync.mode)
                .collect::<Vec<_>>(),
            vec![SyncMode::Push, SyncMode::Pull, SyncMode::TwoWay]
        );
        assert_eq!(config.syncs[0].delete, DeletePolicy::Keep);
        assert_eq!(config.syncs[1].delete, DeletePolicy::Trash);
        assert_eq!(config.syncs[2].conflict, ConflictPolicy::Fail);
    }

    #[cfg(unix)]
    #[test]
    fn cli_drive_reuses_one_repl_process() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let script = temp.path().join("fake proton drive");
        fs::write(
            &script,
            r#"#!/usr/bin/env bash
count=0
printf 'proton-drive> '
while IFS= read -r command; do
    if [ "$command" = exit ]; then
        exit 0
    fi
    count=$((count + 1))
    if [ "$count" -eq 1 ]; then
        printf '[]\n'
    else
        printf '[{"uid":"same-session","name":{"ok":true,"value":"folder"},"type":"folder"}]\n'
    fi
    printf 'proton-drive> '
done
"#,
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let mut drive = CliDrive::new(script);

        assert!(drive.list("/my-files/one").unwrap().is_empty());
        let second = drive.list("/my-files/two").unwrap();

        assert_eq!(second.len(), 1);
        assert_eq!(second[0].uid, "same-session");
    }

    #[cfg(unix)]
    #[test]
    fn cli_drive_uses_one_shot_for_newline_arguments() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let script = temp.path().join("fake-proton-drive");
        fs::write(
            &script,
            r#"#!/usr/bin/env bash
if [[ "$*" != *$'\n'* ]]; then
    printf 'newline argument was not preserved\n' >&2
    exit 2
fi
printf '{"transferredItems":1,"failedItems":0}\n'
"#,
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let local = temp.path().join("line\nbreak.txt");
        fs::write(&local, "content").unwrap();
        let mut drive = CliDrive::new(script);

        drive.upload(&local, "/my-files/target").unwrap();
    }

    #[test]
    fn repl_arguments_are_quoted_without_shell_interpolation() {
        assert_eq!(
            quote_repl_argument("space \" quote \\ slash $HOME"),
            "\"space \\\" quote \\\\ slash $HOME\""
        );
        assert!(reject_repl_newlines(&["line\nbreak"]).is_err());
    }

    #[test]
    fn proton_cli_caches_are_switched_to_wal() {
        let temp = TempDir::new().unwrap();
        for name in ["cache-entities.sqlite", "cache-crypto.sqlite"] {
            let connection = Connection::open(temp.path().join(name)).unwrap();
            connection
                .execute("CREATE TABLE entities (key TEXT PRIMARY KEY)", [])
                .unwrap();
        }

        assert_eq!(optimize_cli_cache_dir(temp.path()).unwrap(), 2);

        for name in ["cache-entities.sqlite", "cache-crypto.sqlite"] {
            let connection = Connection::open(temp.path().join(name)).unwrap();
            let mode: String = connection
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            assert_eq!(mode, "wal");
        }
    }
}
