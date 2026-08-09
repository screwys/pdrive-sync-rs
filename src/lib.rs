// SPDX-License-Identifier: MIT

mod drive;
mod state;

pub use drive::{
    CliDrive, DriveClient, RemoteDigests, RemoteFile, RemoteNode, RemoteRevision, ResultValue,
    TrashBatchResult, TrashTarget, UploadBatchResult, UploadFailure, optimize_cli_cache,
    optimize_cli_cache_dir,
};
#[cfg(test)]
use drive::{quote_repl_argument, reject_repl_newlines};
#[cfg(test)]
use state::CHECKPOINT_BATCH_SIZE;
use state::{
    CheckpointBatch, FileState, all_file_states, delete_file_state, file_state, metadata_value,
    remote_directory_known, replace_remote_directories, save_remote_directory, set_metadata,
    stale_paths,
};
pub use state::{default_state_dir, open_database, write_success_file};

use anyhow::{Context, Result, bail};
use globset::{Glob, GlobSet, GlobSetBuilder};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const UPLOAD_BATCH_SIZE: usize = 32;
const TRASH_BATCH_SIZE: usize = 64;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_proton_drive_bin")]
    pub proton_drive_bin: PathBuf,
    #[serde(default = "default_optimize_cli_cache")]
    pub optimize_cli_cache: bool,
    #[serde(default = "default_notifications")]
    pub notifications: bool,
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

fn default_notifications() -> bool {
    true
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
struct PendingUpload {
    file: LocalFile,
    checkpoint_sha1: Option<String>,
}

#[derive(Default)]
struct RemoteTree {
    files: HashMap<String, RemoteFile>,
    directories: HashSet<String>,
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
    let states = all_file_states(connection, &mirror.name)?;
    let baseline_key = format!("baseline:{}", mirror.name);
    let baseline_complete = metadata_value(connection, &baseline_key)?.as_deref() == Some("1");
    let mut remote_tree = if !baseline_complete && mirror.delete == DeletePolicy::Trash {
        Some(inventory_remote(
            mirror,
            connection,
            drive,
            Some("baseline"),
        )?)
    } else {
        None
    };

    let mut summary = SyncSummary {
        scanned: files.len(),
        skipped_symlinks,
        ..SyncSummary::default()
    };
    let mut seen = HashSet::with_capacity(files.len());
    let mut uploads = Vec::new();

    for local in files {
        seen.insert(local.relative.clone());
        if states.get(&local.relative).is_some_and(|previous| {
            previous.size == local.size && previous.mtime_ns == local.mtime_ns
        }) {
            summary.unchanged += 1;
            continue;
        }
        uploads.push(PendingUpload {
            file: local,
            checkpoint_sha1: None,
        });
    }
    if !uploads.is_empty() {
        eprintln!(
            "[pdrive-sync] {}: {} files need remote reconciliation",
            mirror.name,
            uploads.len()
        );
    }
    execute_uploads(mirror, connection, drive, uploads, &mut summary)?;

    let stale = stale_paths(connection, &mirror.name, &seen)?;
    if mirror.delete == DeletePolicy::Trash {
        if remote_tree.is_none() && !stale.is_empty() {
            remote_tree = Some(inventory_remote(
                mirror,
                connection,
                drive,
                Some("cleanup"),
            )?);
        }

        let mut trash_paths = BTreeSet::new();
        if let Some(tree) = remote_tree.as_ref() {
            for path in &stale {
                if excludes.is_match(path) {
                    continue;
                }
                if tree.files.contains_key(path) {
                    trash_paths.insert(path.clone());
                } else {
                    delete_file_state(connection, &mirror.name, path)?;
                }
            }
            if !baseline_complete {
                trash_paths.extend(
                    tree.files
                        .keys()
                        .filter(|path| !seen.contains(*path) && !excludes.is_match(path))
                        .cloned(),
                );
            }
        }
        let trash_items = trash_paths
            .into_iter()
            .filter_map(|path| {
                remote_tree
                    .as_ref()
                    .and_then(|tree| tree.files.get(&path))
                    .cloned()
                    .map(|remote| (path, remote))
            })
            .collect();
        execute_remote_trash(mirror, connection, drive, trash_items, &mut summary)?;
    } else {
        for path in stale {
            if !excludes.is_match(&path) {
                delete_file_state(connection, &mirror.name, &path)?;
            }
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
    let tree = inventory_remote(sync, connection, drive, None)?;

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
        let sha1 = if states.get(&file.relative).is_some_and(|state| {
            !state.sha1.is_empty() && state.size == file.size && state.mtime_ns == file.mtime_ns
        }) {
            states[&file.relative].sha1.clone()
        } else {
            sha1_file(&file.absolute)?
        };
        local.insert(file.relative.clone(), LocalSnapshot { file, sha1 });
    }

    let mut remote = inventory_remote(sync, connection, drive, None)?;
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
    checkpoints.flush()?;

    let uploads = actions
        .iter()
        .filter_map(|action| match action {
            TwoWayAction::Upload { path, sha1 } => Some(PendingUpload {
                file: local[path].file.clone(),
                checkpoint_sha1: Some(sha1.clone()),
            }),
            _ => None,
        })
        .collect();
    execute_uploads(sync, connection, drive, uploads, &mut summary)?;

    let mut download_checkpoints = CheckpointBatch::new(connection);
    for action in &actions {
        let TwoWayAction::Download { path } = action else {
            continue;
        };
        let remote_file = &remote.files[path];
        let file = download_remote_file(sync, drive, path, remote_file)?;
        download_checkpoints.push(
            &sync.name,
            path,
            file.size,
            file.mtime_ns,
            &remote_file.sha1,
        )?;
        summary.downloaded += 1;
    }
    download_checkpoints.flush()?;

    let remote_trash = actions
        .iter()
        .filter_map(|action| match action {
            TwoWayAction::TrashRemote { path } => Some((path.clone(), remote.files[path].clone())),
            _ => None,
        })
        .collect();
    execute_remote_trash(sync, connection, drive, remote_trash, &mut summary)?;

    for action in &actions {
        if let TwoWayAction::TrashLocal { path } = action {
            trash::delete(&local[path].file.absolute)
                .with_context(|| format!("failed to trash local path {path}"))?;
            delete_file_state(connection, &sync.name, path)?;
            summary.trashed_local += 1;
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

fn execute_uploads(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    uploads: Vec<PendingUpload>,
    summary: &mut SyncSummary,
) -> Result<()> {
    if uploads.is_empty() {
        return Ok(());
    }

    let total = uploads.len();
    let mut by_parent = BTreeMap::<String, Vec<PendingUpload>>::new();
    for upload in uploads {
        by_parent
            .entry(relative_parent(&upload.file.relative).to_owned())
            .or_default()
            .push(upload);
    }

    let mut completed = 0;
    let mut failures = Vec::new();
    for (parent, parent_uploads) in by_parent {
        ensure_remote_directory(sync, connection, drive, &parent)?;
        for batch in parent_uploads.chunks(UPLOAD_BATCH_SIZE) {
            let batch_bytes = batch.iter().map(|upload| upload.file.size).sum::<u64>();
            eprintln!(
                "[pdrive-sync] {}: uploading batch of {} files ({} bytes)",
                sync.name,
                batch.len(),
                batch_bytes
            );
            let local_paths = batch
                .iter()
                .map(|upload| upload.file.absolute.clone())
                .collect::<Vec<_>>();
            let result = drive
                .upload_many(&local_paths, &remote_path(&sync.remote, &parent))
                .with_context(|| {
                    format!("failed to upload batch for remote directory {parent:?}")
                })?;
            let failed_names = result
                .failures
                .iter()
                .map(|failure| failure.name.as_str())
                .collect::<HashSet<_>>();
            let mut checkpoints = CheckpointBatch::new(connection);
            for upload in batch {
                let name = upload
                    .file
                    .absolute
                    .file_name()
                    .and_then(|name| name.to_str())
                    .context("local upload path has no UTF-8 file name")?;
                if failed_names.contains(name) {
                    continue;
                }
                checkpoints.push(
                    &sync.name,
                    &upload.file.relative,
                    upload.file.size,
                    upload.file.mtime_ns,
                    upload.checkpoint_sha1.as_deref().unwrap_or_default(),
                )?;
            }
            checkpoints.flush()?;

            summary.uploaded += result.transferred_items;
            summary.matched_remote += result.skipped_items;
            completed += batch.len();
            eprintln!(
                "[pdrive-sync] {}: reconciled {completed}/{total} files (uploaded={} already_present={} failed={} bytes={})",
                sync.name,
                result.transferred_items,
                result.skipped_items,
                result.failures.len(),
                result.transferred_bytes
            );
            failures.extend(result.failures);
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        let first = &failures[0];
        bail!(
            "{} uploads failed; first failure was {}: {}",
            failures.len(),
            first.name,
            first.error
        )
    }
}

fn execute_remote_trash(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    items: Vec<(String, RemoteFile)>,
    summary: &mut SyncSummary,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }

    let total = items.len();
    let mut completed = 0;
    let mut failures = 0;
    for batch in items.chunks(TRASH_BATCH_SIZE) {
        eprintln!(
            "[pdrive-sync] {}: moving {} remote files to trash",
            sync.name,
            batch.len()
        );
        let targets = batch
            .iter()
            .map(|(path, remote)| TrashTarget {
                remote_path: remote_path(&sync.remote, path),
                uid: remote.uid.clone(),
            })
            .collect::<Vec<_>>();
        let result = drive.trash_many(&targets)?;
        let succeeded = result.succeeded_uids.into_iter().collect::<HashSet<_>>();
        for (path, remote) in batch {
            if succeeded.contains(&remote.uid) {
                delete_file_state(connection, &sync.name, path)?;
                summary.trashed += 1;
            }
        }
        failures += result.failed_uids.len();
        completed += batch.len();
        eprintln!(
            "[pdrive-sync] {}: remote cleanup {completed}/{total} (failed={})",
            sync.name,
            result.failed_uids.len()
        );
    }

    if failures == 0 {
        Ok(())
    } else {
        bail!("{failures} remote files could not be moved to trash")
    }
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

fn inventory_remote(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    reason: Option<&str>,
) -> Result<RemoteTree> {
    let mut tree = RemoteTree::default();
    tree.directories.insert(String::new());
    let mut visited = HashSet::new();
    load_remote_tree(drive, &sync.remote, "", &mut visited, &mut tree)?;
    if let Some(reason) = reason {
        eprintln!(
            "[pdrive-sync] {}: remote {reason} listed {} files in {} directories",
            sync.name,
            tree.files.len(),
            tree.directories.len()
        );
    }
    replace_remote_directories(connection, &sync.name, &tree.directories)?;
    drive.release_session()?;
    Ok(tree)
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
    let uid = node.uid;
    let revision = node.active_revision?;
    Some(RemoteFile {
        uid,
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
        upload_batches: Vec<Vec<String>>,
        downloads: Vec<String>,
        trashed: Vec<String>,
        trash_batches: Vec<Vec<String>>,
        fail_upload: bool,
        fail_upload_names: HashSet<String>,
        fail_after_upload: bool,
        info_calls: usize,
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
                uid: file.uid.clone(),
                name: ResultValue {
                    ok: true,
                    value: Some(path.rsplit('/').next().unwrap().to_string()),
                },
                kind: "file".to_string(),
                total_storage_size: Some(file.claimed_size),
                active_revision: Some(RemoteRevision {
                    uid: format!("revision:{path}"),
                    storage_size: file.claimed_size,
                    claimed_size: file.claimed_size,
                    claimed_modification_time: None,
                    claimed_digests: RemoteDigests {
                        sha1: file.sha1.clone(),
                        sha1_verified: Some(true),
                    },
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
                    uid: format!("uid:{path}"),
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
            self.info_calls += 1;
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

        fn upload_many(
            &mut self,
            local_paths: &[PathBuf],
            remote_parent: &str,
        ) -> Result<UploadBatchResult> {
            if self.fail_upload {
                return Ok(UploadBatchResult {
                    failures: local_paths
                        .iter()
                        .map(|path| UploadFailure {
                            name: path.file_name().unwrap().to_string_lossy().into_owned(),
                            error: "simulated upload failure".to_owned(),
                        })
                        .collect(),
                    ..UploadBatchResult::default()
                });
            }
            self.upload_batches.push(
                local_paths
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect(),
            );
            let mut result = UploadBatchResult::default();
            for local_path in local_paths {
                let name = local_path.file_name().unwrap().to_string_lossy();
                if self.fail_upload_names.contains(name.as_ref()) {
                    result.failures.push(UploadFailure {
                        name: name.into_owned(),
                        error: "simulated upload failure".to_owned(),
                    });
                    continue;
                }
                let path = format!("{}/{}", remote_parent.trim_end_matches('/'), name);
                let metadata = fs::metadata(local_path)?;
                let digest = sha1_file(local_path)?;
                if self
                    .files
                    .get(&path)
                    .is_some_and(|file| file.sha1 == digest && file.claimed_size == metadata.len())
                {
                    result.skipped_items += 1;
                    continue;
                }
                self.files.insert(
                    path.clone(),
                    RemoteFile {
                        uid: format!("uid:{path}"),
                        sha1: digest,
                        claimed_size: metadata.len(),
                    },
                );
                self.contents.insert(path.clone(), fs::read(local_path)?);
                self.uploads.push(path);
                result.transferred_items += 1;
                result.transferred_bytes += metadata.len();
            }
            if self.fail_after_upload {
                bail!("simulated failure after accepted upload");
            }
            Ok(result)
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

        fn trash_many(&mut self, targets: &[TrashTarget]) -> Result<TrashBatchResult> {
            self.trash_batches.push(
                targets
                    .iter()
                    .map(|target| target.remote_path.clone())
                    .collect(),
            );
            let mut result = TrashBatchResult::default();
            for target in targets {
                if self.files.remove(&target.remote_path).is_some() {
                    self.contents.remove(&target.remote_path);
                    self.trashed.push(target.remote_path.clone());
                    result.succeeded_uids.push(target.uid.clone());
                } else {
                    result.failed_uids.push(target.uid.clone());
                }
            }
            Ok(result)
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
                uid: format!("uid:{}/already-there.txt", fixture.mirror.remote),
                sha1: sha1_file(&path).unwrap(),
                claimed_size: fs::metadata(path).unwrap().len(),
            },
        );

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.matched_remote, 1);
        assert_eq!(summary.uploaded, 0);
        assert!(drive.uploads.is_empty());
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

        assert_eq!(summary.matched_remote, 1);
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
    fn push_reconciles_files_in_bounded_batches_without_remote_info_calls() {
        let fixture = Fixture::new();
        for index in 0..(UPLOAD_BATCH_SIZE + 1) {
            fixture.write(&format!("file-{index:02}.txt"), "content");
        }
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.uploaded, UPLOAD_BATCH_SIZE + 1);
        assert_eq!(drive.info_calls, 0);
        assert_eq!(
            drive
                .upload_batches
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![UPLOAD_BATCH_SIZE, 1]
        );
        assert!(
            all_file_states(&fixture.connection, &fixture.mirror.name)
                .unwrap()
                .values()
                .all(|state| state.sha1.is_empty())
        );
    }

    #[test]
    fn successful_files_are_checkpointed_when_a_batch_partially_fails() {
        let fixture = Fixture::new();
        fixture.write("good.txt", "uploaded");
        fixture.write("retry.txt", "retry later");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        drive.fail_upload_names.insert("retry.txt".to_owned());

        assert!(sync_push(&fixture.mirror, &fixture.connection, &mut drive).is_err());

        assert!(
            file_state(&fixture.connection, &fixture.mirror.name, "good.txt")
                .unwrap()
                .is_some()
        );
        assert!(
            file_state(&fixture.connection, &fixture.mirror.name, "retry.txt")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn failed_upload_prevents_remote_cleanup() {
        let mut fixture = Fixture::new();
        fixture.mirror.delete = DeletePolicy::Trash;
        let stale = fixture.write("keep-until-success.txt", "existing");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        fs::remove_file(stale).unwrap();
        fixture.write("retry.txt", "retry later");
        drive.fail_upload_names.insert("retry.txt".to_owned());

        assert!(sync_push(&fixture.mirror, &fixture.connection, &mut drive).is_err());
        assert!(drive.trashed.is_empty());
        assert!(
            drive
                .files
                .contains_key(&format!("{}/keep-until-success.txt", fixture.mirror.remote))
        );
    }

    #[test]
    fn remote_cleanup_uses_bounded_batches() {
        let mut fixture = Fixture::new();
        fixture.mirror.delete = DeletePolicy::Trash;
        let mut paths = Vec::new();
        for index in 0..(TRASH_BATCH_SIZE + 1) {
            paths.push(fixture.write(&format!("file-{index:02}.txt"), "content"));
        }
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        for path in paths {
            fs::remove_file(path).unwrap();
        }
        drive.trash_batches.clear();

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.trashed, TRASH_BATCH_SIZE + 1);
        assert_eq!(
            drive.trash_batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![TRASH_BATCH_SIZE, 1]
        );
        assert!(
            all_file_states(&fixture.connection, &fixture.mirror.name)
                .unwrap()
                .is_empty()
        );
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
                uid: format!("uid:{}/remote-only.txt", fixture.mirror.remote),
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
                uid: format!("uid:{}/remote-only.txt", fixture.mirror.remote),
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
    fn two_way_rebuilds_the_digest_omitted_by_push() {
        let mut fixture = Fixture::new();
        fixture.write("shared.txt", "content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        assert!(
            file_state(&fixture.connection, &fixture.mirror.name, "shared.txt")
                .unwrap()
                .unwrap()
                .sha1
                .is_empty()
        );

        fixture.mirror.mode = SyncMode::TwoWay;
        let summary = sync_two_way(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.unchanged, 1);
        assert_eq!(drive.uploads.len(), 1);
        assert!(
            !file_state(&fixture.connection, &fixture.mirror.name, "shared.txt")
                .unwrap()
                .unwrap()
                .sha1
                .is_empty()
        );
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
            notifications: true,
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

        drive.upload_many(&[local], "/my-files/target").unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn cli_drive_maps_batch_transfer_and_trash_results() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let script = temp.path().join("fake-proton-drive");
        fs::write(
            &script,
            r#"#!/usr/bin/env bash
case "$2" in
    upload)
        printf '%s\n' '{"transferredItems":1,"transferredBytes":7,"skippedItems":0,"failedItems":1,"failures":[{"name":"retry\nfile.txt","error":"No space"}]}'
        ;;
    trash)
        printf '%s\n' '[{"uid":"one","ok":true},{"uid":"two","ok":false}]'
        ;;
    *)
        exit 2
        ;;
esac
"#,
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let good = temp.path().join("good.txt");
        let retry = temp.path().join("retry\nfile.txt");
        fs::write(&good, "content").unwrap();
        fs::write(&retry, "content").unwrap();
        let mut drive = CliDrive::new(script);

        let upload = drive
            .upload_many(&[good, retry], "/my-files/target")
            .unwrap();
        let trash = drive
            .trash_many(&[
                TrashTarget {
                    remote_path: "/my-files/one\n".to_owned(),
                    uid: "one".to_owned(),
                },
                TrashTarget {
                    remote_path: "/my-files/two".to_owned(),
                    uid: "two".to_owned(),
                },
            ])
            .unwrap();

        assert_eq!(upload.transferred_items, 1);
        assert_eq!(upload.transferred_bytes, 7);
        assert_eq!(upload.failures.len(), 1);
        assert_eq!(upload.failures[0].name, "retry\nfile.txt");
        assert_eq!(trash.succeeded_uids, vec!["one"]);
        assert_eq!(trash.failed_uids, vec!["two"]);
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
