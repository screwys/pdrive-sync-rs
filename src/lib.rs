// SPDX-License-Identifier: MIT

mod drive;
mod state;

pub use drive::{
    DriveClient, LocalUpload, RemoteDigests, RemoteEvent, RemoteEvents, RemoteFile, RemoteNode,
    RemoteParent, RemoteRevision, RemoteVersion, ResultValue, SdkDrive, TrashBatchResult,
    TrashTarget, UploadBatchResult, UploadFailure,
};
#[cfg(test)]
use state::CHECKPOINT_BATCH_SIZE;
use state::{
    CheckpointBatch, FileState, RemoteSnapshot, all_file_states, bind_sync, delete_file_state,
    delete_file_states_and_remote_nodes, file_state, metadata_value, remote_snapshot,
    replace_remote_snapshot, set_metadata, stale_paths,
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
    #[serde(default = "default_sdk_bin")]
    pub sdk_bin: PathBuf,
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

pub fn default_sdk_bin() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join("pdrive-sync-sdk")))
        .unwrap_or_else(|| PathBuf::from("pdrive-sync-sdk"))
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
    expected_remote: Option<RemoteVersion>,
}

#[derive(Default)]
struct RemoteTree {
    files: HashMap<String, RemoteFile>,
    directories: HashSet<String>,
    directory_uids: HashMap<String, String>,
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
    let mut remote_tree = inventory_remote(mirror, connection, drive, None)?;
    let (files, skipped_symlinks) = scan_local_files(mirror, &excludes)?;
    let states = all_file_states(connection, &mirror.name)?;
    let baseline_key = format!("baseline:{}", mirror.name);
    let baseline_complete = metadata_value(connection, &baseline_key)?.as_deref() == Some("1");
    let mut summary = SyncSummary {
        scanned: files.len(),
        skipped_symlinks,
        ..SyncSummary::default()
    };
    let mut seen = HashSet::with_capacity(files.len());
    let mut uploads = Vec::new();

    let mut rebuilt_digests = 0;
    let mut migration = CheckpointBatch::new(connection);
    for local in files {
        seen.insert(local.relative.clone());
        let previous = states
            .get(&local.relative)
            .filter(|previous| previous.size == local.size && previous.mtime_ns == local.mtime_ns);
        let rebuilt = if previous.is_some_and(|previous| previous.sha1.is_empty()) {
            let digest = sha1_file(&local.absolute)?;
            ensure_local_version(mirror, &local.relative, Some(&local))?;
            rebuilt_digests += 1;
            if rebuilt_digests % 1000 == 0 {
                eprintln!(
                    "[pdrive-sync] {}: rebuilt {rebuilt_digests} checkpoint digests",
                    mirror.name
                );
            }
            Some(digest)
        } else {
            None
        };
        let digest = rebuilt
            .as_deref()
            .or_else(|| previous.map(|previous| previous.sha1.as_str()));
        if previous.is_some()
            && remote_tree
                .files
                .get(&local.relative)
                .is_some_and(|remote| {
                    remote.claimed_size == local.size
                        && digest.is_some_and(|digest| remote.sha1.eq_ignore_ascii_case(digest))
                })
        {
            if let Some(digest) = &rebuilt {
                migration.push(
                    &mirror.name,
                    &local.relative,
                    local.size,
                    local.mtime_ns,
                    digest,
                )?;
            }
            summary.unchanged += 1;
            continue;
        }
        uploads.push(PendingUpload {
            file: local,
            checkpoint_sha1: rebuilt,
            expected_remote: None,
        });
    }
    migration.flush()?;
    if !uploads.is_empty() {
        eprintln!(
            "[pdrive-sync] {}: {} files need checking",
            mirror.name,
            uploads.len()
        );
    }
    let had_uploads = !uploads.is_empty();
    execute_uploads(
        mirror,
        connection,
        drive,
        uploads,
        &mut remote_tree.directory_uids,
        &mut summary,
    )?;

    // Uploads can take long enough for local files to be added or removed.
    if had_uploads {
        require_ready(mirror)?;
        let (current_files, _) = scan_local_files(mirror, &excludes)?;
        seen = current_files
            .into_iter()
            .map(|file| file.relative)
            .collect();
    }
    let stale = stale_paths(connection, &mirror.name, &seen)?;
    if mirror.delete == DeletePolicy::Trash {
        let mut trash_paths = BTreeSet::new();
        {
            let tree = &remote_tree;
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
                    .files
                    .get(&path)
                    .cloned()
                    .map(|remote| (path, remote))
            })
            .collect();
        execute_remote_trash(
            mirror,
            connection,
            drive,
            trash_items,
            &remote_tree.directory_uids,
            &mut summary,
        )?;
    } else {
        for path in stale {
            if !excludes.is_match(&path) {
                delete_file_state(connection, &mirror.name, &path)?;
            }
        }
    }
    set_metadata(connection, &baseline_key, "1")?;
    Ok(summary)
}

pub fn sync_pull(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
) -> Result<SyncSummary> {
    require_ready(sync)?;
    let excludes = build_excludes(sync)?;
    let tree = inventory_remote(sync, connection, drive, None)?;
    let (local_files, skipped_symlinks) = scan_local_files(sync, &excludes)?;
    let local_files = local_files
        .into_iter()
        .map(|file| (file.relative.clone(), file))
        .collect::<HashMap<_, _>>();

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

        let local = download_remote_file(sync, drive, path, remote, None, false)?;
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
    let initial = inventory_remote(sync, connection, drive, None)?;
    let initial_root_uid = initial.directory_uids[""].clone();
    drop(initial);
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
    if remote.directory_uids[""] != initial_root_uid {
        bail!("remote sync root changed during local scan; retry the sync");
    }
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
                expected_remote: Some(remote.files.get(path).map_or(
                    RemoteVersion::Absent,
                    |file| RemoteVersion::Revision {
                        uid: file.uid.clone(),
                        revision_uid: file.revision_uid.clone(),
                    },
                )),
            }),
            _ => None,
        })
        .collect();
    execute_uploads(
        sync,
        connection,
        drive,
        uploads,
        &mut remote.directory_uids,
        &mut summary,
    )?;

    let mut download_checkpoints = CheckpointBatch::new(connection);
    for action in &actions {
        let TwoWayAction::Download { path } = action else {
            continue;
        };
        let remote_file = &remote.files[path];
        let file = download_remote_file(
            sync,
            drive,
            path,
            remote_file,
            local.get(path).map(|snapshot| &snapshot.file),
            true,
        )?;
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
    execute_remote_trash(
        sync,
        connection,
        drive,
        remote_trash,
        &remote.directory_uids,
        &mut summary,
    )?;

    for action in &actions {
        if let TwoWayAction::TrashLocal { path } = action {
            ensure_local_version(sync, path, Some(&local[path].file))?;
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
    expected_local: Option<&LocalFile>,
    protect_local: bool,
) -> Result<LocalFile> {
    let target = sync.local.join(relative);
    let parent = target
        .parent()
        .context("local download target has no parent")?;
    fs::create_dir_all(parent)?;
    let staging = tempfile::Builder::new()
        .prefix(".pdrive-sync-download-")
        .tempdir_in(parent)?;
    if protect_local {
        ensure_local_version(sync, relative, expected_local)?;
    }
    drive.download(remote, staging.path())?;
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
    if protect_local {
        ensure_local_version(sync, relative, expected_local)?;
    }
    fs::rename(&staged, &target)
        .with_context(|| format!("failed to install downloaded file {relative}"))?;
    local_file(&sync.local, target)
}

fn ensure_local_version(
    sync: &SyncConfig,
    relative: &str,
    expected: Option<&LocalFile>,
) -> Result<()> {
    let path = sync.local.join(relative);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let current = local_file(&sync.local, path)?;
            if expected
                .is_some_and(|file| current.size == file.size && current.mtime_ns == file.mtime_ns)
            {
                return Ok(());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && expected.is_none() => {
            return Ok(());
        }
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    bail!("local file changed during sync: {relative}; retry the sync")
}

fn execute_uploads(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    uploads: Vec<PendingUpload>,
    directories: &mut HashMap<String, String>,
    summary: &mut SyncSummary,
) -> Result<()> {
    if uploads.is_empty() {
        return Ok(());
    }

    let total = uploads.len();
    let mut by_parent = BTreeMap::<String, Vec<PendingUpload>>::new();
    for mut upload in uploads {
        if upload.checkpoint_sha1.is_some() {
            ensure_local_version(sync, &upload.file.relative, Some(&upload.file))?;
        } else {
            upload.file = local_file(&sync.local, upload.file.absolute.clone())?;
        }
        by_parent
            .entry(relative_parent(&upload.file.relative).to_owned())
            .or_default()
            .push(upload);
    }

    let mut completed = 0;
    let mut failures = Vec::new();
    for (parent, parent_uploads) in by_parent {
        let parent_uid = ensure_remote_directory(sync, connection, drive, &parent, directories)?;
        let target = remote_parent(sync, &parent, parent_uid);
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
                .map(|upload| LocalUpload {
                    path: upload.file.absolute.clone(),
                    sha1: upload.checkpoint_sha1.clone(),
                    expected_remote: upload.expected_remote.clone(),
                })
                .collect::<Vec<_>>();
            let result = drive.upload_many(&local_paths, &target).with_context(|| {
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
                    result
                        .nodes
                        .iter()
                        .find(|node| node.name.value.as_deref() == Some(name))
                        .and_then(|node| node.active_revision.as_ref())
                        .and_then(|revision| revision.claimed_digests.as_ref())
                        .and_then(|digests| digests.sha1.as_deref())
                        .context("successful SDK upload has no checksum receipt")?,
                )?;
            }
            checkpoints.flush_with_remote_nodes(&sync.name, &result.nodes)?;

            summary.uploaded += result.transferred_items;
            summary.matched_remote += result.skipped_items;
            completed += batch.len();
            eprintln!(
                "[pdrive-sync] {}: processed {completed}/{total} files (uploaded={} already_present={} failed={} bytes={})",
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
    directories: &HashMap<String, String>,
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
                parent: remote_parent(
                    sync,
                    relative_parent(path),
                    directories[relative_parent(path)].clone(),
                ),
                name: path.rsplit('/').next().unwrap_or(path).to_owned(),
                uid: remote.uid.clone(),
                revision_uid: (sync.mode == SyncMode::TwoWay).then(|| remote.revision_uid.clone()),
            })
            .collect::<Vec<_>>();
        let result = drive.trash_many(&targets)?;
        let succeeded = result
            .succeeded_uids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let paths = batch
            .iter()
            .filter(|(_, remote)| succeeded.contains(&remote.uid))
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        delete_file_states_and_remote_nodes(
            connection,
            &sync.name,
            &paths,
            &result.succeeded_uids,
        )?;
        summary.trashed += paths.len();
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
    let entries = fs::read_dir(directory)
        .with_context(|| format!("failed to read {}", directory.display()))?;
    for entry in entries {
        let entry = entry?;
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
    let excludes = build_excludes(sync)?;
    bind_sync(connection, sync, None)?;
    let mut cached = remote_snapshot(connection, &sync.name)?;
    if cached.is_none() {
        drive.reset_cache()?;
    }
    // Process saved events before resolving names through the SDK's node cache.
    let changes = cached
        .as_ref()
        .map(|snapshot| drive.events(&snapshot.scope_id, Some(&snapshot.cursor)))
        .transpose()?;
    if changes.as_ref().is_some_and(|changes| changes.removed) {
        drive.reset_cache()?;
    }
    let root = drive
        .info(&sync.remote)?
        .context("remote sync root does not exist")?;
    if root.kind != "folder" {
        bail!("remote sync root is not a folder");
    }
    let scope = root
        .tree_event_scope_id
        .clone()
        .context("remote root has no event scope")?;
    if bind_sync(connection, sync, Some(&root.uid))? {
        cached = None;
    }
    let filters_key = format!("remote-excludes:{}", sync.name);
    let filters = serde_json::to_string(&sync.exclude)?;
    if metadata_value(connection, &filters_key)?.as_deref() != Some(&filters) {
        cached = None;
    }
    if changes.as_ref().is_some_and(|events| {
        events.events.iter().any(|event| {
            event.node_uid.as_deref() == Some(&root.uid)
                && (event.kind == "node_deleted" || event.is_trashed == Some(true))
        })
    }) {
        cached = None;
    }
    let verify_root = cached.is_none()
        || changes.as_ref().is_some_and(|changes| {
            changes.refresh
                || changes.removed
                || changes
                    .events
                    .iter()
                    .any(|event| event.node_uid.as_deref() == Some(&root.uid))
        });
    let mut snapshot_changed = true;
    let mut snapshot = match (cached, changes) {
        (Some(mut snapshot), Some(changes))
            if snapshot.root_uid == root.uid
                && snapshot.scope_id == scope
                && !changes.refresh
                && !changes.removed =>
        {
            snapshot_changed = snapshot.cursor != changes.cursor
                || !changes.events.is_empty()
                || snapshot.nodes.get(&root.uid) != Some(&root);
            snapshot.nodes.insert(root.uid.clone(), root.clone());
            apply_remote_events(drive, &excludes, &mut snapshot, changes)?;
            snapshot
        }
        _ => {
            let start = drive.events(&scope, None)?;
            if start.removed {
                bail!("remote sync tree is no longer accessible");
            }
            let mut snapshot = RemoteSnapshot {
                root_uid: root.uid.clone(),
                scope_id: scope,
                cursor: start.cursor,
                nodes: HashMap::new(),
            };
            load_remote_nodes(drive, &root, "", &excludes, &mut snapshot.nodes)?;
            let changes = drive.events(&snapshot.scope_id, Some(&snapshot.cursor))?;
            if changes.refresh || changes.removed {
                bail!("remote tree needs a new inventory; retry the sync");
            }
            apply_remote_events(drive, &excludes, &mut snapshot, changes)?;
            snapshot
        }
    };
    let root = if verify_root {
        let current = drive
            .info(&sync.remote)?
            .context("remote sync root was removed during inventory")?;
        if current.uid != root.uid {
            bail!("remote sync root changed during inventory; retry the sync");
        }
        current
    } else {
        root
    };
    snapshot_changed |= snapshot.nodes.get(&root.uid) != Some(&root);
    snapshot.nodes.insert(root.uid.clone(), root);
    let tree = remote_tree(&snapshot, &excludes)?;
    if snapshot_changed {
        replace_remote_snapshot(connection, &sync.name, &snapshot)?;
    }
    set_metadata(connection, &filters_key, &filters)?;
    if let Some(reason) = reason {
        eprintln!(
            "[pdrive-sync] {}: remote {reason} has {} files in {} directories",
            sync.name,
            tree.files.len(),
            tree.directories.len()
        );
    }
    Ok(tree)
}

fn remote_name(node: &RemoteNode) -> Result<&str> {
    if !node.name.ok {
        bail!("remote node name could not be decrypted; inventory is incomplete");
    }
    let name = node
        .name
        .value
        .as_deref()
        .context("remote node has no name; inventory is incomplete")?;
    if name.contains('/') {
        bail!("remote name contains a path separator");
    }
    Ok(name)
}

fn load_remote_nodes(
    drive: &mut dyn DriveClient,
    root: &RemoteNode,
    relative: &str,
    excludes: &GlobSet,
    nodes: &mut HashMap<String, RemoteNode>,
) -> Result<()> {
    nodes.insert(root.uid.clone(), root.clone());
    for mut node in drive.list(&root.uid)? {
        let path = join_relative(relative, remote_name(&node)?);
        if excludes.is_match(&path)
            || (node.kind == "folder" && excludes.is_match(format!("{path}/")))
        {
            continue;
        }
        node.parent_uid = Some(root.uid.clone());
        if node.kind == "folder" {
            load_remote_nodes(drive, &node, &path, excludes, nodes)?;
            if nodes.len().is_multiple_of(250) {
                eprintln!(
                    "[pdrive-sync] remote inventory: {} nodes listed",
                    nodes.len()
                );
            }
        } else {
            nodes.insert(node.uid.clone(), node);
        }
    }
    Ok(())
}

fn remove_remote_subtree(nodes: &mut HashMap<String, RemoteNode>, uid: &str) {
    if nodes.get(uid).is_none_or(|node| node.kind != "folder") {
        nodes.remove(uid);
        return;
    }
    let mut children = HashMap::<String, Vec<String>>::new();
    for node in nodes.values() {
        if let Some(parent) = &node.parent_uid {
            children
                .entry(parent.clone())
                .or_default()
                .push(node.uid.clone());
        }
    }
    let mut remove = vec![uid.to_owned()];
    while let Some(uid) = remove.pop() {
        if let Some(descendants) = children.remove(&uid) {
            remove.extend(descendants);
        }
        nodes.remove(&uid);
    }
}

fn remote_relative(uid: &str, snapshot: &RemoteSnapshot) -> Result<Option<String>> {
    let mut uid = uid;
    let mut parts = Vec::new();
    while uid != snapshot.root_uid {
        let Some(node) = snapshot.nodes.get(uid) else {
            return Ok(None);
        };
        parts.push(node);
        let Some(parent) = node.parent_uid.as_deref() else {
            return Ok(None);
        };
        uid = parent;
    }
    parts.reverse();
    Ok(Some(
        parts
            .into_iter()
            .map(remote_name)
            .collect::<Result<Vec<_>>>()?
            .join("/"),
    ))
}

fn apply_remote_events(
    drive: &mut dyn DriveClient,
    excludes: &GlobSet,
    snapshot: &mut RemoteSnapshot,
    changes: RemoteEvents,
) -> Result<()> {
    let has_node_changes = changes.events.iter().any(|event| event.node_uid.is_some());
    for event in changes.events {
        let Some(uid) = event.node_uid else {
            continue;
        };
        if event.kind == "node_deleted" || event.is_trashed == Some(true) {
            if uid == snapshot.root_uid {
                bail!("remote sync root was removed during inventory");
            }
            remove_remote_subtree(&mut snapshot.nodes, &uid);
            continue;
        }
        let Some(node) = drive.info(&uid)? else {
            remove_remote_subtree(&mut snapshot.nodes, &uid);
            continue;
        };
        let mut chain = vec![node.clone()];
        let mut parent = node.parent_uid.clone();
        while let Some(uid) = parent {
            if snapshot.nodes.contains_key(&uid) {
                break;
            }
            let Some(ancestor) = drive.info(&uid)? else {
                break;
            };
            parent = ancestor.parent_uid.clone();
            chain.push(ancestor);
        }
        let new_folders = chain
            .iter()
            .filter(|node| node.kind == "folder" && !snapshot.nodes.contains_key(&node.uid))
            .map(|node| node.uid.clone())
            .collect::<Vec<_>>();
        let chain_uids = chain
            .iter()
            .map(|node| node.uid.clone())
            .collect::<Vec<_>>();
        for node in chain {
            snapshot.nodes.insert(node.uid.clone(), node);
        }
        if remote_relative(&node.uid, snapshot)?.is_some() {
            // A child event can introduce several ancestors. Remove excluded ones before listing.
            for uid in chain_uids.into_iter().rev() {
                if let Some(relative) = remote_relative(&uid, snapshot)? {
                    let entry = &snapshot.nodes[&uid];
                    if excludes.is_match(&relative)
                        || (entry.kind == "folder" && excludes.is_match(format!("{relative}/")))
                    {
                        remove_remote_subtree(&mut snapshot.nodes, &uid);
                    }
                }
            }
            for uid in new_folders.into_iter().rev() {
                if let Some(relative) = remote_relative(&uid, snapshot)? {
                    let folder = snapshot.nodes[&uid].clone();
                    load_remote_nodes(drive, &folder, &relative, excludes, &mut snapshot.nodes)?;
                    break;
                }
            }
        } else {
            remove_remote_subtree(&mut snapshot.nodes, &node.uid);
        }
    }
    snapshot.cursor = changes.cursor;
    if has_node_changes {
        let mut children = HashMap::<&str, Vec<&str>>::new();
        for node in snapshot.nodes.values() {
            if let Some(parent) = node.parent_uid.as_deref() {
                children.entry(parent).or_default().push(&node.uid);
            }
        }
        let mut keep = HashSet::new();
        let mut pending = vec![snapshot.root_uid.as_str()];
        while let Some(uid) = pending.pop() {
            keep.insert(uid.to_owned());
            if let Some(nodes) = children.get(uid) {
                pending.extend(nodes);
            }
        }
        snapshot.nodes.retain(|uid, _| keep.contains(uid));
    }
    Ok(())
}

fn remote_tree(snapshot: &RemoteSnapshot, excludes: &GlobSet) -> Result<RemoteTree> {
    let mut tree = RemoteTree::default();
    tree.directories.insert(String::new());
    tree.directory_uids
        .insert(String::new(), snapshot.root_uid.clone());
    let mut children = HashMap::<&str, Vec<&RemoteNode>>::new();
    for node in snapshot.nodes.values() {
        if let Some(parent) = node.parent_uid.as_deref() {
            children.entry(parent).or_default().push(node);
        }
    }
    let mut folders = vec![(snapshot.root_uid.as_str(), String::new())];
    while let Some((uid, parent)) = folders.pop() {
        for node in children.get(uid).into_iter().flatten() {
            let relative = join_relative(&parent, remote_name(node)?);
            if excludes.is_match(&relative)
                || (node.kind == "folder" && excludes.is_match(format!("{relative}/")))
            {
                continue;
            }
            match node.kind.as_str() {
                "folder" => {
                    tree.directories.insert(relative.clone());
                    tree.directory_uids
                        .insert(relative.clone(), node.uid.clone());
                    folders.push((&node.uid, relative));
                }
                "file" => {
                    let revision = node
                        .active_revision
                        .as_ref()
                        .context("remote file metadata is incomplete")?;
                    let sha1 = revision
                        .claimed_digests
                        .as_ref()
                        .and_then(|digests| digests.sha1.as_deref())
                        .filter(|digest| !digest.is_empty())
                        .with_context(|| {
                            format!("remote checksum is unavailable for {relative}")
                        })?;
                    let size = revision
                        .claimed_size
                        .with_context(|| format!("remote size is unavailable for {relative}"))?;
                    tree.files.insert(
                        relative,
                        RemoteFile {
                            uid: node.uid.clone(),
                            revision_uid: revision.uid.clone(),
                            sha1: sha1.to_owned(),
                            claimed_size: size,
                        },
                    );
                }
                _ => {}
            }
        }
    }
    Ok(tree)
}

fn ensure_remote_directory(
    sync: &SyncConfig,
    connection: &Connection,
    drive: &mut dyn DriveClient,
    relative: &str,
    directories: &mut HashMap<String, String>,
) -> Result<String> {
    if let Some(uid) = directories.get(relative) {
        return Ok(uid.clone());
    }
    let parent = relative_parent(relative);
    let parent_uid = ensure_remote_directory(sync, connection, drive, parent, directories)?;
    let name = relative.rsplit('/').next().unwrap_or(relative);
    let node = match drive.child(&parent_uid, name)? {
        Some(node) if node.kind == "folder" => node,
        Some(_) => bail!("remote path exists but is not a folder: {relative}"),
        None => drive.create_folder(&remote_parent(sync, parent, parent_uid), name)?,
    };
    let mut receipts = CheckpointBatch::new(connection);
    receipts.flush_with_remote_nodes(&sync.name, std::slice::from_ref(&node))?;
    directories.insert(relative.to_owned(), node.uid.clone());
    Ok(node.uid)
}

fn remote_parent(sync: &SyncConfig, relative: &str, uid: String) -> RemoteParent {
    RemoteParent {
        uid,
        root: sync.remote.clone(),
        components: if relative.is_empty() {
            Vec::new()
        } else {
            relative.split('/').map(str::to_owned).collect()
        },
    }
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
        event_state: HashMap<String, String>,
        event_cursor: usize,
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
                parent_uid: path
                    .rsplit_once('/')
                    .map(|(parent, _)| format!("uid:{parent}")),
                tree_event_scope_id: Some("mock-scope".to_owned()),
                name: ResultValue {
                    ok: true,
                    value: Some(path.rsplit('/').next().unwrap().to_string()),
                },
                kind: "file".to_string(),
                total_storage_size: Some(file.claimed_size),
                active_revision: Some(RemoteRevision {
                    uid: file.revision_uid.clone(),
                    storage_size: Some(file.claimed_size),
                    claimed_size: Some(file.claimed_size),
                    claimed_modification_time: None,
                    claimed_digests: Some(RemoteDigests {
                        sha1: Some(file.sha1.clone()),
                        sha1_verified: Some(true),
                    }),
                }),
            }
        }

        fn folder_node(path: &str) -> RemoteNode {
            RemoteNode {
                uid: format!("uid:{path}"),
                parent_uid: path
                    .rsplit_once('/')
                    .map(|(parent, _)| format!("uid:{parent}")),
                tree_event_scope_id: Some("mock-scope".to_owned()),
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
                    revision_uid: format!("revision:{path}"),
                    sha1: format!("{:x}", hasher.finalize()),
                    claimed_size: content.len() as u64,
                },
            );
            self.contents.insert(path, content.to_vec());
        }
    }

    impl DriveClient for MockDrive {
        fn reset_cache(&mut self) -> Result<()> {
            Ok(())
        }
        fn child(&mut self, parent_uid: &str, name: &str) -> Result<Option<RemoteNode>> {
            let parent = parent_uid.strip_prefix("uid:").unwrap_or(parent_uid);
            self.info(&format!("{parent}/{name}"))
        }

        fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>> {
            let remote_path = remote_path.strip_prefix("uid:").unwrap_or(remote_path);
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
            let remote_path = remote_path.strip_prefix("uid:").unwrap_or(remote_path);
            self.info_calls += 1;
            if let Some(file) = self.files.get(remote_path) {
                return Ok(Some(Self::file_node(remote_path, file)));
            }
            if self.directories.contains(remote_path) {
                return Ok(Some(Self::folder_node(remote_path)));
            }
            Ok(None)
        }

        fn create_folder(&mut self, parent: &RemoteParent, name: &str) -> Result<RemoteNode> {
            let parent_path = parent.uid.strip_prefix("uid:").unwrap_or(&parent.uid);
            let path = format!("{}/{}", parent_path.trim_end_matches('/'), name);
            self.directories.insert(path.clone());
            Ok(Self::folder_node(&path))
        }

        fn upload_many(
            &mut self,
            uploads: &[LocalUpload],
            parent: &RemoteParent,
        ) -> Result<UploadBatchResult> {
            let remote_parent = parent.uid.strip_prefix("uid:").unwrap_or(&parent.uid);
            let local_paths = uploads
                .iter()
                .map(|upload| upload.path.clone())
                .collect::<Vec<_>>();
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
            for local_path in &local_paths {
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
                    result
                        .nodes
                        .push(Self::file_node(&path, &self.files[&path]));
                    continue;
                }
                self.files.insert(
                    path.clone(),
                    RemoteFile {
                        uid: format!("uid:{path}"),
                        revision_uid: format!("revision:{path}"),
                        sha1: digest,
                        claimed_size: metadata.len(),
                    },
                );
                self.contents.insert(path.clone(), fs::read(local_path)?);
                result
                    .nodes
                    .push(Self::file_node(&path, &self.files[&path]));
                self.uploads.push(path);
                result.transferred_items += 1;
                result.transferred_bytes += metadata.len();
            }
            if self.fail_after_upload {
                bail!("simulated failure after accepted upload");
            }
            Ok(result)
        }

        fn download(&mut self, remote: &RemoteFile, local_parent: &Path) -> Result<()> {
            let remote_path = remote.uid.strip_prefix("uid:").unwrap_or(&remote.uid);
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
                    .map(|target| {
                        format!(
                            "{}/{}",
                            target
                                .parent
                                .uid
                                .strip_prefix("uid:")
                                .unwrap_or(&target.parent.uid),
                            target.name
                        )
                    })
                    .collect(),
            );
            let mut result = TrashBatchResult::default();
            for target in targets {
                let path = format!(
                    "{}/{}",
                    target
                        .parent
                        .uid
                        .strip_prefix("uid:")
                        .unwrap_or(&target.parent.uid),
                    target.name
                );
                if self.files.remove(&path).is_some() {
                    self.contents.remove(&path);
                    self.trashed.push(path);
                    result.succeeded_uids.push(target.uid.clone());
                } else {
                    result.failed_uids.push(target.uid.clone());
                }
            }
            Ok(result)
        }

        fn events(&mut self, _: &str, cursor: Option<&str>) -> Result<RemoteEvents> {
            let mut current = self
                .directories
                .iter()
                .map(|path| (format!("uid:{path}"), "folder".to_owned()))
                .collect::<HashMap<_, _>>();
            current.extend(
                self.files
                    .iter()
                    .map(|(path, file)| (format!("uid:{path}"), file.sha1.clone())),
            );
            let mut events = Vec::new();
            if cursor.is_some() {
                for (uid, fingerprint) in &current {
                    if self.event_state.get(uid) != Some(fingerprint) {
                        events.push(RemoteEvent {
                            kind: "node_updated".to_owned(),
                            node_uid: Some(uid.clone()),
                            parent_node_uid: None,
                            is_trashed: Some(false),
                        });
                    }
                }
                for uid in self
                    .event_state
                    .keys()
                    .filter(|uid| !current.contains_key(*uid))
                {
                    events.push(RemoteEvent {
                        kind: "node_deleted".to_owned(),
                        node_uid: Some(uid.clone()),
                        parent_node_uid: None,
                        is_trashed: None,
                    });
                }
            }
            if current != self.event_state || self.event_cursor == 0 {
                self.event_cursor += 1;
            }
            self.event_state = current;
            Ok(RemoteEvents {
                cursor: self.event_cursor.to_string(),
                events,
                refresh: false,
                removed: false,
            })
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
                revision_uid: "mock-revision".to_owned(),
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
    fn push_reconciles_files_in_bounded_batches_without_per_file_lookups() {
        let fixture = Fixture::new();
        for index in 0..(UPLOAD_BATCH_SIZE + 1) {
            fixture.write(&format!("file-{index:02}.txt"), "content");
        }
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);

        let summary = sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();

        assert_eq!(summary.uploaded, UPLOAD_BATCH_SIZE + 1);
        assert_eq!(drive.info_calls, 2);
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
                .all(|state| !state.sha1.is_empty())
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
                revision_uid: "mock-revision".to_owned(),
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
                revision_uid: "mock-revision".to_owned(),
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
    fn two_way_reuses_the_sdk_digest_from_push() {
        let mut fixture = Fixture::new();
        fixture.write("shared.txt", "content");
        let mut drive = MockDrive::with_root(&fixture.mirror.remote);
        sync_push(&fixture.mirror, &fixture.connection, &mut drive).unwrap();
        assert!(
            !file_state(&fixture.connection, &fixture.mirror.name, "shared.txt")
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
            sdk_bin: PathBuf::from("pdrive-sync-sdk"),
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
}
