// SPDX-License-Identifier: MIT

use crate::SyncConfig;
use crate::drive::RemoteNode;
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) const CHECKPOINT_BATCH_SIZE: usize = 256;

#[derive(Clone, Debug)]
pub(crate) struct FileState {
    pub(crate) size: u64,
    pub(crate) mtime_ns: i64,
    pub(crate) sha1: String,
}

pub(crate) struct RemoteSnapshot {
    pub root_uid: String,
    pub scope_id: String,
    pub cursor: String,
    pub nodes: HashMap<String, RemoteNode>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct RemoteScan {
    pub root_uid: String,
    pub scope_id: String,
    pub cursor: String,
    pub excludes: Vec<String>,
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
        DROP TABLE IF EXISTS remote_directories;
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sync_identities (
            mirror TEXT PRIMARY KEY,
            local_root TEXT NOT NULL,
            remote_root TEXT NOT NULL,
            root_uid TEXT
        );
        CREATE TABLE IF NOT EXISTS remote_snapshots (
            mirror TEXT PRIMARY KEY,
            root_uid TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            cursor TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS remote_nodes (
            mirror TEXT NOT NULL,
            uid TEXT NOT NULL,
            node_json TEXT NOT NULL,
            PRIMARY KEY (mirror, uid)
        );
        ",
    )?;
    Ok(connection)
}

pub(crate) fn file_state(
    connection: &Connection,
    mirror: &str,
    path: &str,
) -> Result<Option<FileState>> {
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

pub(crate) fn all_file_states(
    connection: &Connection,
    mirror: &str,
) -> Result<HashMap<String, FileState>> {
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

#[derive(Debug)]
struct FileCheckpoint {
    mirror: String,
    path: String,
    size: u64,
    mtime_ns: i64,
    sha1: String,
}

pub(crate) struct CheckpointBatch<'connection> {
    connection: &'connection Connection,
    pending: Vec<FileCheckpoint>,
    #[cfg(test)]
    pub(crate) commits: usize,
}

impl<'connection> CheckpointBatch<'connection> {
    pub(crate) fn new(connection: &'connection Connection) -> Self {
        Self {
            connection,
            pending: Vec::with_capacity(CHECKPOINT_BATCH_SIZE),
            #[cfg(test)]
            commits: 0,
        }
    }

    pub(crate) fn push(
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

    pub(crate) fn flush(&mut self) -> Result<()> {
        self.flush_batch(None)
    }

    pub(crate) fn flush_with_remote_nodes(
        &mut self,
        mirror: &str,
        nodes: &[RemoteNode],
    ) -> Result<()> {
        self.flush_batch(Some((mirror, nodes)))
    }

    fn flush_batch(&mut self, remote: Option<(&str, &[RemoteNode])>) -> Result<()> {
        if self.pending.is_empty() && remote.is_none_or(|(_, nodes)| nodes.is_empty()) {
            return Ok(());
        }
        let transaction = self.connection.unchecked_transaction()?;
        {
            let mut statement = transaction.prepare(
                "
                INSERT INTO files (mirror, path, size, mtime_ns, sha1)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT (mirror, path) DO UPDATE SET
                    size = excluded.size,
                    mtime_ns = excluded.mtime_ns,
                    sha1 = excluded.sha1
                WHERE files.size != excluded.size
                    OR files.mtime_ns != excluded.mtime_ns
                    OR files.sha1 != excluded.sha1
                ",
            )?;
            for checkpoint in &self.pending {
                statement.execute(params![
                    checkpoint.mirror,
                    checkpoint.path,
                    checkpoint.size,
                    checkpoint.mtime_ns,
                    checkpoint.sha1,
                ])?;
            }
        }
        if let Some((mirror, nodes)) = remote {
            let mut statement = transaction.prepare(SAVE_REMOTE_NODE)?;
            for node in nodes {
                statement.execute(params![mirror, node.uid, serde_json::to_string(node)?])?;
            }
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

pub(crate) fn delete_file_state(connection: &Connection, mirror: &str, path: &str) -> Result<()> {
    delete_file_states_and_remote_nodes(connection, mirror, &[path.to_owned()], &[])
}

pub(crate) fn delete_file_states_and_remote_nodes(
    connection: &Connection,
    mirror: &str,
    paths: &[String],
    uids: &[String],
) -> Result<()> {
    if paths.is_empty() && uids.is_empty() {
        return Ok(());
    }
    let transaction = connection.unchecked_transaction()?;
    {
        let mut statement =
            transaction.prepare("DELETE FROM files WHERE mirror = ?1 AND path = ?2")?;
        for path in paths {
            statement.execute(params![mirror, path])?;
        }
        let mut statement =
            transaction.prepare("DELETE FROM remote_nodes WHERE mirror = ?1 AND uid = ?2")?;
        for uid in uids {
            statement.execute(params![mirror, uid])?;
        }
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn bind_sync(
    connection: &Connection,
    sync: &SyncConfig,
    root_uid: Option<&str>,
) -> Result<bool> {
    let local_root = fs::canonicalize(&sync.local)
        .or_else(|_| std::path::absolute(&sync.local))?
        .to_string_lossy()
        .into_owned();
    let transaction = connection.unchecked_transaction()?;
    let previous = transaction
        .query_row(
            "SELECT local_root, remote_root, root_uid FROM sync_identities WHERE mirror = ?1",
            [&sync.name],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    let changed = previous.as_ref().is_some_and(|(local, remote, uid)| {
        local != &local_root
            || remote != &sync.remote
            || uid
                .as_deref()
                .zip(root_uid)
                .is_some_and(|(previous_uid, current_uid)| previous_uid != current_uid)
    });
    if changed {
        transaction.execute("DELETE FROM files WHERE mirror = ?1", [&sync.name])?;
        transaction.execute(
            "DELETE FROM metadata WHERE key IN (?1, ?2, ?3)",
            params![
                format!("baseline:{}", sync.name),
                format!("remote-excludes:{}", sync.name),
                format!("remote-scan:{}", sync.name)
            ],
        )?;
        transaction.execute("DELETE FROM remote_nodes WHERE mirror = ?1", [&sync.name])?;
        transaction.execute(
            "DELETE FROM remote_snapshots WHERE mirror = ?1",
            [&sync.name],
        )?;
    }
    let retained_uid = previous
        .as_ref()
        .and_then(|(_, _, uid)| uid.as_deref())
        .filter(|_| !changed);
    transaction.execute(
        "
        INSERT INTO sync_identities (mirror, local_root, remote_root, root_uid)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT (mirror) DO UPDATE SET
            local_root = excluded.local_root,
            remote_root = excluded.remote_root,
            root_uid = excluded.root_uid
        WHERE sync_identities.local_root != excluded.local_root
            OR sync_identities.remote_root != excluded.remote_root
            OR sync_identities.root_uid IS NOT excluded.root_uid
        ",
        params![
            sync.name,
            local_root,
            sync.remote,
            root_uid.or(retained_uid)
        ],
    )?;
    transaction.commit()?;
    Ok(changed)
}

pub(crate) fn remote_snapshot(
    connection: &Connection,
    mirror: &str,
) -> Result<Option<RemoteSnapshot>> {
    let snapshot = connection
        .query_row(
            "SELECT root_uid, scope_id, cursor FROM remote_snapshots WHERE mirror = ?1",
            [mirror],
            |row| {
                Ok(RemoteSnapshot {
                    root_uid: row.get(0)?,
                    scope_id: row.get(1)?,
                    cursor: row.get(2)?,
                    nodes: HashMap::new(),
                })
            },
        )
        .optional()?;
    let Some(mut snapshot) = snapshot else {
        return Ok(None);
    };
    let mut statement =
        connection.prepare("SELECT uid, node_json FROM remote_nodes WHERE mirror = ?1")?;
    let rows = statement.query_map([mirror], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (uid, json) = row?;
        let node = serde_json::from_str(&json)
            .with_context(|| format!("failed to read cached remote node {uid}"))?;
        snapshot.nodes.insert(uid, node);
    }
    Ok(Some(snapshot))
}

const SAVE_REMOTE_NODE: &str = "
    INSERT INTO remote_nodes (mirror, uid, node_json) VALUES (?1, ?2, ?3)
    ON CONFLICT (mirror, uid) DO UPDATE SET node_json = excluded.node_json
    WHERE remote_nodes.node_json != excluded.node_json
";

pub(crate) fn replace_remote_snapshot(
    connection: &Connection,
    mirror: &str,
    snapshot: &RemoteSnapshot,
) -> Result<()> {
    let transaction = connection.unchecked_transaction()?;
    {
        let mut statement =
            transaction.prepare("SELECT uid FROM remote_nodes WHERE mirror = ?1")?;
        let rows = statement.query_map([mirror], |row| row.get::<_, String>(0))?;
        let previous_uids = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut save = transaction.prepare(SAVE_REMOTE_NODE)?;
        for (uid, node) in &snapshot.nodes {
            save.execute(params![mirror, uid, serde_json::to_string(node)?])?;
        }
        let mut delete =
            transaction.prepare("DELETE FROM remote_nodes WHERE mirror = ?1 AND uid = ?2")?;
        for uid in previous_uids {
            if !snapshot.nodes.contains_key(&uid) {
                delete.execute(params![mirror, uid])?;
            }
        }
    }
    transaction.execute(
        "
        INSERT INTO remote_snapshots (mirror, root_uid, scope_id, cursor)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT (mirror) DO UPDATE SET
            root_uid = excluded.root_uid,
            scope_id = excluded.scope_id,
            cursor = excluded.cursor
        WHERE remote_snapshots.root_uid != excluded.root_uid
            OR remote_snapshots.scope_id != excluded.scope_id
            OR remote_snapshots.cursor != excluded.cursor
        ",
        params![
            mirror,
            snapshot.root_uid,
            snapshot.scope_id,
            snapshot.cursor
        ],
    )?;
    clear_remote_scan(&transaction, mirror)?;
    transaction.commit()?;
    Ok(())
}

pub(crate) fn remote_scan(connection: &Connection, mirror: &str) -> Result<Option<RemoteScan>> {
    metadata_value(connection, &format!("remote-scan:{mirror}"))?
        .map(|value| serde_json::from_str(&value).context("failed to read unfinished remote scan"))
        .transpose()
}

pub(crate) fn save_remote_scan(
    connection: &Connection,
    mirror: &str,
    scan: &RemoteScan,
) -> Result<()> {
    set_metadata(
        connection,
        &format!("remote-scan:{mirror}"),
        &serde_json::to_string(scan)?,
    )
}

pub(crate) fn clear_remote_scan(connection: &Connection, mirror: &str) -> Result<()> {
    connection.execute(
        "DELETE FROM metadata WHERE key = ?1",
        [format!("remote-scan:{mirror}")],
    )?;
    Ok(())
}

pub(crate) fn stale_paths(
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

pub(crate) fn metadata_value(connection: &Connection, key: &str) -> Result<Option<String>> {
    connection
        .query_row("SELECT value FROM metadata WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(Into::into)
}

pub(crate) fn set_metadata(connection: &Connection, key: &str, value: &str) -> Result<()> {
    connection.execute(
        "
        INSERT INTO metadata (key, value) VALUES (?1, ?2)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value
        WHERE metadata.value != excluded.value
        ",
        params![key, value],
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
