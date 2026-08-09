// SPDX-License-Identifier: MIT

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

pub(crate) fn delete_file_state(connection: &Connection, mirror: &str, path: &str) -> Result<()> {
    connection.execute(
        "DELETE FROM files WHERE mirror = ?1 AND path = ?2",
        params![mirror, path],
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
        ",
        params![key, value],
    )?;
    Ok(())
}

pub(crate) fn replace_remote_directories(
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

pub(crate) fn remote_directory_known(
    connection: &Connection,
    mirror: &str,
    path: &str,
) -> Result<bool> {
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

pub(crate) fn save_remote_directory(
    connection: &Connection,
    mirror: &str,
    path: &str,
) -> Result<()> {
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
