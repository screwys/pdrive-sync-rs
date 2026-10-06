// SPDX-License-Identifier: MIT

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResultValue<T> {
    pub ok: bool,
    pub value: Option<T>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteNode {
    pub uid: String,
    #[serde(default)]
    pub parent_uid: Option<String>,
    #[serde(default)]
    pub tree_event_scope_id: Option<String>,
    pub name: ResultValue<String>,
    #[serde(rename = "type")]
    pub kind: String,
    pub total_storage_size: Option<u64>,
    pub active_revision: Option<RemoteRevision>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteRevision {
    pub uid: String,
    pub storage_size: Option<u64>,
    pub claimed_size: Option<u64>,
    pub claimed_modification_time: Option<String>,
    pub claimed_digests: Option<RemoteDigests>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDigests {
    pub sha1: Option<String>,
    pub sha1_verified: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct RemoteFile {
    pub uid: String,
    pub revision_uid: String,
    pub sha1: String,
    pub claimed_size: u64,
}

#[derive(Clone, Debug)]
pub struct RemoteParent {
    pub uid: String,
    pub root: String,
    pub components: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct LocalUpload {
    pub path: PathBuf,
    pub sha1: Option<String>,
    pub expected_remote: Option<RemoteVersion>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RemoteVersion {
    Absent,
    Revision {
        uid: String,
        #[serde(rename = "revisionUid")]
        revision_uid: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct UploadFailure {
    pub name: String,
    pub error: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadBatchResult {
    pub transferred_items: usize,
    pub skipped_items: usize,
    pub transferred_bytes: u64,
    pub failures: Vec<UploadFailure>,
    #[serde(default)]
    pub nodes: Vec<RemoteNode>,
}

#[derive(Clone, Debug)]
pub struct TrashTarget {
    pub parent: RemoteParent,
    pub name: String,
    pub uid: String,
    pub revision_uid: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrashBatchResult {
    pub succeeded_uids: Vec<String>,
    pub failed_uids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteEvent {
    #[serde(rename = "type")]
    pub kind: String,
    pub node_uid: Option<String>,
    pub parent_node_uid: Option<String>,
    pub is_trashed: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RemoteEvents {
    pub cursor: String,
    pub events: Vec<RemoteEvent>,
    pub refresh: bool,
    pub removed: bool,
}

pub trait DriveClient {
    fn reset_cache(&mut self) -> Result<()>;
    fn child(&mut self, parent_uid: &str, name: &str) -> Result<Option<RemoteNode>>;
    fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>>;
    fn info(&mut self, remote_path: &str) -> Result<Option<RemoteNode>>;
    fn events(&mut self, scope: &str, cursor: Option<&str>) -> Result<RemoteEvents>;
    fn create_folder(&mut self, parent: &RemoteParent, name: &str) -> Result<RemoteNode>;
    fn upload_many(
        &mut self,
        files: &[LocalUpload],
        remote_parent: &RemoteParent,
    ) -> Result<UploadBatchResult>;
    fn download(&mut self, remote: &RemoteFile, local_parent: &Path) -> Result<()>;
    fn trash_many(&mut self, targets: &[TrashTarget]) -> Result<TrashBatchResult>;
}

pub struct SdkDrive {
    binary: PathBuf,
    session: Option<SdkSession>,
}

impl SdkDrive {
    pub fn new(binary: PathBuf) -> Self {
        Self {
            binary,
            session: None,
        }
    }

    fn request<T: DeserializeOwned>(&mut self, request: serde_json::Value) -> Result<T> {
        if self.session.is_none() {
            self.session = Some(SdkSession::start(&self.binary)?);
        }
        self.session
            .as_mut()
            .expect("session was initialized")
            .request(request)
    }
}

impl DriveClient for SdkDrive {
    fn reset_cache(&mut self) -> Result<()> {
        self.request(json!({"method":"reset_cache"}))
    }

    fn child(&mut self, parent_uid: &str, name: &str) -> Result<Option<RemoteNode>> {
        self.request(json!({"method":"child", "parent":parent_uid, "name":name}))
    }

    fn list(&mut self, remote_path: &str) -> Result<Vec<RemoteNode>> {
        self.request(json!({"method":"list", "path":remote_path}))
    }

    fn info(&mut self, remote_path: &str) -> Result<Option<RemoteNode>> {
        self.request(json!({"method":"info", "path":remote_path}))
    }

    fn events(&mut self, scope: &str, cursor: Option<&str>) -> Result<RemoteEvents> {
        self.request(json!({"method": "events", "scope": scope, "cursor": cursor}))
    }

    fn create_folder(&mut self, parent: &RemoteParent, name: &str) -> Result<RemoteNode> {
        self.request(json!({"method":"create_folder", "parent":parent.uid, "root":parent.root, "parentComponents":parent.components, "name":name}))
    }

    fn upload_many(
        &mut self,
        files: &[LocalUpload],
        remote_parent: &RemoteParent,
    ) -> Result<UploadBatchResult> {
        let parent = remote_parent;
        let files = files
            .iter()
            .map(|file| json!({"path": file.path, "sha1": file.sha1, "expectedRemote": file.expected_remote}))
            .collect::<Vec<_>>();
        self.request(json!({"method": "upload", "parent": parent.uid, "root":parent.root, "parentComponents":parent.components, "files": files}))
    }

    fn download(&mut self, remote: &RemoteFile, local_parent: &Path) -> Result<()> {
        self.request(
            json!({"method": "download", "path": remote.uid, "revisionUid": remote.revision_uid,
            "parent": local_parent, "sha1": remote.sha1, "size": remote.claimed_size}),
        )
    }

    fn trash_many(&mut self, targets: &[TrashTarget]) -> Result<TrashBatchResult> {
        let targets = targets
            .iter()
            .map(|target| json!({"uid": target.uid, "revisionUid":target.revision_uid, "parentUid":target.parent.uid, "name":target.name, "root":target.parent.root, "parentComponents":target.parent.components}))
            .collect::<Vec<_>>();
        self.request(json!({"method": "trash", "targets": targets}))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SdkResponse<T> {
    Value(T),
    Error(String),
}

struct SdkSession {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl SdkSession {
    fn start(binary: &Path) -> Result<Self> {
        let mut child = Command::new(binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| format!("failed to start SDK helper {}", binary.display()))?;
        let input = child.stdin.take().context("SDK helper has no stdin")?;
        let stdout = child.stdout.take().context("SDK helper has no stdout")?;
        Ok(Self {
            child,
            input,
            output: BufReader::new(stdout),
        })
    }

    fn request<T: DeserializeOwned>(&mut self, request: serde_json::Value) -> Result<T> {
        serde_json::to_writer(&mut self.input, &request)?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;
        let mut response = String::new();
        if self.output.read_line(&mut response)? == 0 {
            bail!("SDK helper closed unexpectedly");
        }
        match serde_json::from_str::<SdkResponse<T>>(&response)
            .context("invalid SDK helper response")?
        {
            SdkResponse::Value(value) => Ok(value),
            SdkResponse::Error(error) => bail!("Proton Drive SDK: {error}"),
        }
    }
}

impl Drop for SdkSession {
    fn drop(&mut self) {
        let _ = self.input.write_all(b"{\"method\":\"exit\"}\n");
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
