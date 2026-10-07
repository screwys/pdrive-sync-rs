# pdrive-sync

`pdrive-sync` syncs local folders through the official Proton Drive SDK.
A bundled SDK helper handles authentication, encryption, caching, remote events,
and transfers. The Rust service applies folder sync and conflict policies.
It reuses the Proton Drive CLI's login from the configured credentials store.

It supports local-to-remote push, remote-to-local pull, and two-way sync.
Deletion is opt-in: `delete = "trash"` moves removed files to Proton Drive
Trash or the local desktop Trash. It never empties either Trash.

| Mode | Normal changes | `delete = "trash"` |
| --- | --- | --- |
| `push` | Local is authoritative; new and changed local files upload. | Local deletions move the matching remote files to Proton Drive Trash. |
| `pull` | Proton Drive is authoritative; new and changed remote files download. | Remote deletions move the matching local files to the desktop Trash. |
| `two-way` | A change on one side copies to the unchanged side. If both changed, `conflict` decides. | A deletion propagates only when the other side is unchanged; delete/change combinations are conflicts. |

With `delete = "keep"`, files missing from one side are copied back in
two-way mode and left alone in one-way modes.

## Install

If you have already signed in with the Proton Drive CLI, keep that login.
Otherwise, use the [official CLI](https://proton.me/support/drive-cli) to sign in:

```sh
proton-drive auth login
```

Then install the sync service:

```sh
curl -fsSL https://raw.githubusercontent.com/screwys/pdrive-sync-rs/main/install.sh | sh
```

The installer puts `pdrive-sync` and `pdrive-sync-sdk` in `~/.local/bin`, opens the interactive
configuration, and installs and starts `pdrive-sync.service`. It detects a
systemd, dinit, or OpenRC user service manager automatically. Installation uses
the release binaries and does not require Rust, Cargo, or Bun.

For a restore that supplies its own saved configuration and services, run
`sh install.sh --no-setup`. This installs the executable without interactive
setup or starting a service.

Use `pdrive-sync restart` to restart the installed service, or
`pdrive-sync update` to replace the current executable with the latest release.

systemd uses a oneshot service and timer. dinit and OpenRC supervise the
built-in interval loop from `~/.config/dinit.d/pdrive-sync` or
`~/.config/rc/init.d/pdrive-sync`. Force detection when needed:

```sh
pdrive-sync install --init dinit
pdrive-sync status --init dinit
pdrive-sync restart --init dinit
pdrive-sync uninstall --init dinit
```

## Configuration

The default file is `~/.config/pdrive-sync/config.toml`:

```toml
[[sync]]
name = "documents"
mode = "push"
local = "/home/me/Documents"
remote = "/my-files/Documents"
delete = "trash"
```

Add more `[[sync]]` entries as needed. Two-way conflicts default to `fail`,
which plans every action first and changes nothing when a conflict exists.
`local-wins` and `remote-wins` resolve them in the named direction.
`ready_marker = ".sync-ready"` can guard a removable source from being
mistaken for an empty folder. See
[`config.example.toml`](config.example.toml) for every operation.
`exclude = ["private/**", "*.tmp"]` leaves matching paths untouched on both
sides, including when deletion is enabled.

After a failed attempt, a desktop notification is sent when no sync has
completed successfully for 24 hours. Repeated notifications are limited to
once per 24 hours. Set `notifications = false` in the configuration to disable
them for the service, or pass `sync --no-notifications` for one run.

Run selected entries with `pdrive-sync sync documents photos`, or describe a
safe one-off sync with `--local`, `--remote`, `--mode`, and `--delete`.
`pdrive-sync config validate` checks the file.

## Build

Install Rust and Bun 1.4.2, then run:

```sh
sdk/build.sh
cargo build --locked
```
## Behavior

Remote discovery uses node IDs. The first run inventories the included remote
folders; later runs read SDK events from a saved cursor and update that inventory.
The inventory and cursor commit together. A server refresh request or changed
exclusions causes a new inventory. Excluded folders are skipped during traversal.

Push checks local metadata and the last accepted remote checksum. Changed files
upload in bounded batches. The SDK hashes and encrypts their content and skips
files that already match. Its upload receipts supply the accepted checksum and
revision without another Rust hashing pass. Successful items are checkpointed
even when other items in the same batch fail. Cleanup starts after all uploads
succeed, and push checks the local file list again before cleanup after uploads.

Two-way sync hashes changed local files and compares both sides with the last
successful checkpoint. The helper checks remote revisions before writes, and
the service checks for local edits before downloads replace a file or local
files move to Trash. Downloads verify their size and SHA-1 while the SDK writes
the staged content, then move into place.

Runs using the same state database wait for each other. State records the local
and remote roots so changing a configured destination starts a new baseline.

Symlinks and non-UTF-8 names are skipped or rejected. Empty directories are not reproduced.

## License

Licensed under the MIT License.
