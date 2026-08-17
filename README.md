# pdrive-sync

`pdrive-sync` is a simple and lightweight way to orchestrate the official Proton Drive CLI. It adds a scheduled one-way and two-way folder sync layer as a system service (systemd/dinit/openRC); it is not another Proton client and
does not implement authentication, or store Proton credentials.

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

First install the [official Proton Drive CLI](https://proton.me/support/drive-cli). This will be kept up-to-date with latest API until the official desktop app is here. 

```sh
proton-drive auth login
```

`proton-drive` must be on `PATH`, or `proton_drive_bin` must point to its
executable. Then install the sync wrapper:

```sh
curl -fsSL https://raw.githubusercontent.com/screwys/pdrive-sync-rs/main/install.sh | sh
```

The installer puts `pdrive-sync` in `~/.local/bin`, opens the interactive
configuration, and installs and starts `pdrive-sync.service`. It detects a
systemd, dinit, or OpenRC user service manager automatically.

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

The systemd unit also applies a soft `MemoryHigh=512M` cache-reclaim boundary.

## Configuration

The default file is `~/.config/pdrive-sync/config.toml`:

```toml
proton_drive_bin = "proton-drive"

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

## Behavior

Push scans only file metadata locally. New or changed files are sent to the
Proton Drive CLI in bounded batches; the CLI performs its required hashing and
automatically skips files whose remote content already matches. Each successful
batch item is checkpointed immediately, including when another item in the same
batch fails. Remote cleanup is also batched and begins only after every upload
batch succeeds.

Pull and two-way sync still verify downloaded content against Proton Drive's
SHA-1 metadata. A push checkpoint does not store a duplicate locally computed
SHA-1; if the same entry later changes to two-way mode, its digest is rebuilt
once before conflict planning.

Symlinks and non-UTF-8 names are skipped or rejected. Empty directories are not reproduced. Pull and two-way modes inventory the remote tree on each run because the CLI does not expose the SDK event stream.

## License

Licensed under the MIT License.
