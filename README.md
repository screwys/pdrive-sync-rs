# pdrive-sync-rs

`pdrive-sync-rs` adds scheduled one-way and two-way folder sync to Proton
Drive's official Linux CLI. It is a sync layer, not another Proton client: it
does not implement authentication, store Proton credentials, or install the
official CLI.

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

First install the [official Proton Drive CLI](https://proton.me/support/drive-cli)
and log in with its own command:

```sh
proton-drive auth login
```

`proton-drive` must be on `PATH`, or `proton_drive_bin` must point to its
executable. Then install the sync wrapper:

```sh
curl -fsSL https://raw.githubusercontent.com/screwys/pdrive-sync-rs/main/install.sh | sh
pdrive-sync-rs setup
pdrive-sync-rs install
pdrive-sync-rs status
```

The installer puts `pdrive-sync-rs` in `~/.local/bin`. `install` detects a
systemd, dinit, or OpenRC user service manager and creates
`pdrive-sync.service`. Use `--interval 30m` to change the default one-hour
interval.

systemd uses a oneshot service and timer. dinit and OpenRC supervise the
built-in interval loop from `~/.config/dinit.d/pdrive-sync` or
`~/.config/rc/init.d/pdrive-sync`. Force detection when needed:

```sh
pdrive-sync-rs install --init dinit
pdrive-sync-rs status --init dinit
pdrive-sync-rs uninstall --init dinit
```

The systemd unit also applies a soft `MemoryHigh=512M` cache-reclaim boundary.
dinit can only join a cgroup created elsewhere, and OpenRC user services do not
create cgroups, so the installer does not substitute an unsafe hard memory
limit for those managers.

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

Run selected entries with `pdrive-sync-rs sync documents photos`, or describe a
safe one-off sync with `--local`, `--remote`, `--mode`, and `--delete`.
`pdrive-sync-rs config validate` checks the file.

## Behavior

The first push inventories the remote tree and hashes local files. Later push
runs use local checkpoints and compare Proton Drive's active-revision SHA-1
before replacing anything, so unchanged files are not uploaded again. A failed
transfer is never checkpointed, and deletions run only after transfers
succeed.

Symlinks and non-UTF-8 names are skipped or rejected rather than followed.
Empty directories are not reproduced. Pull and two-way modes inventory the
remote tree on each run because the CLI does not expose the SDK event stream.

## License

Copyright © 2026 screwys. Licensed under the GNU General Public License,
version 3 or any later version (`GPL-3.0-or-later`).
