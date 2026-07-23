#!/usr/bin/env sh
# SPDX-License-Identifier: GPL-3.0-or-later
set -eu

repository="${PDRIVE_SYNC_REPOSITORY:-screwys/pdrive-sync-rs}"
install_dir="${PDRIVE_SYNC_INSTALL_DIR:-$HOME/.local/bin}"
binary="$install_dir/pdrive-sync-rs"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT HUP INT TERM

case "$(uname -m)" in
    x86_64) target="x86_64-unknown-linux-gnu" ;;
    aarch64 | arm64) target="aarch64-unknown-linux-gnu" ;;
    *)
        printf 'pdrive-sync-rs: unsupported architecture: %s\n' "$(uname -m)" >&2
        exit 1
        ;;
esac

archive="pdrive-sync-rs-$target.tar.gz"
release="https://github.com/$repository/releases/latest/download"
if curl -fL "$release/$archive" -o "$temporary_dir/$archive" &&
    curl -fL "$release/$archive.sha256" -o "$temporary_dir/$archive.sha256"; then
    (
        cd "$temporary_dir"
        sha256sum -c "$archive.sha256"
        tar -xzf "$archive"
    )
    install -d "$install_dir"
    install -m 0755 "$temporary_dir/pdrive-sync-rs" "$binary"
elif command -v cargo >/dev/null 2>&1; then
    printf 'No release archive was found; building the current main branch with Cargo.\n'
    cargo install \
        --locked \
        --git "https://github.com/$repository" \
        --root "$temporary_dir/cargo"
    install -d "$install_dir"
    install -m 0755 "$temporary_dir/cargo/bin/pdrive-sync-rs" "$binary"
else
    printf 'pdrive-sync-rs: no release archive is available and Cargo is not installed\n' >&2
    exit 1
fi

printf 'Installed %s\n' "$binary"
printf 'Next: pdrive-sync-rs setup && pdrive-sync-rs install\n'
