#!/usr/bin/env sh
# SPDX-License-Identifier: GPL-3.0-or-later
set -eu

repository="${PDRIVE_SYNC_REPOSITORY:-screwys/pdrive-sync-rs}"
install_dir="${PDRIVE_SYNC_INSTALL_DIR:-$HOME/.local/bin}"
binary="$install_dir/pdrive-sync"
legacy_binary="$install_dir/pdrive-sync-rs"
replacement="$install_dir/.pdrive-sync.$$"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"; rm -f "$replacement"' EXIT HUP INT TERM

if [ -z "${PDRIVE_SYNC_CURRENT_VERSION:-}" ] && ! (: </dev/tty) 2>/dev/null; then
    printf 'pdrive-sync: installation needs an interactive terminal for setup\n' >&2
    exit 1
fi

case "$(uname -m)" in
    x86_64) platform="x86_64-linux-gnu" ;;
    aarch64 | arm64) platform="aarch64-linux-gnu" ;;
    *)
        printf 'pdrive-sync: unsupported architecture: %s\n' "$(uname -m)" >&2
        exit 1
        ;;
esac

release="https://github.com/$repository/releases/latest/download"
archive="pdrive-sync-rs-$platform.tar.gz"
if curl -fL "$release/$archive" -o "$temporary_dir/$archive"; then
    (
        cd "$temporary_dir"
        tar -xzf "$archive"
    )
    if [ -f "$temporary_dir/pdrive-sync" ]; then
        source_binary="$temporary_dir/pdrive-sync"
    else
        source_binary="$temporary_dir/pdrive-sync-rs"
    fi
elif command -v cargo >/dev/null 2>&1; then
    printf 'No release archive was found; building the current main branch with Cargo.\n'
    cargo install \
        --locked \
        --git "https://github.com/$repository" \
        --root "$temporary_dir/cargo"
    source_binary="$temporary_dir/cargo/bin/pdrive-sync"
else
    printf 'pdrive-sync: no release archive is available and Cargo is not installed\n' >&2
    exit 1
fi

if [ -n "${PDRIVE_SYNC_CURRENT_VERSION:-}" ]; then
    candidate_version="$("$source_binary" --version)"
    candidate_version="${candidate_version##* }"
    if [ "$candidate_version" = "$PDRIVE_SYNC_CURRENT_VERSION" ]; then
        printf 'pdrive-sync %s is already up to date\n' "$candidate_version"
        exit 0
    fi
fi

install -d "$install_dir"
install -m 0755 "$source_binary" "$replacement"
mv -f "$replacement" "$binary"

config_dir="${XDG_CONFIG_HOME:-$HOME/.config}"
systemd_service="$config_dir/systemd/user/pdrive-sync.service"
systemd_service_changed=false
for service_file in \
    "$systemd_service" \
    "$config_dir/dinit.d/pdrive-sync" \
    "$config_dir/rc/init.d/pdrive-sync"
do
    if [ -f "$service_file" ] && grep -q 'pdrive-sync-rs' "$service_file"; then
        sed --follow-symlinks -i 's/pdrive-sync-rs/pdrive-sync/g' "$service_file"
        if [ "$service_file" = "$systemd_service" ]; then
            systemd_service_changed=true
        fi
    fi
done
if [ "$systemd_service_changed" = true ] && command -v systemctl >/dev/null 2>&1; then
    if ! systemctl --user daemon-reload; then
        printf 'pdrive-sync: installed, but systemd could not reload the updated service file\n' >&2
    fi
fi

rm -f "$legacy_binary"

printf 'Installed %s\n' "$binary"
if [ -z "${PDRIVE_SYNC_CURRENT_VERSION:-}" ]; then
    "$binary" setup </dev/tty
    "$binary" install </dev/tty
fi
