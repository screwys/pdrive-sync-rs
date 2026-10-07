#!/usr/bin/env sh
# SPDX-License-Identifier: MIT
set -eu

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
revision=df3a717bb8123791abf115a1dd171cb66813e9f4
sdk_version=0.22.2
app_version="$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$root/Cargo.toml")"
source_dir="$root/target/proton-sdk-$revision"
output_dir="${SDK_OUTPUT_DIR:-$root/target/debug}"
bun="${BUN:-bun}"
target="${1:-bun}"
case "$target" in
    bun) runtime="$("$bun" -p 'process.execPath')" ;;
    bun-linux-x64-baseline) runtime_package=bun-linux-x64-baseline ;;
    bun-linux-arm64) runtime_package=bun-linux-aarch64 ;;
    *) printf 'pdrive-sync-sdk: unsupported build target: %s\n' "$target" >&2; exit 1 ;;
esac

if [ "$("$bun" --version)" != 1.4.2 ]; then
    printf 'pdrive-sync-sdk: Bun 1.4.2 is required\n' >&2
    exit 1
fi

mkdir -p "$root/target" "$output_dir"
output_dir="$(CDPATH= cd -- "$output_dir" && pwd)"
if [ "$target" != bun ]; then
    runtime_dir="$root/target/$runtime_package-1.4.2"
    runtime="$runtime_dir/package/bin/bun"
    if [ ! -x "$runtime" ]; then
        mkdir -p "$runtime_dir"
        curl -fsSL "https://registry.npmjs.org/@oven/$runtime_package/-/$runtime_package-1.4.2.tgz" -o "$runtime_dir/runtime.tar.gz"
        tar -xzf "$runtime_dir/runtime.tar.gz" -C "$runtime_dir"
        rm "$runtime_dir/runtime.tar.gz"
    fi
fi
if [ ! -f "$source_dir/cli/bun.lock" ]; then
    archive="$(mktemp "$root/target/proton-sdk.XXXXXX.tar.gz")"
    unpack="$(mktemp -d "$root/target/proton-sdk.XXXXXX")"
    trap 'rm -f "$archive"; rm -rf "$unpack"' EXIT HUP INT TERM
    curl -fsSL "https://github.com/ProtonDriveApps/sdk/archive/$revision.tar.gz" -o "$archive"
    tar -xzf "$archive" -C "$unpack" --strip-components=1
    mv "$unpack" "$source_dir"
    rm -f "$archive"
    trap - EXIT HUP INT TERM
fi

cp "$root/sdk/pdrive-sync-sdk.ts" "$source_dir/cli/src/pdrive-sync-sdk.ts"
cp "$root/sdk/key-lifetime.ts" "$source_dir/cli/src/key-lifetime.ts"
dependencies="$root/target/sdk-build-dependencies"
mkdir -p "$dependencies"
cp "$root/sdk/build-dependencies/package.json" "$root/sdk/build-dependencies/bun.lock" "$dependencies/"
cd "$dependencies"
"$bun" install --frozen-lockfile --cache-dir "$root/target/bun-install-cache"
cd "$source_dir/cli"
"$bun" install --frozen-lockfile --cache-dir "$root/target/bun-install-cache"
mkdir -p node_modules/@boltffi
ln -snf "$dependencies/node_modules/@boltffi/runtime" node_modules/@boltffi/runtime
ln -snf "$dependencies/node_modules/comlink" node_modules/comlink
ln -snf cli/node_modules "$source_dir/node_modules"
"$bun" node_modules/typescript/bin/tsc --noEmit --incremental false
CLI_APP_VERSION_NAME=external-drive-pdrive_sync \
CLI_VERSION="$app_version+$revision" JS_VERSION="$sdk_version+$revision" \
CLI_OUTPUT_DIR="$output_dir" \
    "$bun" scripts/build-cli.mjs src/pdrive-sync-sdk.ts bun --bundle-only
"$bun" build --compile --bytecode --target="$target" --format=esm --minify --sourcemap=inline \
    --compile-executable-path="$runtime" "$output_dir/pdrive-sync-sdk.js" --outfile="$output_dir/pdrive-sync-sdk"
rm "$output_dir/pdrive-sync-sdk.js"
cp "$source_dir/LICENSE.md" "$output_dir/PROTON-SDK-LICENSE.md"
