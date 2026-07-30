#!/bin/sh
# build-ohos.sh - macOS/Linux counterpart of build-ohos.ps1
#
# .cargo/config.toml hard-codes the Windows toolchain (D:\Huawei\..., *.cmd).
# Environment variables take precedence over config.toml, so we point cargo and
# cc-rs at the local SDK here instead of forking the config file.
#
# AR_* matters most: without it cc-rs falls back to macOS /usr/bin/ar (BSD ar),
# which silently produces an archive with no members. ring's assembly objects
# then never make it into libcloudreve.so, every ring_core_* symbol stays
# undefined, and because the .so is linked BIND_NOW the app crashes the moment
# the N-API module is dlopen'd.
set -e

SDK_NATIVE="${OHOS_SDK_NATIVE:-/Applications/DevEco-Studio.app/Contents/sdk/default/openharmony/native}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -d "$SDK_NATIVE" ]; then
  echo "OpenHarmony native SDK not found at: $SDK_NATIVE" >&2
  echo "Set OHOS_SDK_NATIVE to the correct path and retry." >&2
  exit 1
fi

export OHOS_SDK_NATIVE="$SDK_NATIVE"
export AR_aarch64_unknown_linux_ohos="$SDK_NATIVE/llvm/bin/llvm-ar"
export AR_x86_64_unknown_linux_ohos="$SDK_NATIVE/llvm/bin/llvm-ar"
export CC_aarch64_unknown_linux_ohos="$SCRIPT_DIR/scripts/ohos-clang-aarch64.sh"
export CC_x86_64_unknown_linux_ohos="$SCRIPT_DIR/scripts/ohos-clang-x86_64.sh"
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_OHOS_LINKER="$SCRIPT_DIR/scripts/ohos-clang-aarch64.sh"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_OHOS_LINKER="$SCRIPT_DIR/scripts/ohos-clang-x86_64.sh"

chmod +x "$SCRIPT_DIR"/scripts/ohos-clang-*.sh

cd "$SCRIPT_DIR"

for pair in "aarch64-unknown-linux-ohos:arm64-v8a" "x86_64-unknown-linux-ohos:x86_64"; do
  target="${pair%%:*}"
  abi="${pair##*:}"
  echo "Building $abi ($target)..."
  cargo build --target "$target" --release

  so="target/$target/release/libcloudreve.so"
  # Guard against the silent-empty-archive failure described above.
  undef=$("$SDK_NATIVE/llvm/bin/llvm-readelf" --dyn-syms "$so" | grep -c 'UND .*ring_core_' || true)
  if [ "$undef" -ne 0 ]; then
    echo "ERROR: $so has $undef undefined ring_core_* symbols; ring's objects were not linked." >&2
    echo "The app would crash on dlopen. Aborting instead of shipping a broken .so." >&2
    exit 1
  fi

  cp -f "$so" "$PROJECT_ROOT/entry/libs/$abi/libcloudreve.so"
done

echo "Done. .so files placed in entry/libs/"
