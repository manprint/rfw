#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# rfw — Cross-platform build script via Docker
# Builds for Linux, macOS, Windows, Android (arm64)
#
# Usage:
#   ./build.sh          # Build all 7 targets
#   ./build.sh linux    # Linux only
#   ./build.sh macos    # macOS only
#   ./build.sh windows  # Windows only
#   ./build.sh android  # Android only
# ============================================================

HERE="$(cd "$(dirname "$0")" && pwd)"
DIST="${HERE}/dist"
mkdir -p "${DIST}"

FILTER="${1:-all}"

if command -v rustc >/dev/null 2>&1; then
    DEFAULT_RUST_VERSION="$(rustc --version | awk 'NR==1 { print $2 }')"
else
    DEFAULT_RUST_VERSION="latest"
fi
RUST_VERSION="${RUST_VERSION:-${DEFAULT_RUST_VERSION}}"

if [ -n "${DOCKER_NETWORK:-}" ]; then
    BUILDER_NETWORK="${DOCKER_NETWORK}"
elif [ "$(uname -s)" = "Linux" ]; then
    BUILDER_NETWORK="host"
else
    BUILDER_NETWORK="default"
fi

retry_command() {
    local max_attempts="$1"
    shift

    local attempt=1
    local exit_code=0
    while true; do
        if "$@"; then
            return 0
        else
            exit_code=$?
        fi
        if [ "$attempt" -ge "$max_attempts" ]; then
            return "$exit_code"
        fi

        echo "    attempt ${attempt}/${max_attempts} failed with exit ${exit_code}; retrying..."
        sleep $((attempt * 5))
        attempt=$((attempt + 1))
    done
}

# Target matrix: (triple|output-name|linker-env-var|extra-rustflags|build-kind|tool-target)
# build-kind: cargo | zig | ndk
TARGETS=()

case "$FILTER" in
    all|linux)
        TARGETS+=(
            "x86_64-unknown-linux-gnu|rfw-linux-amd64|||cargo"
            "aarch64-unknown-linux-gnu|rfw-linux-arm64|aarch64-linux-gnu-gcc|-C link-arg=-fuse-ld=bfd|cargo"
        ) ;;
esac
case "$FILTER" in
    all|macos)
        TARGETS+=(
            "x86_64-apple-darwin|rfw-macos-amd64|||zig"
            "aarch64-apple-darwin|rfw-macos-arm64|||zig"
        ) ;;
esac
case "$FILTER" in
    all|windows)
        TARGETS+=(
            "x86_64-pc-windows-gnu|rfw-windows-amd64.exe|||cargo"
            "aarch64-pc-windows-gnullvm|rfw-windows-arm64.exe|||zig"
        ) ;;
esac
case "$FILTER" in
    all|android)
        TARGETS+=(
            "aarch64-linux-android|rfw-android-arm64|||ndk|arm64-v8a"
        ) ;;
esac

if [ ${#TARGETS[@]} -eq 0 ]; then
    echo "Unknown filter: $FILTER"
    echo "Usage: $0 [all|linux|macos|windows|android]"
    exit 1
fi

RUST_TARGETS=()
NEEDS_LINUX_AARCH64=0
NEEDS_WINDOWS_GNU=0
NEEDS_ZIG=0
NEEDS_ANDROID=0
NEEDS_MACOS_SDK=0

for entry in "${TARGETS[@]}"; do
    IFS='|' read -r TARGET _OUTPUT _LINKER _RUSTFLAGS BUILD_KIND TOOL_TARGET <<< "${entry}"
    RUST_TARGETS+=("${TARGET}")

    case "${TARGET}" in
        aarch64-unknown-linux-gnu)
            NEEDS_LINUX_AARCH64=1
            ;;
        x86_64-pc-windows-gnu)
            NEEDS_WINDOWS_GNU=1
            ;;
        *apple-darwin)
            NEEDS_MACOS_SDK=1
            ;;
    esac

    case "${BUILD_KIND}" in
        zig)
            NEEDS_ZIG=1
            ;;
        ndk)
            NEEDS_ANDROID=1
            ;;
    esac
done

echo "==> rfw cross-platform build (filter: $FILTER)"
echo "    Output: ${DIST}"
echo "    Builder Rust: ${RUST_VERSION}"
echo "    Docker network: ${BUILDER_NETWORK}"
echo ""

FAILED=0

# Build Docker image
echo "==> Building Docker build image..."
retry_command 3 \
    docker build \
    --network "${BUILDER_NETWORK}" \
    --file "${HERE}/Dockerfile.build" \
    --tag rfw-builder \
    --build-arg "RUST_VERSION=${RUST_VERSION}" \
    --build-arg "RUST_TARGETS=${RUST_TARGETS[*]}" \
    --build-arg "INSTALL_LINUX_AARCH64=${NEEDS_LINUX_AARCH64}" \
    --build-arg "INSTALL_WINDOWS_GNU=${NEEDS_WINDOWS_GNU}" \
    --build-arg "INSTALL_ZIG=${NEEDS_ZIG}" \
    --build-arg "INSTALL_ANDROID=${NEEDS_ANDROID}" \
    --build-arg "INSTALL_MACOS_SDK=${NEEDS_MACOS_SDK}" \
    "${HERE}"

# Build each target
for entry in "${TARGETS[@]}"; do
    IFS='|' read -r TARGET OUTPUT LINKER RUSTFLAGS BUILD_KIND TOOL_TARGET <<< "${entry}"
    echo ""
    echo "==> Building ${TARGET}  →  ${OUTPUT}"

    # Build command
    if [ "$BUILD_KIND" = zig ]; then
        BUILD_CMD="cargo zigbuild --release --target ${TARGET}"
    elif [ "$BUILD_KIND" = ndk ]; then
        BUILD_CMD="cargo ndk -t ${TOOL_TARGET:-${TARGET}} build --release"
    else
        BUILD_CMD="cargo build --release --target ${TARGET}"
    fi

    # Set target-specific env vars
    DOCKER_ARGS=()
    if [ -n "$LINKER" ]; then
        DOCKER_ARGS+=( -e "CARGO_TARGET_$(echo "$TARGET" | tr '[:lower:]-' '[:upper:]_')_LINKER=$LINKER" )
    fi
    if [ -n "$RUSTFLAGS" ]; then
        DOCKER_ARGS+=( -e "RUSTFLAGS=$RUSTFLAGS" )
    fi

    if docker run --rm \
        --network "${BUILDER_NETWORK}" \
        "${DOCKER_ARGS[@]}" \
        --volume "${HERE}:${HERE}" \
        --workdir "${HERE}" \
        rfw-builder \
        bash -c "$BUILD_CMD" 2>&1; then
        echo "    ✓ Compilation succeeded"
    else
        echo "    ✗ Compilation failed for ${TARGET}, skipping"
        FAILED=1
        continue
    fi

    # Copy binary to dist/
    case "${TARGET}" in
        *windows*) BIN="rfw.exe" ;;
        *)         BIN="rfw" ;;
    esac

    SRC="${HERE}/target/${TARGET}/release/${BIN}"
    if [ -f "$SRC" ]; then
        cp "$SRC" "${DIST}/${OUTPUT}"
        echo "    ✓ ${OUTPUT}  ($(du -h "${DIST}/${OUTPUT}" | cut -f1))"
    else
        echo "    ✗ Binary not found at ${SRC}"
        FAILED=1
    fi
done

echo ""
echo "==> Build complete. Files in ${DIST}:"
ls -lh "${DIST}/" 2>/dev/null || echo "    (none)"

exit "$FAILED"
