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

# Target matrix: (triple|output-name|linker-env-var|extra-rustflags|build-kind)
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
            "aarch64-linux-android|rfw-android-arm64|||ndk"
        ) ;;
esac

if [ ${#TARGETS[@]} -eq 0 ]; then
    echo "Unknown filter: $FILTER"
    echo "Usage: $0 [all|linux|macos|windows|android]"
    exit 1
fi

echo "==> rfw cross-platform build (filter: $FILTER)"
echo "    Output: ${DIST}"
echo ""

FAILED=0

# Build Docker image
echo "==> Building Docker build image..."
docker build \
    --file "${HERE}/Dockerfile.build" \
    --tag rfw-builder \
    "${HERE}"

# Build each target
for entry in "${TARGETS[@]}"; do
    IFS='|' read -r TARGET OUTPUT LINKER RUSTFLAGS BUILD_KIND <<< "${entry}"
    echo ""
    echo "==> Building ${TARGET}  →  ${OUTPUT}"

    # Build command
    if [ "$BUILD_KIND" = zig ]; then
        BUILD_CMD="cargo zigbuild --release --target ${TARGET}"
    elif [ "$BUILD_KIND" = ndk ]; then
        BUILD_CMD="cargo ndk -t ${TARGET} build --release"
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
