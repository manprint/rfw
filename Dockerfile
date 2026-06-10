# ============================================================
# Dockerfile — Multi-stage production image for rfw
# ============================================================

# ── Stage 1: Build ──────────────────────────────────────────
FROM rust:latest AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN cargo build --release && \
    strip target/release/rfw

# ── Stage 2: Minimal runtime image ──────────────────────────
FROM gcr.io/distroless/cc-debian12:latest

LABEL org.opencontainers.image.title="rfw"
LABEL org.opencontainers.image.description="Rust Forwarder — TCP port forwarder"
LABEL org.opencontainers.image.source="https://github.com/manprint/rfw"
LABEL org.opencontainers.image.licenses="MIT"

COPY --from=builder /build/target/release/rfw /usr/local/bin/rfw

ENTRYPOINT ["rfw"]
CMD ["--help"]
