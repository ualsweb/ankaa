# syntax=docker/dockerfile:1.7
# ANKAA server image, built from source. Multi-arch: build on the platform you run
# (linux/arm64 on Graviton, Axion, Cobalt, Apple silicon; linux/amd64 elsewhere).
#
#   docker build -t ankaa .
#   docker run --rm -e ANKAA_TOKEN=change-me -p 50051:50051 -p 9100:9100 -p 5432:5432 \
#     -v ./ankaad.toml:/etc/ankaa/ankaad.toml:ro -v ankaa-data:/var/lib/ankaa ankaa
#
# Release images are published to ghcr.io by .github/workflows/release.yml from the same file.

FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# rust-toolchain.toml pins the compiler; rustup installs it on first use
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked -p ankaad \
    && install -D -m 0755 target/release/ankaad /out/ankaad \
    && strip --strip-debug /out/ankaad \
    && install -d /out/data/wal /out/data/segments

FROM gcr.io/distroless/cc-debian12:nonroot
LABEL org.opencontainers.image.title="ANKAA" \
      org.opencontainers.image.description="Real-time time-series engine for Arm servers" \
      org.opencontainers.image.source="https://github.com/ualsweb/ankaa" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/ankaad /usr/local/bin/ankaad
COPY deploy/docker/ankaad.toml /etc/ankaa/ankaad.toml
COPY --from=build --chown=65532:65532 /out/data /var/lib/ankaa
# data (WAL and segments) lives in a volume owned by the nonroot user (uid 65532)
VOLUME ["/var/lib/ankaa"]
EXPOSE 50051 9100 5432
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/ankaad"]
CMD ["serve", "--config", "/etc/ankaa/ankaad.toml"]
