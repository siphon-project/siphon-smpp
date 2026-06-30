# syntax=docker/dockerfile:1
#
# Template Dockerfile for an SMSC built on siphon-smpp.
#
# siphon-smpp is a *library*: it plugs an `smpp` namespace + a tokio SMPP
# runtime into a siphon binary that YOU build. This image builds that
# binary (the `${SMSC_BIN}` crate in your workspace) and ships it with the
# CPython runtime pyo3 embeds and your `smpp.py` handler script.
#
# Replace `your-smsc` with your binary crate name and adjust paths to your
# repo layout. Nothing siphon-specific about the *composition* belongs in a
# Dockerfile — it's an ordinary `cargo build`.
#
#   docker build -f deploy/Dockerfile --build-arg SMSC_BIN=your-smsc -t your-registry/your-smsc:latest .

# ── Build stage ─────────────────────────────────────────────────────────
FROM rust:1-bookworm AS build
ARG SMSC_BIN=your-smsc

# pyo3 (auto-initialize) links libpython; the build needs the dev headers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bin "${SMSC_BIN}" \
    && cp "target/release/${SMSC_BIN}" /usr/local/bin/smsc

# ── Runtime stage ───────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# libpython3 is required at runtime (pyo3 embeds CPython). Add CA certs if
# your binds use TLS.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpython3.11 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home smsc

COPY --from=build /usr/local/bin/smsc /usr/local/bin/smsc
# Your handler script + addon config. Mount these as a volume/ConfigMap in
# production so you can hot-reload smpp.py without rebuilding the image.
COPY python/ /etc/smsc/python/
COPY deploy/smpp.example.yaml /etc/smsc/smpp.yaml

USER 10001
# SMPP listener. Map to whatever your `server.bind_address`/`port` resolve to.
EXPOSE 2775

# Graceful shutdown: on SIGTERM your binary should unbind outbound binds
# and stop accepting new binds before exiting (give Kubernetes a
# terminationGracePeriodSeconds long enough to drain — see deploy/k8s).
STOPSIGNAL SIGTERM

ENV SMPP_CONFIG=/etc/smsc/smpp.yaml
ENTRYPOINT ["/usr/local/bin/smsc"]
