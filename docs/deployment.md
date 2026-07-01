# Deployment

!!! warning "Templates, not a runnable product"
    siphon-smpp is a Rust **library** that plugs an `smpp` namespace + SMPP
    runtime into a [SIPhon](https://siphon-sip.org/) binary **you** build and
    compose. There is no `siphon-smpp` server to run on its own. Everything here
    is parameterised on *your* image (`your-registry/your-smsc`) and *your* binary
    crate (`your-smsc`). See [Using it in a SIPhon build](integration.md).

The repo ships deployment templates under
[`deploy/`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/README.md):

| File | Purpose |
|---|---|
| [`Dockerfile`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/Dockerfile) | Multi-stage build of your SMSC binary + the embedded CPython runtime pyo3 needs, shipping your `smpp.py`. |
| [`smpp.example.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/smpp.example.yaml) | Annotated addon config: inbound listener, outbound binds, routing, env injection. |
| [`docker-compose.yml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/docker-compose.yml) | Local dev stack; bind-mounts `smpp.py` for hot reload. |
| [`k8s/`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s) | HA deployment — see [Kubernetes & scaling](kubernetes.md). |

## Topology at a glance

```
                    ┌──────────── your SMSC (a SIPhon binary) ─────────┐
  ESMEs  ──bind──▶  │  siphon-smpp:                                    │  ──bind──▶  upstream
 (apps)  ◀─deliver─ │    • inbound listener (server)                   │  ◀─deliver─   SMSCs
                    │    • outbound binds (client, reconnect+throttle)  │           (aggregators)
                    │    • dispatch → your smpp.py handlers            │
                    └──────────────────────────────────────────────────┘
                                     your script owns:
                              auth · routing · DLR correlation · queueing
```

## Runtime prerequisites

- **libpython** in the runtime image — pyo3 runs with `auto-initialize`, so the
  binary embeds CPython and needs `libpython3.x` present (the template Dockerfile
  installs it).
- **Your `smpp.py`** mounted at runtime (volume / ConfigMap) so handlers
  [hot-reload](concepts.md#dispatch-hot-reload) without a rebuild.
- **CA certificates** if any bind uses TLS.

## Building the image

The template Dockerfile is an ordinary two-stage `cargo build` of *your* binary
crate — nothing SIPhon-specific about the composition belongs in it. Point
`SMSC_BIN` at your crate name:

```bash
docker build -f deploy/Dockerfile --build-arg SMSC_BIN=your-smsc \
  -t your-registry/your-smsc:latest .
```

The build stage compiles with `python3-dev` (pyo3 links libpython); the runtime
stage is `debian:bookworm-slim` + `libpython3.x` + CA certs, runs as a non-root
user, `EXPOSE 2775`, and uses `STOPSIGNAL SIGTERM` for graceful drain.

## Local dev with docker-compose

`docker-compose.yml` brings up your SMSC with the handler script and addon config
**bind-mounted**, so you can edit `smpp.py` on the host and SIPhon hot-reloads it
— no rebuild, no rebind:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Point an SMPP client at `localhost:2775` to bind + submit. The compose file shows
both ways to inject upstream credentials: named env vars referenced by
`${VAR}` expansion in `smpp.yaml`, and whole
[env-var binds](configuration.md#declaring-binds-via-environment-variables)
(`SMPP_BIND_<NAME>_*`).

## Wiring config

Two ways, combinable — see [Configuration](configuration.md):

1. **File** — `smpp.yaml`, with `${VAR}` / `${VAR:-default}` expansion for
   secrets.
2. **Environment** — declare whole outbound binds via `SMPP_BIND_<NAME>_HOST`,
   `_PORT`, `_SYSTEM_ID`, `_PASSWORD`, `_BIND_TYPE`, `_MAX_MPS`. Keeps
   credentials out of the file and out of your image.

## Graceful shutdown

On rollout / scale-down the orchestrator sends `SIGTERM`. Your binary should
**unbind its outbound binds and stop accepting new binds, then exit**. Give it
room to drain in-flight responses first — `stop_grace_period` in compose,
`terminationGracePeriodSeconds` + a `preStop` sleep in
[Kubernetes](kubernetes.md#graceful-shutdown).

## Next

- **Run it HA** → [Kubernetes & scaling](kubernetes.md) — the SMPP failover model
  and how to scale inbound vs. outbound safely.
- **Load-test it** → [Performance & load testing](performance.md).
