# Deploying an SMSC built on siphon-smpp

> **These are templates, not a runnable product.** siphon-smpp is a Rust
> *library* that plugs an `smpp` namespace + SMPP runtime into a siphon
> binary **you** build and compose. There is no `siphon-smpp` server to
> run on its own. Everything here is parameterised on *your* image
> (`your-registry/your-smsc`) and *your* binary crate (`your-smsc`).

## What's here

| File | Purpose |
|---|---|
| [`Dockerfile`](Dockerfile) | Multi-stage build of your SMSC binary + the embedded CPython runtime pyo3 needs, shipping your `smpp.py`. |
| [`smpp.example.yaml`](smpp.example.yaml) | Annotated addon config: inbound listener, outbound binds, routing, env injection. |
| [`docker-compose.yml`](docker-compose.yml) | Local dev stack; bind-mounts `smpp.py` for hot reload. |
| [`k8s/`](k8s/) | HA deployment: Deployment + Service (L4 LB) + PDB + optional HPA + ConfigMap/Secret, with a [failover model writeup](k8s/README.md). |

## Topology at a glance

```
                       ┌──────────── your SMSC (siphon binary) ───────────┐
   ESMEs  ──bind──▶    │  siphon-smpp:                                     │   ──bind──▶  upstream
  (apps)  ◀─deliver─   │    • inbound listener (server)                    │   ◀─deliver─   SMSCs
                       │    • outbound binds (client, reconnect+throttle)  │            (aggregators)
                       │    • dispatch → your smpp.py handlers             │
                       └──────────────────────────────────────────────────┘
                                         your script owns:
                                  auth · routing · DLR correlation · queueing
```

## Runtime prerequisites

- **libpython** in the runtime image — pyo3 runs with `auto-initialize`,
  so the binary embeds CPython and needs `libpython3.x` present (the
  Dockerfile installs it).
- **Your `smpp.py`** mounted at runtime (volume / ConfigMap) so handlers
  can be hot-reloaded without a rebuild.
- **CA certificates** if any bind uses TLS.

## Wiring config

Two ways, combinable:

1. **File** — `smpp.yaml` (see `smpp.example.yaml`), with `${VAR}` /
   `${VAR:-default}` expansion for secrets.
2. **Environment** — declare whole outbound binds via
   `SMPP_BIND_<NAME>_HOST`, `_PORT`, `_SYSTEM_ID`, `_PASSWORD`,
   `_BIND_TYPE`, `_MAX_MPS`, … Keeps credentials out of the file and out
   of your image.

See [`k8s/README.md`](k8s/README.md) for the HA / failover trade-offs
(SMPP is stateful per session — scaling outbound binds has caveats).
