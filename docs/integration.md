# Using it in a SIPhon build

siphon-smpp is a **library**, not a standalone server. The runnable artifact is a
[SIPhon](https://siphon-sip.org/) binary that has the `smpp` addon enabled. This
page explains how the addon is consumed and configured; the details of composing
extensions into a binary live in the **[SIPhon
documentation](https://siphon-sip.org/extensions/)**.

## How it's consumed

siphon-smpp is **not published to crates.io** — it's an extension, consumed by
git from a composing SIPhon binary. That binary depends on siphon-smpp (and on
SIPhon itself) as Cargo git dependencies, and at startup it wires in the addon's
two hooks:

- **`namespace(cfg)`** — builds the `smpp` Python module your scripts import
  (`from siphon import smpp`): the decorators, the helper classes, and the send
  helpers.
- **`task(cfg)`** — spawns the SMPP runtime (the inbound listener + the
  supervised outbound binds).

You don't call these yourself — the composing binary does, at startup. See the
SIPhon docs for how a binary registers an extension's hooks.

## The `smpp` feature

In practice you build a `siphon` binary with the `smpp` addon turned on via a
feature flag, from SIPhon's binary package:

```bash
cargo build -p siphon-bin --release --features smpp
```

- **Feature on** → the binary registers the `smpp` namespace + runtime and reads
  `extensions.smpp` from your config.
- **Feature off, but `extensions.smpp` present in config** → the binary logs a
  loud warn-and-skip (it won't silently ignore an addon you configured).

## Wiring the config

Once you have an `smpp`-enabled binary, point it at your addon config from the
main SIPhon config:

```yaml
# siphon.yaml (main config)
extensions:
  smpp: /etc/siphon/smpp.yaml
```

Everything about that `smpp.yaml` — the inbound listener, outbound binds,
routing, env-var injection — is in [Configuration](configuration.md). Your
handler script (`smpp.py`) is loaded as SIPhon's script; see the
[Quickstart](quickstart.md) to run one and the
[Script API](script-api.md) for what it can call.

## Version pinning

siphon-smpp pins **PyO3 0.29** and tracks SIPhon's PyO3 major version. Both link
the `python` native library, and Cargo allows only one version of a `links` crate
per dependency graph — so when SIPhon bumps PyO3, siphon-smpp bumps in lockstep.
If you compose your own binary, keep the SIPhon and siphon-smpp revisions
compatible (a mismatch surfaces as a build error, not a runtime surprise).

## Putting it together

```
your composing SIPhon binary  (cargo build --features smpp)
        │  registers at startup
        ├── smpp namespace  ──▶  `from siphon import smpp`  (your smpp.py)
        └── smpp runtime    ──▶  inbound listener + outbound binds
                                        ▲
   siphon.yaml  ──extensions.smpp──▶  smpp.yaml  (server / binds / routing)
```

- **Build** → this page + the SIPhon extension docs.
- **Configure** → [Configuration](configuration.md).
- **Write handlers** → [Script API](script-api.md),
  [Cookbook](cookbook/smsc-gateway.md).
- **Deploy** → [Deployment](deployment.md), [Kubernetes & scaling](kubernetes.md).
