# Configuration

siphon-smpp is configured by its **own YAML file**, separate from SIPhon's main
config. You reference it from `siphon.yaml` under `extensions`:

```yaml
# siphon.yaml (main config)
extensions:
  smpp: /etc/siphon/smpp.yaml
```

The addon config has three top-level sections: [`server`](#server-the-inbound-listener)
(the inbound listener), [`binds`](#binds-outbound-binds) (outbound binds to
upstream SMSCs), and [`routing`](#routing-declarative-routing-rules) (declarative
routing your script reads). All three are
optional — an inbound-only echo needs just `server`; a pure relay-out client
needs just `binds`.

!!! tip "Annotated example"
    [`deploy/smpp.example.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/smpp.example.yaml)
    is a fully commented config covering every field below.

## Variable expansion

Any string value supports `${VAR}` and `${VAR:-default}` expansion, resolved from
the process environment at load time. Use it to keep secrets out of the file:

```yaml
password: ${SMPP_AGG_PASSWORD}
port: ${SMPP_PORT:-2775}
```

## `server` — the inbound listener

Where external ESMEs connect and `bind_transceiver`.

```yaml
server:
  bind_address: "0.0.0.0"
  port: 2775
  session_init_timer_ms: 5000        # await bind after TCP connect (default)
  enquire_link_timer_ms: 30000       # keep-alive interval (default)
  inactivity_timer_ms: 300000        # drop an idle session — 5 min (default)
  response_timer_ms: 30000           # await a PDU response (default)
  max_msg_per_sec: 200               # inbound throttle, per ESME session; 0 = unlimited
  throttle_action: pace              # over-rate: pace (default) | reject (ESME_RTHROTTLED)
  # tls:
  #   cert_path: /etc/siphon/tls/smpp.crt
  #   key_path:  /etc/siphon/tls/smpp.key
  #   ca_path:   /etc/siphon/tls/ca.crt   # optional, for client-cert checks
```

| Field | Default | Meaning |
|---|---|---|
| `bind_address` | `0.0.0.0` | Listen address. |
| `port` | `2775` | Listen port (SMPP standard is 2775). |
| `session_init_timer_ms` | `5000` | How long a freshly connected TCP peer has to send its bind before it's dropped. |
| `enquire_link_timer_ms` | `30000` | Keep-alive interval; the runtime answers and issues `enquire_link`. |
| `inactivity_timer_ms` | `300000` | Idle sessions (no PDUs) are unbound after this. |
| `response_timer_ms` | `30000` | How long to wait for a PDU response before failing it. |
| `max_msg_per_sec` | `0` (unlimited) | **Inbound** rate cap, per ESME session — the ingress mirror of a bind's outbound cap. |
| `throttle_action` | `pace` | What to do when a session exceeds `max_msg_per_sec`: `pace` or `reject` (see [Throttling](#throttling)). |
| `tls` | *(off)* | If present, the listener speaks TLS. `ca_path` enables client-certificate verification. |

Omit `tls` for plaintext SMPP.

## `binds` — outbound binds

Each entry opens a supervised outbound connection to a remote SMSC / aggregator
and binds as an ESME. The `name` is how your script targets it:
`submit_via(bind="aggregator-eu", …)`.

```yaml
binds:
  - name: aggregator-eu             # referenced by *_via(bind="aggregator-eu")
    host: smpp.example-aggregator.com
    port: 2775
    system_id: my-esme
    password: ${SMPP_AGG_PASSWORD}
    bind_type: transceiver           # transmitter | receiver | transceiver (default)
    max_msg_per_sec: 100             # outbound throttle; 0 = unlimited
    # tls: { ca_path: /etc/siphon/tls/ca.crt }
```

| Field | Default | Meaning |
|---|---|---|
| `name` | *(required)* | Logical name your script uses in `*_via(bind=…)`. |
| `host` / `port` | *(required)* | Upstream address. |
| `system_id` / `password` | *(required)* | Your ESME credentials at the upstream. |
| `bind_type` | `transceiver` | `transmitter`, `receiver`, or `transceiver`. |
| `max_msg_per_sec` | `0` (unlimited) | Per-bind outbound submit rate cap (token bucket). |
| `tls` | *(off)* | TLS to the upstream; `ca_path` to pin the server CA. |

Each bind is supervised independently: connect → bind → hold → on drop, reconnect
with exponential backoff (capped at 60 s, reset after a healthy session).

### Declaring binds via environment variables

Whole outbound binds can be declared from env vars instead of the file — handy
for secrets and for injecting binds per-environment. A bind named `aggregator-eu`
is discovered from its `_HOST` variable:

```bash
SMPP_BIND_AGGREGATOR_EU_HOST=smpp.example-aggregator.com
SMPP_BIND_AGGREGATOR_EU_PORT=2775
SMPP_BIND_AGGREGATOR_EU_SYSTEM_ID=my-esme
SMPP_BIND_AGGREGATOR_EU_PASSWORD=s3cr3t
SMPP_BIND_AGGREGATOR_EU_BIND_TYPE=transceiver   # optional
SMPP_BIND_AGGREGATOR_EU_MAX_MPS=100             # optional, 0 = unlimited
```

The `<NAME>` segment is **uppercased** in the env var and **lowercased** to form
the bind name; names must not contain underscores (the underscore is the field
separator). Env-var binds **merge** with any declared in the file. This is the
recommended way to inject upstream credentials in
[Kubernetes](kubernetes.md#configuration-configmap-secret) (via a `Secret` +
`envFrom`).

A few non-bind settings are also overridable from the environment:

```bash
SMPP_SERVER_MAX_MPS=500              # overrides server.max_msg_per_sec (inbound throttle)
SMPP_SERVER_THROTTLE_ACTION=reject   # overrides server.throttle_action
SMPP_DEFAULT_CHAIN=bind:aggregator-eu,queue   # overrides routing.default_chain
```

## Throttling

Throttling is **symmetric**, and both directions are enforced in Rust (token
buckets) — your script decides *policy*, not per-message pacing:

| Direction | Where | Behaviour |
|---|---|---|
| **Outbound** | `max_msg_per_sec` on each `binds:` entry | Pure speed limit — submits are **delayed** to stay under the cap, never rejected. |
| **Inbound** | `server.max_msg_per_sec`, per ESME session | Over-rate behaviour is chosen by `server.throttle_action`. |

`server.throttle_action`:

- **`pace`** (default) — delay the response, backpressuring through the ESME's
  SMPP window. Smooth, but relies on the ESME respecting its window.
- **`reject`** — answer immediately with `ESME_RTHROTTLED`, the SMPP-native
  back-off signal. Well-behaved ESMEs slow down and retry.

Set `max_msg_per_sec: 0` (the default) on either side to disable that cap.

## `routing` — declarative routing rules

siphon-smpp does **not** route for you — routing is a policy decision, so it's
your script's job. This section is just a place to *declare* rules your script
reads via [`smpp.routing_rules()`](script-api.md#config-readouts); the runtime
never acts on it directly.

```yaml
routing:
  default_chain: ["bind:aggregator-eu", "queue"]
  rules:
    - prefix: "31"                   # E.164 prefix (no '+'); longest match wins
      name: nl
      chain: ["bind:aggregator-eu"]
    - prefix: "1"
      name: na
      chain: ["bind:aggregator-us"]
```

- **`rules`** — a list of `{prefix, name, chain}`. A `chain` is an ordered list
  of steps; a step of the form `bind:<name>` names an outbound bind. Other step
  tokens (e.g. `queue`) are conventions *your script* interprets.
- **`default_chain`** — used when no rule prefix matches. `SMPP_DEFAULT_CHAIN`
  (env) overrides it.

The [gateway example](cookbook/smsc-gateway.md#routing) implements
longest-prefix-wins over exactly this structure. Because it's just data your
script reads, you're free to ignore it and route however you like.

## How the pieces connect

```
siphon.yaml ──extensions.smpp──▶ smpp.yaml
                                   ├─ server  ─────▶ inbound listener (ESMEs bind in)
                                   ├─ binds   ─────▶ outbound binds (you bind out)  ◀── SMPP_BIND_* env
                                   └─ routing ─────▶ smpp.routing_rules()  (your script reads)
```

Config is re-parsed on boot and on hot-reload (parsing `smpp.yaml` costs ~9 µs,
so this is free). Next: the [Script API](script-api.md) your handlers use, or
[Building an SMSC gateway](cookbook/smsc-gateway.md) to see config and handlers
work together.
