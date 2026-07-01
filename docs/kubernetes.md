# Kubernetes & scaling

How to run an SMSC built on siphon-smpp with high availability on Kubernetes —
and, just as important, **what "scaling" can and can't mean for a stateful
protocol like SMPP**. Read [the failover model](#the-failover-model-read-this-first)
before you touch `replicas`.

The manifests live in
[`deploy/k8s/`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s)
and are a **template** for *your* SIPhon binary (the one that registers the smpp
addon) — see [Deployment](deployment.md) for why siphon-smpp ships templates
rather than a runnable image.

```bash
kubectl apply -f configmap.yaml     # addon config + handler script + secrets
kubectl apply -f deployment.yaml    # the SMSC pods
kubectl apply -f service.yaml       # L4 load balancer on :2775
kubectl apply -f pdb.yaml           # keep a survivor during drains
kubectl apply -f hpa.yaml           # optional autoscaler (read the caveats!)
```

## The failover model (read this first)

SMPP is **stateful per TCP session**. That single fact shapes everything about
HA and scaling:

### Inbound (ESMEs → you)

Each ESME binds to **exactly one replica** over a long-lived TCP connection.
There is **no session migration**. If that replica dies:

1. the connection resets;
2. the ESME must **rebind**;
3. the load balancer steers the new connection to a surviving replica.

Well-behaved ESMEs reconnect with backoff, so the practical SLA is "**rebind
within a few seconds**". The manifests optimise for exactly this:

- **spread replicas across nodes** (`topologySpreadConstraints`) so one node
  loss can't take the whole SMSC down;
- a **PodDisruptionBudget** so voluntary drains never remove the last replica;
- **`maxUnavailable: 0`** rolling updates so you never dip below desired capacity
  mid-roll.

### Outbound (you → upstream)

Each replica opens its **own** outbound binds (the supervisor in siphon-smpp
reconnects with backoff). With N replicas you present **N binds** to each
upstream. Before you scale past one replica, confirm **two** things:

!!! danger "The two questions to answer before scaling out"
    1. **Does the upstream allow multiple concurrent binds for your
       `system_id`?** Many aggregators do; some permit exactly one. If yours is
       single-bind, N replicas will fight over the one allowed session.
    2. **Is your DLR correlation store shared across replicas?** A delivery
       receipt can come back on **any** replica's outbound bind — not
       necessarily the one that sent the message. That's why the
       [gateway example](cookbook/smsc-gateway.md#state-use-the-shared-store-not-a-dict)
       keys correlation in `siphon.cache` (a shared store), **not** a per-process
       dict. In Kubernetes that store must be shared *across pods* (e.g. a shared
       cache/DB backing `siphon.cache`), or receipts will be dropped as
       "unknown id" on the wrong replica.

### Two topologies

| Topology | How | When to use it |
|---|---|---|
| **Active/active** (these manifests) | ≥2 replicas behind an L4 LB; ESMEs rebind on failover | **Default.** Upstream allows multiple binds; DLR correlation is shared. |
| **Active/standby** | `replicas: 1` + fast reschedule (PDB + spread), or a leader-elected single binder | Upstream permits only one bind per `system_id`, or you need strict single-egress ordering. |

If you **can't** share DLR correlation or the upstream is single-bind, prefer
active/standby and accept the failover gap rather than double-binding.

## The Deployment

Key fields (full file in
[`deployment.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s/deployment.yaml)):

```yaml
spec:
  replicas: 2
  strategy:
    rollingUpdate:
      maxUnavailable: 0        # never drop below desired during a roll
      maxSurge: 1
  template:
    spec:
      topologySpreadConstraints:      # one node loss ≠ whole SMSC down
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: DoNotSchedule
          labelSelector: { matchLabels: { app: smsc } }
      terminationGracePeriodSeconds: 45
      containers:
        - name: smsc
          image: your-registry/your-smsc:latest
          ports: [{ name: smpp, containerPort: 2775 }]
          readinessProbe:                # gate LB traffic on "can accept binds"
            tcpSocket: { port: smpp }
          livenessProbe:                 # restart a wedged replica
            tcpSocket: { port: smpp }
          lifecycle:
            preStop:
              exec: { command: ["sleep", "10"] }   # let the LB stop sending binds
```

- **Readiness** gates the Service on the SMPP port accepting connections, so the
  LB only routes to replicas that can actually bind an ESME.
- **Liveness** restarts a wedged replica.
- **`preStop` sleep + `terminationGracePeriodSeconds`** give the pod time to stop
  receiving new binds and drain in-flight responses before `SIGKILL`. Tune the
  grace period to your drain time.

## The Service (L4 load balancer)

SMPP is a long-lived TCP session, so the LB just needs to pick a healthy replica
**at bind time** and keep the connection pinned to it — see
[`service.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s/service.yaml):

```yaml
spec:
  type: LoadBalancer
  externalTrafficPolicy: Local     # preserve client IP for allow-listing
  ports:
    - { name: smpp, port: 2775, targetPort: smpp, protocol: TCP }
```

!!! warning "Don't let the LB rebalance mid-connection"
    Most cloud L4 LBs pin a TCP connection to one backend for its lifetime. If
    yours pools or rebalances **mid-connection**, disable that for this Service —
    an SMPP session can't survive being moved to another replica. Set a long idle
    timeout too: binds stay open for hours or days.

`externalTrafficPolicy: Local` preserves the client source IP so your
`@smpp.on_bind` handler can allow-list by `client_addr`.

## PodDisruptionBudget

Keep at least one replica serving during voluntary disruptions (node drain,
cluster upgrade). With `replicas: 2`, `minAvailable: 1` means drains take one
replica at a time, so ESMEs always have a survivor to rebind to —
[`pdb.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s/pdb.yaml):

```yaml
spec:
  minAvailable: 1
  selector: { matchLabels: { app: smsc } }
```

## Autoscaling (HPA) — with caveats

SMPP throughput is usually **CPU-bound on the script side** (routing, DLR
correlation, persistence), so CPU is a reasonable HPA signal. But autoscaling
**changes the replica set**, and every new replica opens its **own** outbound
binds — so the [two questions above](#outbound-you-upstream) apply on *every*
scale event, automatically. Only enable the
[HPA](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s/hpa.yaml)
once you're sure the upstream tolerates a variable number of binds:

```yaml
spec:
  minReplicas: 2
  maxReplicas: 6
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 70 } }
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300   # don't thrash binds
```

The `scaleDown` stabilization window keeps the autoscaler from repeatedly
tearing down and re-establishing upstream binds.

!!! tip "Scale up for redundancy first, throughput second"
    A single node already does **tens of thousands of `submit_sm/s`** through one
    bind ([Performance](performance.md)). On a standard (GIL) CPython build,
    aggregate throughput is capped by the per-message Python handler running on
    one core, so **adding replicas buys you redundancy and node-failure
    tolerance more than raw throughput**. The real throughput unlock is
    free-threaded CPython (see [Performance](performance.md#scaling-past-the-gil)),
    not more pods.

## Configuration: ConfigMap + Secret

Mount the addon config **and** the handler script from a ConfigMap; keep upstream
credentials in a Secret referenced via `envFrom`
([`configmap.yaml`](https://github.com/siphon-project/siphon-smpp/blob/main/deploy/k8s/configmap.yaml)):

```yaml
apiVersion: v1
kind: ConfigMap
metadata: { name: smsc-config }
data:
  smpp.yaml: |
    server: { bind_address: "0.0.0.0", port: 2775 }
    binds:
      - name: alpha
        host: smsc-a.example.net
        port: 2775
        system_id: ${SMPP_ALPHA_SYSTEM_ID}   # from the Secret via envFrom
        password: ${SMPP_ALPHA_PASSWORD}
        bind_type: transceiver
        max_msg_per_sec: 50
    routing: { default_chain: ["bind:alpha"] }
  smpp_script.py: |
    from siphon import smpp
    @smpp.on_bind
    async def authorise(bind):
        return bind.accept()          # replace with your credential policy
    @smpp.on_pdu("submit_sm")
    async def on_submit(pdu, session):
        return pdu.reply(message_id="replace-me")
---
apiVersion: v1
kind: Secret
metadata: { name: smsc-secrets }
type: Opaque
stringData:
  SMPP_ALPHA_SYSTEM_ID: alpha_esme
  SMPP_ALPHA_PASSWORD: changeme        # use a real secrets manager in prod
```

The `${VAR}` references in `smpp.yaml` are filled from the Secret at load time;
alternatively declare whole binds via
[`SMPP_BIND_<NAME>_*`](configuration.md#declaring-binds-via-environment-variables).

## Hot reload in-cluster

Because `smpp.py` is mounted from the ConfigMap, you can edit it, `kubectl
apply`, and let SIPhon hot-reload the handlers — **no image rebuild, no rebind**.
The kubelet propagates ConfigMap changes to the mounted file within about a
minute; SIPhon picks up the new script on the next PDU. Keep handlers free of
import-time side effects so a reload mid-traffic is safe, and keep cross-message
state in the [shared store](concepts.md#where-state-lives).

## Graceful shutdown

On rollout/scale-down Kubernetes sends `SIGTERM`. Your binary should unbind its
outbound binds and stop accepting new binds, then exit. The Deployment gives it
room: a `preStop` sleep so the LB stops sending new binds first, and
`terminationGracePeriodSeconds: 45` before `SIGKILL`. Tune the grace period to
your actual drain time.

## Checklist before scaling out

- [ ] Upstream allows **N concurrent binds** for your `system_id`.
- [ ] DLR correlation / session maps are in a store **shared across pods**.
- [ ] The LB **pins** each TCP connection to one backend for its lifetime.
- [ ] ESMEs reconnect with backoff (they must, to survive failover).
- [ ] Your handler unbinds cleanly on `SIGTERM` within the grace period.
- [ ] If any box is unchecked → use **active/standby**, not active/active.
