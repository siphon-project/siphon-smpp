# Running an SMSC on Kubernetes

These manifests are a **template** for an SMSC built on siphon-smpp. They
deploy *your* siphon binary (the one that registers the smpp addon) — see
[`../README.md`](../README.md) for why siphon-smpp ships templates rather
than a runnable image.

```
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f pdb.yaml
# optional:
kubectl apply -f hpa.yaml
```

## The failover model (read this before scaling)

SMPP is **stateful per TCP session**. That shapes what "HA" can mean:

- **Inbound (ESMEs → you).** Each ESME binds to exactly one replica over a
  long-lived TCP connection. If that replica dies, the connection resets
  and the ESME must **rebind** — at which point the load balancer steers
  it to a surviving replica. There is no session migration; well-behaved
  ESMEs reconnect with backoff, so the practical SLA is "rebind within a
  few seconds". The manifests optimise for this: replicas spread across
  nodes (`topologySpreadConstraints`), a `PodDisruptionBudget` so drains
  never remove the last replica, and `maxUnavailable: 0` rolls.

- **Outbound (you → upstream SMSCs).** Each replica opens its **own**
  outbound binds (the supervisor in siphon-smpp reconnects with backoff).
  With N replicas you present N binds to each upstream. Confirm before
  scaling:
    1. the upstream **allows multiple concurrent binds** for your
       system_id (many do; some don't);
    2. your **DLR correlation store is shared** across replicas, because a
       receipt can come back on any replica's bind — that's why the
       example keys correlation in `siphon.cache` (a shared store), not a
       per-process dict.

### Two topologies

| Topology | How | When |
|---|---|---|
| **Active/active** (these manifests) | ≥2 replicas behind an L4 LB; ESMEs rebind on failover | Default. Upstream allows multiple binds; correlation is shared. |
| **Active/standby** | `replicas: 1` + fast reschedule (PDB + spread), or a leader-elected single binder | Upstream permits only one bind per system_id, or you need strict single-egress ordering. |

If you can't share DLR correlation or the upstream is single-bind, prefer
active/standby and accept the failover gap rather than double-binding.

## Graceful shutdown

On rollout/scale-down Kubernetes sends `SIGTERM`. Your binary should
unbind its outbound binds and stop accepting new binds, then exit. The
Deployment gives it room: a `preStop` sleep so the LB stops sending new
binds first, and `terminationGracePeriodSeconds: 45` before `SIGKILL`.
Tune the grace period to your drain time.

## Hot reload

`smpp.py` is mounted from the ConfigMap. Edit it, `kubectl apply`, and the
kubelet propagates the change to the mounted file; siphon reloads the
handlers on the next PDU — no restart, no rebind. Keep handlers free of
import-time side effects so a reload is safe mid-traffic.
