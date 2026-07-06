# Auto-Scaling Multi-Tier Cloud Service

A self-scaling, multi-tier web service that elastically adds and removes VMs in response to load —
built to handle bursty, "Black Friday"-style traffic while meeting latency SLAs and avoiding
over-provisioning.

## The idea

Requests flow through three tiers, coordinated by a master node that watches load and scales the
fleet up or down in real time:

```
  clients ──►  Front-End tier  ──(RMI)──►  App tier  ──►  data store
                     ▲                        ▲
                     └──────── Master (orchestrator) ───────┘
                       provisions/decommissions VMs,
                       assigns roles, load-balances
```

## Key features

- **3-tier architecture** — a front-end tier that accepts and parses requests, an application tier
  that processes them, and a **master orchestrator** that manages the whole fleet.
- **Dynamic horizontal autoscaling** — scales out when queue depth / pending requests rise, and
  scales back in (with a cooldown) when load subsides, to control cost.
- **Load balancing** — round-robin request forwarding across app-tier workers with stub caching.
- **Load shedding** — drops queued requests that can no longer meet their latency target instead of
  processing them late.
- **Graceful lifecycle** — clean startup ordering (wait for capacity before accepting traffic) and
  graceful shutdown (drain queues, unregister, unexport).

## Components

| File | Role |
|---|---|
| `MasterServiceImpl.java` | Orchestrator: role assignment, VM lifecycle, scaling loop, load balancing |
| `FrontEndWorkerServiceImpl.java` | Accepts client connections, parses and forwards requests |
| `AppTierServiceImpl.java` | Processes requests via a bounded worker pool |
| `*Service.java` | RMI service interfaces |
| `Server.java` | Entry point; boots into the correct role |

## Results

Tuned across a series of benchmark experiments; reached **~96% client satisfaction** on oscillating
("step up / step down") workloads and sustained heavy peak load (~7 clients/sec) while keeping VM
usage efficient.

## Tech Stack

Java · Java RMI · distributed systems · autoscaling · load balancing · performance benchmarking
