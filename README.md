# Auto-Scaling Multi-Tier Cloud Service

A self-scaling, multi-tier web service that elastically adds and removes VMs in response to load —
built to handle bursty, "Black Friday"-style traffic while meeting latency SLAs and avoiding
over-provisioning.

> **Deep-dive highlight —** the **scaling loop + load shedding** in `MasterServiceImpl.java`: the
> master watches pending-request depth to decide when to provision or decommission VMs (with a
> scale-in cooldown to avoid thrashing), and drops requests that can no longer meet their latency
> target instead of processing them late. This is the "watch live state → drive node lifecycle"
> control loop at the heart of the project.

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

## Key code to look at

The whole orchestration story lives in `src/MasterServiceImpl.java`:

| Lines | What it is |
|---|---|
| `312-402` | `scalingLoop()` — the control loop that polls load every 500ms and decides to scale |
| `348-371` | Scale-**up** logic — scales out on high app-tier pending / queue depth, with a cooldown |
| `373-393` | Scale-**down** logic — conservative scale-in only after load stays low for a patience window |
| `266-282` | Startup ordering in `run()` — waits for a readiness threshold before accepting traffic |
| `288-298` | Load shedding — drops requests that can no longer meet their latency target |
| `411-449` | `scaleDownAppTier()` — graceful teardown: pick least-loaded victim, drain, then unexport |

## Tech Stack

Java · Java RMI · distributed systems · autoscaling · load balancing · performance benchmarking
