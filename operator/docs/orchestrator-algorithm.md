# QoS-Aware Resource Orchestration for AI-RAN Workloads: Algorithm Description

## 1. Overview

This document describes the resource allocation and scheduling algorithm implemented in the AI-RAN orchestrator. The orchestrator manages a heterogeneous Kubernetes cluster shared between Radio Access Network (RAN) functions and general AI/ML workloads, enforcing strict Quality-of-Service (QoS) differentiation while exploiting traffic predictions from a Digital Twin (DT) to act proactively rather than purely reactively.

The reconciliation loop runs every 30 seconds. At each cycle the orchestrator:

1. collects current workload metrics from Prometheus,
2. queries the Digital Twin for a 1-hour-ahead traffic load prediction,
3. reads the current cluster node state (available CPU, memory, GPU per node),
4. runs the allocation algorithm and emits a prioritised list of *scaling directives*,
5. applies each directive in priority order via the Kubernetes API.

---

## 2. System Model

### 2.1 Workload Classes

Workloads are classified into three QoS tiers based on the Kubernetes namespace they belong to.

| QoS Level | Value | Namespace(s) | Scaling Mode | Capacity Guarantee |
|-----------|-------|--------------|--------------|-------------------|
| **High** | 1 | `srsran` | Vertical only | Always guaranteed; preempts Low |
| **Medium** | 2 | `ricplt`, `ricxapp` | Vertical only | Always guaranteed; preempts Low |
| **Low** | 3 | `ai-workloads` | Horizontal only | Best-effort; may be preempted |

RAN workloads (High and Medium) comprise the gNB (srsran), the near-RT RIC platform (ricplt), and RIC xApps (ricxapp). Non-RAN workloads (Low) are general AI/ML inference and training jobs.

The fundamental scheduling assumption is that the infrastructure is dimensioned to always run all RAN workloads. Non-RAN workloads occupy remaining capacity and may be throttled or evicted when RAN functions require additional resources.

### 2.2 Node Topology

Nodes are identified by Kubernetes labels. Three label types are relevant:

| Label | Meaning | Eligible Workloads |
|-------|---------|--------------------|
| `ran` | RAN infrastructure node | High, Medium |
| `gpu` | Node has GPU resources | Low (when traffic is low) |
| `computing` | General compute node | Low (always eligible) |

A node may carry more than one label. In the reference deployment, `edge162.mmwunibo.it` carries both `ran` and `computing`; `smontebu-gpu-worker` carries both `gpu` and `computing`. Non-RAN workloads are never placed on nodes that carry only the `ran` label.

### 2.3 Digital Twin Traffic Prediction

The DT exposes a gRPC endpoint that returns a `TrafficPrediction` record for a given time horizon:

```
TrafficPrediction {
    PredictedLoad   ∈ [0, 1]   // normalised RAN traffic load
    Confidence      ∈ [0, 1]   // model confidence
    PeakExpected    bool
    PeakStartTime   time?
    PeakEndTime     time?
    PeakMagnitude   float       // relative increase (1.5 = +50%)
}
```

Predictions are used in two ways:

- **Resource sizing** — to compute how many resources a RAN workload will need before latency degrades.
- **Scheduling gate** — to decide whether non-RAN workloads should be allowed to scale up or must be deferred.

Only predictions with `Confidence ≥ MinConfidenceThreshold` (default 0.5) are acted upon.

---

## 3. Configuration Parameters

| Symbol | Parameter | Default | Description |
|--------|-----------|---------|-------------|
| α | `ScalingSensitivity` | 1.0 | Exponent controlling scaling aggressiveness |
| β | `PredictionWeight` | 0.5 | Weight of DT prediction in resource sizing |
| θ_up | `ScaleUpThreshold` | 0.0 | Latency deviation threshold to trigger scale-up |
| θ_down | `ScaleDownThreshold` | 0.3 | Latency deviation threshold to trigger scale-down |
| θ_load | `TrafficHighLoadThreshold` | 0.7 | Predicted load above which GPU is withheld and non-RAN scheduling is blocked |
| γ_conf | `MinConfidenceThreshold` | 0.5 | Minimum DT confidence to use a prediction |
| φ_cpu | `CPUSaturationThreshold` | 0.85 | CPU utilisation above which RAN is considered degraded |
| T_postpone | `TrafficPeakPostponeWindow` | 15 min | Fallback deferral window when no DT low-traffic window is found |
| H_max | `MaxScheduleLookAheadHours` | 6 h | Horizon for scanning future low-traffic windows |
| T_rec | `ReconciliationPeriod` | 30 s | Allocation cycle interval |

---

## 4. Resource Sizing Formulae

### 4.1 Latency Deviation

Let $L_{cur}$ be the observed latency and $L_{tgt}$ the SLA target. The normalised deviation is:

$$\delta = \frac{L_{cur} - L_{tgt}}{L_{tgt}}$$

A positive $\delta$ indicates an SLA violation; a negative $\delta$ indicates slack.

### 4.2 Scaling Ratio (Vertical)

The scaling ratio $\rho$ combines the current latency ratio with a prediction factor:

$$\rho = \left(\frac{L_{cur}}{L_{tgt}}\right)^{\alpha} \cdot \phi_{pred}$$

where the prediction factor is:

$$\phi_{pred} = \begin{cases} 1 + \beta \cdot (P_{load} - U_{cur}) & \text{if } P_{load} > U_{cur} \text{ and } C_{pred} \geq \gamma_{conf} \\ 1.0 & \text{otherwise} \end{cases}$$

$P_{load}$ is the DT predicted load, $U_{cur} = \frac{U_{cpu} + U_{gpu}}{2}$ is the current mean utilisation, and $C_{pred}$ is the prediction confidence.

**Proactive behaviour**: when the workload is within its SLA ($L_{cur} = L_{tgt}$, so $\delta = 0$), the base ratio is 1.0. However, if the DT forecasts a load increase ($P_{load} > U_{cur}$), the prediction factor $\phi_{pred} > 1$ drives a proactive scale-up before any latency degradation occurs.

### 4.3 New Resource Target (Vertical)

$$R_{new} = \text{clamp}\!\left(\rho \cdot R_{cur},\; R_{min},\; R_{max}\right)$$

applied independently to CPU and Memory. GPU allocation is unchanged by vertical scaling.

### 4.4 New Replica Count (Horizontal)

$$n_{new} = \text{clamp}\!\left(\left\lceil n_{cur} \cdot (1 + \delta)^{\alpha} \right\rceil,\; n_{min},\; n_{max}\right) \quad \text{(scale-up)}$$

$$n_{new} = \text{clamp}\!\left(\left\lfloor n_{cur} \cdot (1 + \delta)^{\alpha} \right\rfloor,\; n_{min},\; n_{max}\right) \quad \text{(scale-down)}$$

---

## 5. Main Allocation Algorithm

```
Algorithm ALLOCATE(workloads, t)
  Input:  list of WorkloadSpec, current time t
  Output: prioritised list of ScalingDirective

  metrics   ← GetMetrics(workloads)
  pred_1h   ← DT.GetPrediction(horizon = 1h)
  cluster   ← GetClusterState()

  sort workloads by QoSLevel ascending (High first)

  // ── Traffic gate ──────────────────────────────────────────────────────────
  highTrafficGate ← pred_1h.Confidence ≥ γ_conf
                    AND pred_1h.PredictedLoad ≥ θ_load

  if highTrafficGate then
    nextSafeTime ← FIND_NEXT_LOW_TRAFFIC_WINDOW(t)
  end if

  directives ← []

  // ── Resume any previously deferred non-RAN workloads ─────────────────────
  directives += CHECK_DEFERRED_WORKLOADS(pred_1h, t)

  // ── Reactive RAN protection ───────────────────────────────────────────────
  if DETECT_RAN_DEGRADATION(workloads, metrics) then
    directives += REACTIVE_NON_RAN_REDUCTION(workloads)
  end if

  // ── Phase 1: High-priority RAN workloads (srsran) ────────────────────────
  for each w in workloads where w.QoSLevel = High do
    d ← PROCESS_RAN_WORKLOAD(w, metrics[w], cluster, pred_1h)
    if d ≠ null and VALIDATE_RAN_PLACEMENT(d, cluster) then
      directives += d
    end if
  end for

  // ── Phase 2: Medium-priority RAN workloads (ricplt, ricxapp) ─────────────
  for each w in workloads where w.QoSLevel = Medium do
    d ← PROCESS_RAN_WORKLOAD(w, metrics[w], cluster, pred_1h)
    if d ≠ null and VALIDATE_RAN_PLACEMENT(d, cluster) then
      directives += d
    end if
  end for

  // ── Phase 3: Low-priority non-RAN workloads (ai-workloads) ───────────────
  for each w in workloads where w.QoSLevel = Low do
    if IS_DEFERRED(w) then continue end if

    if highTrafficGate then
      // Scheduler gate: only RAN workloads run during predicted peaks
      deferUntil ← nextSafeTime ?? (t + T_postpone)
      DEFER_WORKLOAD(w, deferUntil)

      if w.CurrentReplicas > w.MinReplicas then
        // Throttle running workload to free resources for RAN
        directives += ScaleDown(w, n = w.MinReplicas, postponeUntil = deferUntil)
      else
        directives += Postpone(w, until = deferUntil)
      end if
      continue
    end if

    d ← PROCESS_NON_RAN_WORKLOAD(w, metrics[w], cluster, pred_1h)
    if d ≠ null and VALIDATE_NON_RAN_PLACEMENT(d, w, cluster) then
      directives += d
    end if
  end for

  sort directives by Priority ascending   // lower value = higher urgency
  return directives
```

---

## 6. RAN Workload Processing

RAN workloads (both High and Medium QoS) use **vertical scaling only** — resources (CPU and memory) are resized in place on the same node. No horizontal scaling (replica addition) is ever performed for RAN workloads.

**Capacity guarantee**: if no RAN node has sufficient free capacity for a vertical resize, resources are reclaimed from Low-priority non-RAN workloads via preemption. The preemption priority reflects the QoS tier: High triggers preemption at priority 0 (highest urgency), Medium at priority 1. This ordering ensures that in situations of extreme scarcity, the most critical RAN functions (gNB) are served before the near-RT RIC.

```
Algorithm PROCESS_RAN_WORKLOAD(w, m, cluster, pred)
  Input:  WorkloadSpec w, WorkloadMetrics m,
          ClusterState cluster, TrafficPrediction pred
  Output: ScalingDirective or null

  // Always compute prediction-aware resource needs.
  // When δ = 0, ρ > 1 if DT forecasts load growth → proactive sizing.
  R_proactive ← COMPUTE_VERTICAL_SCALE_UP(w, m, deviation=0, pred)

  // ── Phase A: Initial placement ───────────────────────────────────────────
  if w.CurrentNode = ∅ then
    node ← FIND_RAN_NODE(cluster, R_proactive)
    if node ≠ ∅ then
      return VerticalScaleUp(w, R_proactive, targetNode=node)
    end if

    // Try minimum resources so workload starts; proactive cycle will upsize
    node ← FIND_RAN_NODE(cluster, w.R_min)
    if node ≠ ∅ then
      return VerticalScaleUp(w, w.R_min, targetNode=node)
    end if

    // No capacity at all → preempt non-RAN
    return Preempt(w, R_proactive, priority = QoSLevel(w) - 1)
  end if

  // ── Phase B: Proactive ongoing pre-scaling ───────────────────────────────
  if R_proactive.CPU > w.R_cur.CPU OR R_proactive.Memory > w.R_cur.Memory then
    node ← FIND_RAN_NODE(cluster, R_proactive)
    if node ≠ ∅ then
      return VerticalScaleUp(w, R_proactive, targetNode=node)
    end if
    // No free capacity → preempt non-RAN
    return Preempt(w, R_proactive, priority = QoSLevel(w) - 1)
  end if

  // ── Phase C: Reactive scale-up (latency SLA violated) ────────────────────
  δ ← COMPUTE_DEVIATION(m)
  if δ > θ_up then
    R_reactive ← COMPUTE_VERTICAL_SCALE_UP(w, m, δ, pred)
    node ← FIND_RAN_NODE(cluster, R_reactive)
    if node ≠ ∅ then
      return VerticalScaleUp(w, R_reactive, targetNode=node)
    end if
    return Preempt(w, R_reactive, priority = QoSLevel(w) - 1)
  end if

  // ── Phase D: Scale-down (with prediction guard) ──────────────────────────
  if δ < -θ_down then
    highTraffic ← pred.Confidence ≥ γ_conf AND pred.PredictedLoad ≥ θ_load
    if NOT highTraffic then
      R_new ← COMPUTE_VERTICAL_SCALE_DOWN(w, δ)
      if R_new ≠ w.R_cur then
        return VerticalScaleDown(w, R_new)
      end if
    end if
    // else: hold current resources; prediction shows upcoming traffic
  end if

  return null
```

**Key properties of Phase B**: at every 30-second cycle the orchestrator computes `R_proactive` using the DT prediction. The prediction factor $\phi_{pred}$ is greater than 1 whenever the forecast load exceeds current utilisation, causing `R_proactive > R_cur` even with zero latency deviation. This ensures the workload is sized ahead of demand, not after an SLA breach has already occurred.

**Phase D guard**: scale-down is suppressed when the DT predicts high upcoming load (`PredictedLoad ≥ θ_load`), preventing resource thrashing in the common pattern of a traffic valley followed immediately by a peak.

---

## 7. Non-RAN Workload Processing

Non-RAN workloads use **horizontal scaling only** — replicas are added or removed; per-pod resources remain fixed. Node selection is prediction-aware: the GPU node is withheld when high RAN traffic is forecast, reserving GPU compute for RAN workloads.

```
Algorithm PROCESS_NON_RAN_WORKLOAD(w, m, cluster, pred)
  Input:  WorkloadSpec w, WorkloadMetrics m,
          ClusterState cluster, TrafficPrediction pred
  Output: ScalingDirective or null

  // ── Step 1: Traffic prediction check (always evaluated first) ────────────
  highTraffic ← pred.Confidence ≥ γ_conf AND pred.PredictedLoad ≥ θ_load

  // If high traffic is forecast and workload occupies a GPU node,
  // migrate it proactively to a computing (non-RAN) node to free the GPU.
  if highTraffic AND w.RequiresGPU AND w.CurrentNode ≠ ∅ then
    computingNode ← FIND_COMPUTING_NODE(cluster, w)
    if computingNode ≠ ∅ AND computingNode ≠ w.CurrentNode then
      return MigrateToNode(w, computingNode)
    end if
  end if

  δ ← COMPUTE_DEVIATION(m)

  // ── Step 2: Peak deferral ─────────────────────────────────────────────────
  if w.PostponeDuringPeak AND pred.Confidence ≥ γ_conf then
    if PEAK_IMMINENT(pred, T_postpone) OR IN_PEAK(pred) then
      if δ > θ_up then
        deferUntil ← pred.PeakEndTime ?? (now + T_postpone)
        DEFER_WORKLOAD(w, deferUntil, targetReplicas = COMPUTE_SCALE_UP(w, δ))
        return Postpone(w, until = deferUntil)
      end if
    end if
  end if

  // ── Step 3: Normal horizontal scaling ────────────────────────────────────
  if δ > θ_up then
    n_new ← COMPUTE_HORIZONTAL_SCALE_UP(w, δ)
    if n_new > w.n_cur AND n_new ≤ w.n_max then
      node ← SELECT_NON_RAN_NODE(cluster, w, pred)
      if node = ∅ AND w.RequiresGPU AND NOT w.GPUFallbackCPU then
        return null   // GPU unavailable, fallback not allowed
      end if
      return HorizontalScaleUp(w, n_new, targetNode = node)
    end if

  else if δ < -θ_down then
    n_new ← COMPUTE_HORIZONTAL_SCALE_DOWN(w, δ)
    if n_new < w.n_cur AND n_new ≥ w.n_min then
      return HorizontalScaleDown(w, n_new)
    end if
  end if

  return null
```

### 7.1 Node Selection for Non-RAN Workloads

```
Algorithm SELECT_NON_RAN_NODE(cluster, w, pred)
  if NOT w.RequiresGPU then
    return FIND_COMPUTING_NODE(cluster, w)
  end if

  highTraffic ← pred.Confidence ≥ γ_conf AND pred.PredictedLoad ≥ θ_load

  if highTraffic then
    // GPU reserved for RAN — use CPU-only computing node
    return FIND_COMPUTING_NODE(cluster, w)
  end if

  // Low traffic: GPU is available for non-RAN
  gpuNode ← FIND_GPU_NODE(cluster)
  if gpuNode ≠ ∅ then
    return gpuNode
  end if

  if w.GPUFallbackCPU then
    return FIND_COMPUTING_NODE(cluster, w)
  end if

  return ∅   // no suitable node
```

---

## 8. Scheduling Gate and Deferred Workload Management

### 8.1 Traffic Gate

When the DT forecasts `PredictedLoad ≥ θ_load` with sufficient confidence, the scheduler gate activates. All Phase 3 (non-RAN) workloads are blocked from new scheduling for that cycle. The gate also actively throttles workloads that are currently running above their minimum replica count, reducing them to the minimum to release node headroom for RAN.

The target resume time is not a fixed timeout. Instead, the orchestrator scans future hourly DT predictions to find the first hour at which load is predicted to fall below the threshold:

```
Algorithm FIND_NEXT_LOW_TRAFFIC_WINDOW(t)
  for h = 2 to H_max do
    pred_h ← DT.GetPrediction(horizon = h hours)
    if pred_h.Confidence ≥ γ_conf AND pred_h.PredictedLoad < θ_load then
      return t + h hours
    end if
  end for
  return null   // no low-traffic window found within horizon
```

If no low-traffic window is found within `H_max` hours, the fallback deferral window `T_postpone` is used.

### 8.2 Deferred Workload Resume

At the start of every allocation cycle, the orchestrator checks whether any previously deferred workload can be resumed:

```
Algorithm CHECK_DEFERRED_WORKLOADS(pred, t)
  directives ← []
  for each deferred workload d do
    canResume ← false

    if t > d.DeferUntil then
      canResume ← true
    end if

    if pred is available AND NOT IN_PEAK(pred) then
      if d.PredictedPeakEnd ≠ null AND t > d.PredictedPeakEnd then
        canResume ← true
      end if
    end if

    if pred.Confidence ≥ 0.5 AND NOT pred.PeakExpected
       AND NOT PEAK_IMMINENT(pred, 15 min) then
      canResume ← true
    end if

    if canResume then
      directives += HorizontalScaleUp(d.WorkloadID, n = d.OriginalReplicas)
      remove d from deferred list
    end if
  end for
  return directives
```

---

## 9. Reactive RAN Protection

In addition to the proactive mechanisms above, the orchestrator includes a reactive safety net that activates when RAN degradation is detected at runtime — for example when the DT prediction underestimated actual traffic or when an unexpected interference pattern occurs.

A RAN workload is considered degraded if:

$$L_{cur} > L_{tgt} \quad \text{OR} \quad U_{cpu} > \phi_{cpu}$$

When degradation is detected, all non-RAN workloads with replicas above their minimum are immediately scaled down by 50%:

```
Algorithm REACTIVE_NON_RAN_REDUCTION(workloads)
  directives ← []
  for each w in workloads where NOT w.IsRAN() do
    if w.n_cur > w.n_min then
      n_new ← max(w.n_min, floor(w.n_cur × 0.5))
      directives += HorizontalScaleDown(w, n_new, priority=1)
    end if
  end for
  return directives
```

This mechanism operates independently of the DT prediction and fires within a single reconciliation cycle (≤ 30 s) of the degradation being observed.

---

## 10. Summary of Decision Logic

The table below summarises when each action is taken and for which workload class.

| Condition | Workload Class | Action |
|-----------|---------------|--------|
| Initial placement, DT prediction available | RAN (High/Med) | Vertical scale-up to predicted resource need on RAN node |
| Predicted resources > current resources | RAN (High/Med) | Proactive vertical scale-up every cycle |
| No RAN node has capacity | RAN (High/Med) | Preempt non-RAN workloads (High: priority 0, Med: priority 1) |
| Latency deviation > θ_up | RAN (High/Med) | Reactive vertical scale-up |
| Latency deviation < -θ_down AND prediction low | RAN (High/Med) | Vertical scale-down |
| Latency deviation < -θ_down AND prediction high | RAN (High/Med) | Hold resources (no scale-down) |
| DT predicts high load AND workload on GPU node | Non-RAN | Migrate to computing node (proactive) |
| Peak imminent/ongoing AND PostponeDuringPeak | Non-RAN | Defer scale-up to predicted peak end |
| Traffic gate active AND replicas > min | Non-RAN | Scale down to min replicas; schedule resume at next low-traffic window |
| Traffic gate active AND replicas = min | Non-RAN | Postpone; schedule resume at next low-traffic window |
| DT predicts high load (scale-up path) | Non-RAN | Scale up on computing node (GPU withheld) |
| DT predicts low load (scale-up path) | Non-RAN | Scale up, GPU node eligible |
| RAN degradation detected | Non-RAN | Immediate 50% replica reduction (reactive) |
| Traffic normalised, deferral window elapsed | Non-RAN | Resume to original replica count |
