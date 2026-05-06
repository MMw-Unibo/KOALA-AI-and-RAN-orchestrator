package allocation

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
)

// Allocator implements the QoS-Aware Resource Allocation Function
type Allocator struct {
	config        AllocationConfig
	metricsClient MetricsClient
	digitalTwin   DigitalTwinClient
	clusterState  ClusterStateProvider

	// Deferred workloads waiting for traffic to normalize
	deferredWorkloads map[string]*DeferredWorkload
	deferredMutex     sync.RWMutex
}

// NewAllocator creates a new Allocator instance
func NewAllocator(
	config AllocationConfig,
	metrics MetricsClient,
	twin DigitalTwinClient,
	cluster ClusterStateProvider,
) *Allocator {
	return &Allocator{
		config:            config,
		metricsClient:     metrics,
		digitalTwin:       twin,
		clusterState:      cluster,
		deferredWorkloads: make(map[string]*DeferredWorkload),
	}
}

// Allocate is the main entry point - implements the QoS-aware allocation algorithm
// Node placement validation ensures workloads match node labels (gpu, ran, computing)
func (a *Allocator) Allocate(ctx context.Context, workloads []WorkloadSpec) ([]ScalingDirective, error) {
	// Fetch current metrics for all workloads
	metrics, err := a.metricsClient.GetMetrics(ctx, workloadIDs(workloads))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics: %w", err)
	}

	// Fetch traffic prediction from Digital Twin
	prediction, err := a.digitalTwin.GetPrediction(ctx, 10*time.Minute)
	if err != nil {
		klog.Warningf("Failed to get traffic prediction, continuing without: %v", err)
		prediction = &TrafficPrediction{PredictedLoad: 0, Confidence: 0}
	}

	klog.Infof("Traffic predicted load %.2f", prediction.PredictedLoad)

	// Get cluster state with node labels
	cluster, err := a.clusterState.GetState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster state: %w", err)
	}

	// Sort workloads by priority (high first)
	sortByPriority(workloads)

	// Determine if the traffic gate should block non-RAN scheduling this cycle.
	// When predicted load is high, non-RAN workloads are deferred to the next
	// low-traffic window found by scanning future hourly predictions.
	highTrafficGate := prediction != nil &&
		prediction.Confidence >= a.config.MinConfidenceThreshold &&
		prediction.PredictedLoad >= a.config.TrafficHighLoadThreshold

	var nextSafeSchedulingTime *time.Time
	if highTrafficGate {
		klog.Infof("Traffic gate active: predicted load %.2f >= threshold %.2f, searching for next low-traffic window",
			prediction.PredictedLoad, a.config.TrafficHighLoadThreshold)
		nextSafeSchedulingTime = a.findNextLowTrafficTime(ctx)
		if nextSafeSchedulingTime != nil {
			klog.Infof("Next low-traffic window found at %s", nextSafeSchedulingTime.Format(time.RFC3339))
		} else {
			klog.Warningf("No low-traffic window found within %dm, using postpone window as fallback",
				a.config.MaxScheduleLookAheadMinutes)
		}
	}

	var directives []ScalingDirective

	// Check for deferred workloads that can now be scheduled (traffic normalized)
	resumedDirectives := a.checkDeferredWorkloads(prediction)
	directives = append(directives, resumedDirectives...)

	// Check for RAN degradation - triggers reactive scaling of non-RAN workloads
	ranDegraded := a.detectRANDegradation(workloads, metrics)
	if ranDegraded {
		klog.Warning("RAN degradation detected, triggering reactive non-RAN reduction")
		nonRANDirectives := a.reactiveNonRANReduction(workloads, metrics, cluster)
		directives = append(directives, nonRANDirectives...)
	}

	// Phase 1: HIGH priority workloads (srsran namespace) - Vertical scaling only
	// Must be placed on nodes labeled "ran"
	klog.V(2).Info("Phase 1: Processing HIGH priority workloads (srsran)")
	for _, w := range filterByQoS(workloads, QoSHigh) {
		m, ok := metrics[w.ID]
		if !ok {
			klog.Warningf("No metrics found for workload %s, skipping", w.ID)
			continue
		}

		directive := a.processRANWorkload(ctx, w, m, cluster, prediction)
		if directive != nil {
			// Validate node placement for RAN workload
			if err := a.validateRANPlacement(directive, cluster); err != nil {
				klog.Warningf("RAN placement validation failed for %s: %v", w.ID, err)
				continue
			}
			directives = append(directives, *directive)
		}
	}

	// Phase 2: MEDIUM priority workloads (ricplt, ricxapp namespaces) - Vertical scaling only
	// Must be placed on nodes labeled "ran"
	klog.V(2).Info("Phase 2: Processing MEDIUM priority workloads (ricplt, ricxapp)")
	for _, w := range filterByQoS(workloads, QoSMedium) {
		m, ok := metrics[w.ID]
		if !ok {
			klog.Warningf("No metrics found for workload %s, skipping", w.ID)
			continue
		}

		directive := a.processRANWorkload(ctx, w, m, cluster, prediction)
		if directive != nil {
			if err := a.validateRANPlacement(directive, cluster); err != nil {
				klog.Warningf("RAN placement validation failed for %s: %v", w.ID, err)
				continue
			}
			directives = append(directives, *directive)
		}
	}

	// Phase 3: LOW priority workloads (ai-workloads namespace) - Horizontal scaling only
	// Placed on nodes labeled "computing" or "gpu"
	// Subject to proactive orchestration from Digital Twin
	// When the traffic gate is active (load >= TrafficHighLoadThreshold), ALL non-RAN
	// workloads are deferred to the next predicted low-traffic window.
	klog.V(2).Info("Phase 3: Processing LOW priority workloads (ai-workloads)")
	for _, w := range filterByQoS(workloads, QoSLow) {
		// Skip if this workload is already deferred
		if a.isDeferred(w.ID) {
			klog.V(3).Infof("Workload %s is deferred, skipping", w.ID)
			continue
		}

		m, ok := metrics[w.ID]
		if !ok {
			klog.Warningf("No metrics found for workload %s, skipping", w.ID)
			continue
		}

		// SCHEDULING GATE: if traffic is above the threshold, schedule RAN only.
		// For non-RAN workloads:
		//   - Defer any new scheduling to the next predicted low-traffic window.
		//   - If the workload is currently running above minimum replicas, throttle
		//     it down to minimum now and set PostponeUntil for the safe window.
		//     This adds an explicit execution delay rather than just blocking new work.
		// The target resume time is determined by scanning future hourly predictions
		// (findNextLowTrafficTime), so workloads are rescheduled exactly when traffic
		// is forecast to drop — not after a fixed timeout.
		if highTrafficGate {
			deferUntil := time.Now().Add(a.config.TrafficPeakPostponeWindow)
			if nextSafeSchedulingTime != nil {
				deferUntil = *nextSafeSchedulingTime
			}
			a.deferWorkload(&DeferredWorkload{
				WorkloadID:       w.ID,
				Namespace:        w.Namespace,
				DeferredAt:       time.Now(),
				DeferUntil:       deferUntil,
				Reason:           fmt.Sprintf("Scheduler gate: predicted load %.2f exceeds threshold %.2f, only RAN workloads scheduled", prediction.PredictedLoad, a.config.TrafficHighLoadThreshold),
				OriginalAction:   ActionScaleUp,
				OriginalReplicas: &w.CurrentReplicas,
				PredictedPeakEnd: nextSafeSchedulingTime,
			})

			// Throttle running workloads: scale down to minimum replicas so RAN
			// workloads have headroom, then resume at the predicted safe window.
			if w.CurrentReplicas > w.MinReplicas {
				minReplicas := w.MinReplicas
				directives = append(directives, ScalingDirective{
					WorkloadID:    w.ID,
					Namespace:     w.Namespace,
					Action:        ActionScaleDown,
					ScalingType:   ScalingHorizontal,
					NewReplicas:   &minReplicas,
					Priority:      int(w.QoSLevel),
					Reason:        fmt.Sprintf("Scheduler gate active (load=%.2f >= %.2f): throttling non-RAN workload to min replicas (%d), resuming at %s", prediction.PredictedLoad, a.config.TrafficHighLoadThreshold, minReplicas, deferUntil.Format(time.RFC3339)),
					PostponeUntil: &deferUntil,
				})
				klog.Infof("SCHEDULER GATE: throttling running non-RAN workload %s to min replicas (%d), resuming at %s",
					w.ID, minReplicas, deferUntil.Format(time.RFC3339))
			} else {
				directives = append(directives, ScalingDirective{
					WorkloadID:    w.ID,
					Namespace:     w.Namespace,
					Action:        ActionPostpone,
					ScalingType:   ScalingHorizontal,
					Priority:      int(w.QoSLevel),
					Reason:        fmt.Sprintf("Scheduler gate active (load=%.2f >= %.2f): non-RAN scheduling deferred to %s", prediction.PredictedLoad, a.config.TrafficHighLoadThreshold, deferUntil.Format(time.RFC3339)),
					PostponeUntil: &deferUntil,
				})
				klog.Infof("SCHEDULER GATE: deferred non-RAN workload %s until %s", w.ID, deferUntil.Format(time.RFC3339))
			}
			continue
		}

		directive := a.processNonRANWorkload(ctx, w, m, cluster, prediction)
		if directive != nil {
			// Validate node placement for non-RAN workload
			if err := a.validateNonRANPlacement(directive, &w, cluster); err != nil {
				klog.Warningf("Non-RAN placement validation failed for %s: %v", w.ID, err)
				continue
			}
			directives = append(directives, *directive)
		}
	}

	// Sort directives by priority
	sort.Slice(directives, func(i, j int) bool {
		return directives[i].Priority < directives[j].Priority
	})

	return directives, nil
}

// checkDeferredWorkloads checks if any deferred workloads can now be scheduled
// Called when traffic has normalized after a peak
func (a *Allocator) checkDeferredWorkloads(prediction *TrafficPrediction) []ScalingDirective {
	a.deferredMutex.Lock()
	defer a.deferredMutex.Unlock()

	var directives []ScalingDirective
	now := time.Now()

	for id, deferred := range a.deferredWorkloads {
		canResume := false

		// Check if deferral period has passed
		if now.After(deferred.DeferUntil) {
			canResume = true
		}

		// Check if traffic peak has ended
		if prediction != nil && !prediction.IsInTrafficPeak() {
			if deferred.PredictedPeakEnd != nil && now.After(*deferred.PredictedPeakEnd) {
				canResume = true
			}
		}

		// Check if prediction no longer shows peak imminent
		if prediction != nil && prediction.Confidence >= 0.5 {
			if !prediction.PeakExpected && !prediction.IsTrafficPeakImminent(15*time.Minute) {
				canResume = true
			}
		}

		if canResume {
			klog.Infof("Traffic normalized, resuming deferred workload %s", id)

			directive := ScalingDirective{
				WorkloadID:  id,
				Namespace:   deferred.Namespace,
				Action:      deferred.OriginalAction,
				ScalingType: ScalingHorizontal, // Non-RAN uses horizontal
				NewReplicas: deferred.OriginalReplicas,
				Priority:    int(QoSLow),
				Reason:      fmt.Sprintf("Resuming deferred workload after traffic normalized (was deferred at %s)", deferred.DeferredAt.Format(time.RFC3339)),
			}
			directives = append(directives, directive)

			// Remove from deferred list
			delete(a.deferredWorkloads, id)
		}
	}

	return directives
}

// isDeferred checks if a workload is currently deferred
func (a *Allocator) isDeferred(workloadID string) bool {
	a.deferredMutex.RLock()
	defer a.deferredMutex.RUnlock()
	_, ok := a.deferredWorkloads[workloadID]
	return ok
}

// deferWorkload adds a workload to the deferred list
func (a *Allocator) deferWorkload(deferred *DeferredWorkload) {
	a.deferredMutex.Lock()
	defer a.deferredMutex.Unlock()
	a.deferredWorkloads[deferred.WorkloadID] = deferred
	klog.V(2).Infof("Deferred workload %s until %s: %s",
		deferred.WorkloadID, deferred.DeferUntil.Format(time.RFC3339), deferred.Reason)
}

// validateRANPlacement ensures RAN workload is placed on node with "ran" label
func (a *Allocator) validateRANPlacement(directive *ScalingDirective, cluster *ClusterState) error {
	if directive.TargetNode == "" {
		return nil // No specific node target, placement will be handled by scheduler
	}

	for _, node := range cluster.Nodes {
		if node.Name == directive.TargetNode {
			if !node.IsRANNode() {
				return fmt.Errorf("node %s does not have 'ran' label required for RAN workloads", node.Name)
			}
			return nil
		}
	}
	return fmt.Errorf("target node %s not found in cluster", directive.TargetNode)
}

// validateNonRANPlacement ensures non-RAN workload is placed on appropriate node
// - GPU workloads: "gpu" label (or "computing" if fallback)
// - Computing workloads: "computing" label
// - Must NOT be placed on "ran" labeled nodes (to not disrupt RAN)
func (a *Allocator) validateNonRANPlacement(directive *ScalingDirective, w *WorkloadSpec, cluster *ClusterState) error {
	if directive.TargetNode == "" && directive.FallbackToNode == "" {
		return nil // Scheduler will handle placement
	}

	targetNode := directive.TargetNode
	if targetNode == "" {
		targetNode = directive.FallbackToNode
	}

	for _, node := range cluster.Nodes {
		if node.Name == targetNode {
			// Non-RAN workloads should NOT be on RAN nodes
			if node.IsRANNode() && !node.IsComputingNode() {
				return fmt.Errorf("node %s is RAN-only, cannot place non-RAN workload", node.Name)
			}

			// Check if workload can run on this node
			if !w.CanRunOnNode(&node) {
				return fmt.Errorf("workload %s cannot run on node %s: resource or label mismatch", w.ID, node.Name)
			}

			return nil
		}
	}
	return fmt.Errorf("target node %s not found in cluster", targetNode)
}

// detectRANDegradation checks if any RAN workload is experiencing degradation
func (a *Allocator) detectRANDegradation(workloads []WorkloadSpec, metrics map[string]WorkloadMetrics) bool {
	for _, w := range workloads {
		if !w.IsRAN() {
			continue
		}
		m, ok := metrics[w.ID]
		if !ok {
			continue
		}
		if m.IsDegraded(a.config.CPUSaturationThreshold) {
			klog.V(2).Infof("RAN workload %s is degraded: latency=%v, cpu=%.2f",
				w.ID, m.CurrentLatency, m.CPUUtilization)
			return true
		}
	}
	return false
}

// reactiveNonRANReduction reduces non-RAN workloads when RAN degradation is detected
func (a *Allocator) reactiveNonRANReduction(
	workloads []WorkloadSpec,
	metrics map[string]WorkloadMetrics,
	cluster *ClusterState,
) []ScalingDirective {
	var directives []ScalingDirective

	for _, w := range workloads {
		if w.IsRAN() {
			continue // Only affect non-RAN workloads
		}

		// Scale down replicas to free resources for RAN
		if w.CurrentReplicas > w.MinReplicas {
			newReplicas := int32(math.Max(float64(w.MinReplicas), float64(w.CurrentReplicas)*0.5))

			directives = append(directives, ScalingDirective{
				WorkloadID:  w.ID,
				Namespace:   w.Namespace,
				Action:      ActionScaleDown,
				ScalingType: ScalingHorizontal,
				NewReplicas: &newReplicas,
				Priority:    1, // High priority for reactive reduction
				Reason:      "Reactive reduction due to RAN degradation - freeing resources for RAN SLA",
			})
			klog.V(2).Infof("Reactive scale-down: %s from %d to %d replicas",
				w.ID, w.CurrentReplicas, newReplicas)
		}
	}

	return directives
}

// processRANWorkload handles RAN workloads (srsran, ricplt, ricxapp).
// VERTICAL scaling only, placed on nodes with "ran" label.
//
// Capacity guarantee: both QoSHigh and QoSMedium RAN workloads ALWAYS get
// the resources they need.  When no RAN node has free capacity, resources are
// reclaimed from QoSLow (non-RAN) workloads via preemption.  The preemption
// priority reflects the QoS tier: QoSHigh (priority 0) preempts before
// QoSMedium (priority 1), so the most critical functions are served first.
//
// Resource sizing uses traffic prediction at EVERY reconciliation cycle — not
// only on first placement or when a latency constraint is already violated.
// This means the workload is right-sized proactively before any SLA breach:
//  1. Initial placement  → proactive sizing with DT prediction.
//  2. Ongoing cycles     → proactive pre-scaling whenever prediction says
//     resources will be needed in the next hour.
//  3. Reactive fallback  → scale-up when latency deviation is observed
//     (handles cases where prediction underestimated load).
//  4. Scale-down guard   → only scale down when traffic prediction is also low,
//     to avoid thrashing during predicted peaks.
func (a *Allocator) processRANWorkload(
	ctx context.Context,
	w WorkloadSpec,
	m WorkloadMetrics,
	cluster *ClusterState,
	prediction *TrafficPrediction,
) *ScalingDirective {

	// PROACTIVE: Compute needed resources using DT prediction at every cycle.
	// computeScalingRatio incorporates both the current latency ratio and the
	// predicted load increase, so even with deviation=0 it returns >1 when
	// traffic is forecast to grow.
	proactiveNeeded := a.computeVerticalScaleUp(w, m, 0, prediction)

	// --- Phase A: Initial placement (no node assigned yet) ---
	if w.CurrentNode == "" {
		targetNode := a.findRANNode(cluster, proactiveNeeded)
		if targetNode != "" {
			klog.Infof("PROACTIVE: Initial RAN placement for %s on node %s (predicted load=%.2f)",
				w.ID, targetNode, prediction.PredictedLoad)
			return &ScalingDirective{
				WorkloadID:   w.ID,
				Namespace:    w.Namespace,
				Action:       ActionScaleUp,
				ScalingType:  ScalingVertical,
				NewResources: &proactiveNeeded,
				TargetNode:   targetNode,
				Priority:     int(w.QoSLevel),
				Reason:       fmt.Sprintf("Proactive initial placement on RAN node %s (predicted load=%.2f, confidence=%.2f)", targetNode, prediction.PredictedLoad, prediction.Confidence),
			}
		}
		// No RAN node has enough capacity — preempt non-RAN workloads.
		// Both QoSHigh and QoSMedium are guaranteed capacity; priority reflects tier.
		if w.IsRAN() {
			return &ScalingDirective{
				WorkloadID:   w.ID,
				Namespace:    w.Namespace,
				Action:       ActionPreempt,
				ScalingType:  ScalingVertical,
				NewResources: &proactiveNeeded,
				Priority:     int(w.QoSLevel) - 1, // QoSHigh→0, QoSMedium→1
				Reason:       fmt.Sprintf("%s RAN initial placement: no free RAN node capacity, preempting non-RAN workloads", w.QoSLevel),
			}
		}
		return nil
	}

	// --- Phase B: Proactive ongoing pre-scaling ---
	// If the prediction-aware calculation says we need more resources than
	// currently allocated, scale up NOW — before latency degrades.
	if proactiveNeeded.CPU.Cmp(w.CurrentResources.CPU) > 0 ||
		proactiveNeeded.Memory.Cmp(w.CurrentResources.Memory) > 0 {
		targetNode := a.findRANNode(cluster, proactiveNeeded)
		if targetNode != "" {
			klog.Infof("PROACTIVE: Pre-scaling RAN workload %s (predicted load=%.2f, confidence=%.2f)",
				w.ID, prediction.PredictedLoad, prediction.Confidence)
			return &ScalingDirective{
				WorkloadID:   w.ID,
				Namespace:    w.Namespace,
				Action:       ActionScaleUp,
				ScalingType:  ScalingVertical,
				NewResources: &proactiveNeeded,
				TargetNode:   targetNode,
				Priority:     int(w.QoSLevel),
				Reason:       fmt.Sprintf("Proactive pre-scaling: traffic prediction (load=%.2f, confidence=%.2f) requires more resources", prediction.PredictedLoad, prediction.Confidence),
			}
		}
		if w.IsRAN() {
			return &ScalingDirective{
				WorkloadID:   w.ID,
				Namespace:    w.Namespace,
				Action:       ActionPreempt,
				ScalingType:  ScalingVertical,
				NewResources: &proactiveNeeded,
				Priority:     int(w.QoSLevel) - 1, // QoSHigh→0, QoSMedium→1
				Reason:       fmt.Sprintf("%s RAN proactive pre-scaling: no free RAN node capacity, preempting non-RAN workloads to meet predicted demand", w.QoSLevel),
			}
		}
	}

	// --- Phase C: Reactive scale-up (latency already degraded) ---
	// Handles situations where the prediction underestimated actual load.
	deviation := a.computeDeviation(m)
	klog.V(3).Infof("RAN workload %s (ns=%s) deviation: %.3f", w.ID, w.Namespace, deviation)

	if deviation > a.config.ScaleUpThreshold {
		needed := a.computeVerticalScaleUp(w, m, deviation, prediction)
		targetNode := a.findRANNode(cluster, needed)
		if targetNode != "" {
			return &ScalingDirective{
				WorkloadID:   w.ID,
				Namespace:    w.Namespace,
				Action:       ActionScaleUp,
				ScalingType:  ScalingVertical,
				NewResources: &needed,
				TargetNode:   targetNode,
				Priority:     int(w.QoSLevel),
				Reason:       fmt.Sprintf("Reactive RAN scale-up: latency deviation %.2f on node %s", deviation, targetNode),
			}
		}
		if w.IsRAN() {
			return &ScalingDirective{
				WorkloadID:   w.ID,
				Namespace:    w.Namespace,
				Action:       ActionPreempt,
				ScalingType:  ScalingVertical,
				NewResources: &needed,
				Priority:     int(w.QoSLevel) - 1, // QoSHigh→0, QoSMedium→1
				Reason:       fmt.Sprintf("%s RAN latency violated: no free RAN node capacity, preempting non-RAN workloads", w.QoSLevel),
			}
		}

	} else if deviation < -a.config.ScaleDownThreshold {
		// --- Phase D: Scale-down guard ---
		// Only release resources when the traffic prediction also shows a low/stable
		// load; skip if a traffic peak is forecast to avoid immediate re-scale-up.
		highTrafficPredicted := prediction != nil &&
			prediction.Confidence >= a.config.MinConfidenceThreshold &&
			prediction.PredictedLoad >= a.config.TrafficHighLoadThreshold
		if highTrafficPredicted {
			klog.V(3).Infof("RAN workload %s: deviation %.2f suggests scale-down but traffic prediction high (load=%.2f), holding resources",
				w.ID, deviation, prediction.PredictedLoad)
		} else {
			newResources := a.computeVerticalScaleDown(w, deviation)
			if !resourcesEqual(newResources, w.CurrentResources) {
				return &ScalingDirective{
					WorkloadID:   w.ID,
					Namespace:    w.Namespace,
					Action:       ActionScaleDown,
					ScalingType:  ScalingVertical,
					NewResources: &newResources,
					Priority:     int(w.QoSLevel) + 10,
					Reason:       "RAN utilization below threshold and traffic prediction low: vertical scale-down",
				}
			}
		}
	}

	return nil
}

// processNonRANWorkload handles non-RAN workloads (ai-workloads namespace)
// HORIZONTAL scaling only, placed on nodes with "computing" or "gpu" labels.
//
// Traffic prediction is checked FIRST to determine GPU eligibility and
// whether the workload should be migrated before any deviation-based logic:
//
//  1. Traffic check  → If DT predicts high load for the next hour, GPU is
//     withheld. If the workload is currently on a GPU node it
//     is migrated proactively to a computing (non-RAN) node.
//  2. Peak deferral  → If a traffic peak is imminent/ongoing and the workload
//     is marked PostponeDuringPeak, defer scaling until the
//     predicted peak end time.
//  3. Normal scaling → Horizontal scale-up/down; node selection remains
//     prediction-aware (selectNonRANNode withholds GPU when
//     traffic is high).
func (a *Allocator) processNonRANWorkload(
	ctx context.Context,
	w WorkloadSpec,
	m WorkloadMetrics,
	cluster *ClusterState,
	prediction *TrafficPrediction,
) *ScalingDirective {

	// Step 1: ALWAYS evaluate traffic prediction for the next hour FIRST.
	// This controls GPU access and may trigger a proactive migration.
	highTrafficPredicted := prediction != nil &&
		prediction.Confidence >= a.config.MinConfidenceThreshold &&
		prediction.PredictedLoad >= a.config.TrafficHighLoadThreshold

	// If high traffic is forecast and this workload currently occupies a GPU
	// node, migrate it to a computing (non-RAN) node to free the GPU for RAN.
	if highTrafficPredicted && w.RequiresGPU && w.CurrentNode != "" {
		computingNode := a.findComputingNode(cluster, w)
		if computingNode != "" && computingNode != w.CurrentNode {
			klog.Infof("TRAFFIC PREDICTION: Migrating non-RAN GPU workload %s from %s to computing node %s "+
				"(predicted load=%.2f >= %.2f, GPU reserved for RAN)",
				w.ID, w.CurrentNode, computingNode, prediction.PredictedLoad, a.config.TrafficHighLoadThreshold)
			return &ScalingDirective{
				WorkloadID:  w.ID,
				Namespace:   w.Namespace,
				Action:      ActionScaleUp,
				ScalingType: ScalingHorizontal,
				TargetNode:  computingNode,
				Priority:    int(w.QoSLevel),
				Reason: fmt.Sprintf("High traffic predicted (load=%.2f >= %.2f): releasing GPU, migrating to computing node %s",
					prediction.PredictedLoad, a.config.TrafficHighLoadThreshold, computingNode),
			}
		}
	}

	deviation := a.computeDeviation(m)
	klog.V(3).Infof("Non-RAN workload %s deviation: %.3f", w.ID, deviation)

	// Step 2: Peak deferral — if a peak is imminent/ongoing and the workload
	// allows postponement, defer the scale-up to the predicted peak end time.
	if w.PostponeDuringPeak && prediction != nil && prediction.Confidence >= a.config.MinConfidenceThreshold {
		if prediction.IsTrafficPeakImminent(a.config.TrafficPeakPostponeWindow) || prediction.IsInTrafficPeak() {
			if deviation > a.config.ScaleUpThreshold {
				deferUntil := time.Now().Add(a.config.TrafficPeakPostponeWindow)
				if prediction.PeakEndTime != nil {
					deferUntil = *prediction.PeakEndTime
				}

				targetReplicas := a.computeHorizontalScaleUp(w, deviation)
				a.deferWorkload(&DeferredWorkload{
					WorkloadID:       w.ID,
					Namespace:        w.Namespace,
					DeferredAt:       time.Now(),
					DeferUntil:       deferUntil,
					Reason:           "RAN traffic peak predicted - deferring to protect RAN SLA",
					OriginalAction:   ActionScaleUp,
					OriginalReplicas: &targetReplicas,
					PredictedPeakEnd: prediction.PeakEndTime,
				})
				klog.Infof("PROACTIVE: Deferring non-RAN workload %s until %s (traffic peak predicted)",
					w.ID, deferUntil.Format(time.RFC3339))
				return &ScalingDirective{
					WorkloadID:    w.ID,
					Namespace:     w.Namespace,
					Action:        ActionPostpone,
					ScalingType:   ScalingHorizontal,
					Priority:      int(w.QoSLevel),
					Reason:        fmt.Sprintf("Scaling postponed until %s - RAN traffic peak predicted", deferUntil.Format(time.RFC3339)),
					PostponeUntil: &deferUntil,
				}
			}
		}
	}

	// Step 3: Normal horizontal scaling.
	// selectNonRANNode is prediction-aware: when DT predicts high traffic the
	// GPU stays reserved for RAN and the workload is placed on a computing node.
	if deviation > a.config.ScaleUpThreshold {
		newReplicas := a.computeHorizontalScaleUp(w, deviation)
		if newReplicas > w.CurrentReplicas && newReplicas <= w.MaxReplicas {
			targetNode := a.selectNonRANNode(cluster, w, prediction)
			if targetNode == "" && w.RequiresGPU && !w.GPUFallbackCPU {
				klog.Warningf("No suitable node for GPU workload %s (GPU unavailable or withheld, CPU fallback disabled)", w.ID)
				return nil
			}
			return &ScalingDirective{
				WorkloadID:  w.ID,
				Namespace:   w.Namespace,
				Action:      ActionScaleUp,
				ScalingType: ScalingHorizontal,
				NewReplicas: &newReplicas,
				TargetNode:  targetNode,
				Priority:    int(w.QoSLevel),
				Reason: fmt.Sprintf("Non-RAN horizontal scale-up from %d to %d replicas on node %s",
					w.CurrentReplicas, newReplicas, targetNode),
			}
		}
	} else if deviation < -a.config.ScaleDownThreshold {
		newReplicas := a.computeHorizontalScaleDown(w, deviation)
		if newReplicas < w.CurrentReplicas && newReplicas >= w.MinReplicas {
			return &ScalingDirective{
				WorkloadID:  w.ID,
				Namespace:   w.Namespace,
				Action:      ActionScaleDown,
				ScalingType: ScalingHorizontal,
				NewReplicas: &newReplicas,
				Priority:    int(w.QoSLevel) + 10,
				Reason: fmt.Sprintf("Non-RAN horizontal scale-down from %d to %d replicas",
					w.CurrentReplicas, newReplicas),
			}
		}
	}

	return nil
}

// findNextLowTrafficTime scans future hourly predictions (2h … MaxScheduleLookAheadHours)
// and returns the first time at which predicted load drops below TrafficHighLoadThreshold.
// Returns nil if no such window is found within the look-ahead horizon.
func (a *Allocator) findNextLowTrafficTime(ctx context.Context) *time.Time {
	for m := 10; m <= a.config.MaxScheduleLookAheadMinutes; m += 10 {
		horizon := time.Duration(m) * time.Minute
		pred, err := a.digitalTwin.GetPrediction(ctx, horizon)
		if err != nil {
			klog.V(3).Infof("Could not get %dh prediction for look-ahead: %v", m, err)
			continue
		}
		if pred.Confidence >= a.config.MinConfidenceThreshold &&
			pred.PredictedLoad < a.config.TrafficHighLoadThreshold {
			t := time.Now().Add(horizon)
			klog.V(2).Infof("Low-traffic window at +%dh (predicted load=%.2f < threshold=%.2f)",
				m, pred.PredictedLoad, a.config.TrafficHighLoadThreshold)
			return &t
		}
	}
	return nil
}

// selectNonRANNode picks the best node for a non-RAN workload using the DT prediction:
//   - If high traffic is predicted (load >= TrafficHighLoadThreshold): GPU is reserved
//     for RAN workloads → place on a computing (CPU-only) node instead.
//   - If traffic is low: GPU can be assigned; falls back to computing if GPU unavailable
//     and GPUFallbackCPU is true.
//   - Non-GPU workloads always go to the computing node.
func (a *Allocator) selectNonRANNode(cluster *ClusterState, w WorkloadSpec, prediction *TrafficPrediction) string {
	if !w.RequiresGPU {
		return a.findComputingNode(cluster, w)
	}

	highTraffic := prediction != nil &&
		prediction.Confidence >= a.config.MinConfidenceThreshold &&
		prediction.PredictedLoad >= a.config.TrafficHighLoadThreshold

	if highTraffic {
		klog.Infof("High traffic predicted (load=%.2f >= %.2f): withholding GPU from non-RAN workload %s, using computing node",
			prediction.PredictedLoad, a.config.TrafficHighLoadThreshold, w.ID)
		return a.findComputingNode(cluster, w)
	}

	// Low traffic: GPU can be given to non-RAN
	gpuNode := a.findGPUNode(cluster)
	if gpuNode != "" {
		return gpuNode
	}

	// GPU node not available
	if w.GPUFallbackCPU {
		klog.Infof("GPU unavailable for non-RAN workload %s, falling back to computing node", w.ID)
		return a.findComputingNode(cluster, w)
	}
	return ""
}

// Node finding functions based on labels

// findRANNode finds a node with "ran" label that has sufficient resources
func (a *Allocator) findRANNode(cluster *ClusterState, needed Resources) string {
	if cluster == nil {
		return ""
	}

	for _, node := range cluster.Nodes {
		if node.IsRANNode() {
			if node.Available.CPU.Cmp(needed.CPU) >= 0 &&
				node.Available.Memory.Cmp(needed.Memory) >= 0 {
				return node.Name
			}
		}
	}
	return ""
}

// findGPUNode finds a node with "gpu" label that has available GPU
func (a *Allocator) findGPUNode(cluster *ClusterState) string {
	if cluster == nil {
		return ""
	}

	for _, node := range cluster.Nodes {
		if node.IsGPUNode() && node.GPUAvailable {
			return node.Name
		}
	}
	return ""
}

// findComputingNode finds a node with "computing" label for non-RAN workloads
func (a *Allocator) findComputingNode(cluster *ClusterState, w WorkloadSpec) string {
	if cluster == nil {
		return ""
	}

	for _, node := range cluster.Nodes {
		if node.IsComputingNode() && !node.IsRANNode() {
			// Check resource availability
			if node.Available.CPU.Cmp(w.MinResources.CPU) >= 0 &&
				node.Available.Memory.Cmp(w.MinResources.Memory) >= 0 {
				return node.Name
			}
		}
	}
	return ""
}

// Computation functions

func (a *Allocator) computeDeviation(m WorkloadMetrics) float64 {
	if m.TargetLatency == 0 {
		return 0
	}
	return float64(m.CurrentLatency-m.TargetLatency) / float64(m.TargetLatency)
}

func (a *Allocator) computeScalingRatio(m WorkloadMetrics, prediction *TrafficPrediction) float64 {
	if m.TargetLatency == 0 {
		return 1.0
	}

	baseRatio := math.Pow(
		float64(m.CurrentLatency)/float64(m.TargetLatency),
		a.config.ScalingSensitivity,
	)

	predictionFactor := 1.0
	if prediction != nil && prediction.Confidence >= a.config.MinConfidenceThreshold {
		currentLoad := (m.CPUUtilization + m.GPUUtilization) / 2
		loadIncrease := prediction.PredictedLoad - currentLoad
		if loadIncrease > 0 {
			predictionFactor = 1.0 + a.config.PredictionWeight*loadIncrease
		}
	}

	return baseRatio * predictionFactor
}

func (a *Allocator) computeVerticalScaleUp(w WorkloadSpec, m WorkloadMetrics, deviation float64, prediction *TrafficPrediction) Resources {
	ratio := a.computeScalingRatio(m, prediction)

	newResources := Resources{
		CPU:    multiplyQuantity(w.CurrentResources.CPU, ratio),
		Memory: multiplyQuantity(w.CurrentResources.Memory, ratio),
		GPU:    w.CurrentResources.GPU,
	}

	return clampResources(newResources, w.MinResources, w.MaxResources)
}

func (a *Allocator) computeVerticalScaleDown(w WorkloadSpec, deviation float64) Resources {
	scalingRatio := math.Pow(1+deviation, a.config.ScalingSensitivity)

	newResources := Resources{
		CPU:    multiplyQuantity(w.CurrentResources.CPU, scalingRatio),
		Memory: multiplyQuantity(w.CurrentResources.Memory, scalingRatio),
		GPU:    w.CurrentResources.GPU,
	}

	return clampResources(newResources, w.MinResources, w.MaxResources)
}

func (a *Allocator) computeHorizontalScaleUp(w WorkloadSpec, deviation float64) int32 {
	scalingRatio := math.Pow(1+deviation, a.config.ScalingSensitivity)
	newReplicas := int32(math.Ceil(float64(w.CurrentReplicas) * scalingRatio))

	if newReplicas > w.MaxReplicas {
		newReplicas = w.MaxReplicas
	}
	if newReplicas < w.MinReplicas {
		newReplicas = w.MinReplicas
	}

	return newReplicas
}

func (a *Allocator) computeHorizontalScaleDown(w WorkloadSpec, deviation float64) int32 {
	scalingRatio := math.Pow(1+deviation, a.config.ScalingSensitivity)
	newReplicas := int32(math.Floor(float64(w.CurrentReplicas) * scalingRatio))

	if newReplicas < w.MinReplicas {
		newReplicas = w.MinReplicas
	}
	if newReplicas > w.MaxReplicas {
		newReplicas = w.MaxReplicas
	}

	return newReplicas
}

// GetDeferredWorkloads returns list of currently deferred workloads
func (a *Allocator) GetDeferredWorkloads() []DeferredWorkload {
	a.deferredMutex.RLock()
	defer a.deferredMutex.RUnlock()

	result := make([]DeferredWorkload, 0, len(a.deferredWorkloads))
	for _, d := range a.deferredWorkloads {
		result = append(result, *d)
	}
	return result
}

// Helper functions

func sortByPriority(workloads []WorkloadSpec) {
	sort.Slice(workloads, func(i, j int) bool {
		return workloads[i].QoSLevel < workloads[j].QoSLevel
	})
}

func filterByQoS(workloads []WorkloadSpec, qos QoSLevel) []WorkloadSpec {
	var result []WorkloadSpec
	for _, w := range workloads {
		if w.QoSLevel == qos {
			result = append(result, w)
		}
	}
	return result
}

func workloadIDs(workloads []WorkloadSpec) []string {
	ids := make([]string, len(workloads))
	for i, w := range workloads {
		ids[i] = w.ID
	}
	return ids
}

func multiplyQuantity(q resource.Quantity, factor float64) resource.Quantity {
	value := q.AsApproximateFloat64()
	newValue := value * factor
	return *resource.NewMilliQuantity(int64(newValue*1000), resource.DecimalSI)
}

func clampResources(r, min, max Resources) Resources {
	return Resources{
		CPU:    clampQuantity(r.CPU, min.CPU, max.CPU),
		Memory: clampQuantity(r.Memory, min.Memory, max.Memory),
		GPU:    r.GPU,
	}
}

func clampQuantity(q, min, max resource.Quantity) resource.Quantity {
	if q.Cmp(min) < 0 {
		return min.DeepCopy()
	}
	if q.Cmp(max) > 0 {
		return max.DeepCopy()
	}
	return q.DeepCopy()
}

func resourcesEqual(a, b Resources) bool {
	return a.CPU.Cmp(b.CPU) == 0 && a.Memory.Cmp(b.Memory) == 0
}
