// Package allocation implements the QoS-Aware Resource Allocation Function
// for the AI-RAN orchestrator.
package allocation

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Namespace constants for QoS classification
const (
	NamespaceSRSRAN     = "srsran"
	NamespaceRICPlt     = "ricplt"
	NamespaceRICXApp    = "ricxapp"
	NamespaceAIWorkload = "ai-workloads"
)

// QoSLevel represents workload priority classification
type QoSLevel int

const (
	// QoSHigh is for critical RAN workloads (srsran namespace)
	// Vertical scaling only, preemption rights
	QoSHigh QoSLevel = 1

	// QoSMedium is for Near-RT RIC and xApps (ricplt, ricxapp namespaces)
	// Vertical scaling only
	QoSMedium QoSLevel = 2

	// QoSLow is for non-RAN AI workloads (ai-workloads namespace)
	// Horizontal scaling only, best-effort, subject to preemption
	QoSLow QoSLevel = 3
)

// String returns the string representation of QoSLevel
func (q QoSLevel) String() string {
	switch q {
	case QoSHigh:
		return "high"
	case QoSMedium:
		return "medium"
	case QoSLow:
		return "low"
	default:
		return "unknown"
	}
}

// GetQoSLevelFromNamespace determines QoS level based on namespace
func GetQoSLevelFromNamespace(namespace string) QoSLevel {
	switch namespace {
	case NamespaceSRSRAN:
		return QoSHigh
	case NamespaceRICPlt, NamespaceRICXApp:
		return QoSMedium
	case NamespaceAIWorkload:
		return QoSLow
	default:
		// Default to low for unknown namespaces
		return QoSLow
	}
}

// IsRANNamespace returns true if the namespace contains RAN workloads
func IsRANNamespace(namespace string) bool {
	switch namespace {
	case NamespaceSRSRAN, NamespaceRICPlt, NamespaceRICXApp:
		return true
	default:
		return false
	}
}

// GetAllowedScalingType returns the allowed scaling type for a namespace
func GetAllowedScalingType(namespace string) ScalingType {
	if IsRANNamespace(namespace) {
		return ScalingVertical // RAN workloads: vertical only
	}
	return ScalingHorizontal // Non-RAN workloads: horizontal only
}

// ScalingType defines the scaling strategy
type ScalingType string

const (
	// ScalingVertical resizes resources within the same pod/node
	// Used for RAN workloads (srsran, ricplt, ricxapp)
	ScalingVertical ScalingType = "vertical"

	// ScalingHorizontal adds/removes replicas
	// Used for non-RAN workloads (ai-workloads)
	ScalingHorizontal ScalingType = "horizontal"
)

// ScalingAction represents the action to take
type ScalingAction string

const (
	ActionScaleUp   ScalingAction = "scale-up"
	ActionScaleDown ScalingAction = "scale-down"
	ActionNoOp      ScalingAction = "no-op"
	ActionPreempt   ScalingAction = "preempt"
	ActionPostpone  ScalingAction = "postpone" // For traffic prediction-based deferral
)

// Resources represents compute resources
type Resources struct {
	CPU    resource.Quantity `json:"cpu"`
	Memory resource.Quantity `json:"memory"`
	GPU    GPUResources      `json:"gpu,omitempty"`
}

// GPUResources represents GPU allocation details
type GPUResources struct {
	// Count is the number of GPUs (for non-MIG)
	Count int `json:"count,omitempty"`
	// MIGSlice is the MIG partition profile (e.g., "1g.5gb", "2g.10gb")
	MIGSlice string `json:"migSlice,omitempty"`
	// Memory is the GPU memory requirement
	Memory resource.Quantity `json:"memory,omitempty"`
}

// WorkloadMetrics contains current performance metrics for a workload
type WorkloadMetrics struct {
	// WorkloadID is the unique identifier
	WorkloadID string `json:"workloadId"`
	// Namespace for QoS determination
	Namespace string `json:"namespace"`
	// CurrentLatency is the observed latency
	CurrentLatency time.Duration `json:"currentLatency"`
	// TargetLatency is the SLA target
	TargetLatency time.Duration `json:"targetLatency"`
	// CPUUtilization is the current CPU usage (0-1)
	CPUUtilization float64 `json:"cpuUtilization"`
	// GPUUtilization is the current GPU usage (0-1)
	GPUUtilization float64 `json:"gpuUtilization"`
	// MemoryUsage in bytes
	MemoryUsage int64 `json:"memoryUsage"`
	// Timestamp of the metrics
	Timestamp time.Time `json:"timestamp"`
}

// IsDegraded returns true if the workload is experiencing performance degradation
func (m *WorkloadMetrics) IsDegraded(cpuThreshold float64) bool {
	// Degradation: latency exceeds target OR CPU is saturated
	latencyDegraded := m.TargetLatency > 0 && m.CurrentLatency > m.TargetLatency
	cpuSaturated := m.CPUUtilization > cpuThreshold
	return latencyDegraded || cpuSaturated
}

// WorkloadSpec defines workload requirements and constraints
type WorkloadSpec struct {
	// ID is the unique identifier
	ID string `json:"id"`
	// Name is the workload name
	Name string `json:"name"`
	// Namespace is the Kubernetes namespace (determines QoS and scaling type)
	Namespace string `json:"namespace"`
	// QoSLevel is derived from namespace
	QoSLevel QoSLevel `json:"qosLevel"`

	// Resource constraints
	MinResources     Resources `json:"minResources"`
	MaxResources     Resources `json:"maxResources"`
	CurrentResources Resources `json:"currentResources"`

	// SLA targets
	TargetLatency time.Duration `json:"targetLatency"`

	// Scaling configuration (derived from namespace)
	// RAN namespaces: vertical only
	// Non-RAN namespaces: horizontal only
	MinReplicas     int32 `json:"minReplicas"`
	MaxReplicas     int32 `json:"maxReplicas"`
	CurrentReplicas int32 `json:"currentReplicas"`

	// Node placement with label-based selection
	CurrentNode  string            `json:"currentNode,omitempty"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// RequiredNodeLabels specifies required node labels (gpu, ran, computing)
	RequiredNodeLabels []string `json:"requiredNodeLabels,omitempty"`

	// GPU requirements
	RequiresGPU    bool `json:"requiresGpu"`
	GPUFallbackCPU bool `json:"gpuFallbackCpu"` // Allow CPU fallback if GPU unavailable

	// Proactive scheduling settings
	PostponeDuringPeak bool `json:"postponeDuringPeak"` // Defer during traffic peaks
}

// IsRAN returns true if this is a RAN workload
func (w *WorkloadSpec) IsRAN() bool {
	return IsRANNamespace(w.Namespace)
}

// GetAllowedScaling returns the allowed scaling type for this workload
func (w *WorkloadSpec) GetAllowedScaling() ScalingType {
	return GetAllowedScalingType(w.Namespace)
}

// CanScaleVertically returns true if vertical scaling is allowed
func (w *WorkloadSpec) CanScaleVertically() bool {
	return w.IsRAN()
}

// CanScaleHorizontally returns true if horizontal scaling is allowed
func (w *WorkloadSpec) CanScaleHorizontally() bool {
	return !w.IsRAN()
}

// GetRequiredNodeLabels returns the node labels required for this workload
func (w *WorkloadSpec) GetRequiredNodeLabels() []string {
	if len(w.RequiredNodeLabels) > 0 {
		return w.RequiredNodeLabels
	}
	// Default labels based on workload type
	if w.IsRAN() {
		return []string{NodeLabelRAN}
	}
	if w.RequiresGPU {
		return []string{NodeLabelGPU, NodeLabelComputing}
	}
	return []string{NodeLabelComputing}
}

// CanRunOnNode checks if workload can run on a given node based on labels and resources
func (w *WorkloadSpec) CanRunOnNode(node *NodeState) bool {
	// Check label requirements
	for _, requiredLabel := range w.GetRequiredNodeLabels() {
		if _, ok := node.Labels[requiredLabel]; !ok {
			return false
		}
	}

	// Check GPU requirement
	if w.RequiresGPU && !w.GPUFallbackCPU {
		if !node.IsGPUNode() || !node.GPUAvailable {
			return false
		}
	}

	// Check resource availability
	if node.Available.CPU.Cmp(w.MinResources.CPU) < 0 {
		return false
	}
	if node.Available.Memory.Cmp(w.MinResources.Memory) < 0 {
		return false
	}

	return true
}

// ScalingDirective is the output of the allocation function
type ScalingDirective struct {
	// WorkloadID identifies the target workload
	WorkloadID string `json:"workloadId"`
	// Namespace for the workload
	Namespace string `json:"namespace"`
	// Action is the scaling action to perform
	Action ScalingAction `json:"action"`
	// ScalingType is vertical or horizontal
	ScalingType ScalingType `json:"scalingType"`
	// TargetNode for placement (optional)
	TargetNode string `json:"targetNode,omitempty"`

	// NewResources for vertical scaling (RAN workloads)
	NewResources *Resources `json:"newResources,omitempty"`

	// NewReplicas for horizontal scaling (non-RAN workloads)
	NewReplicas *int32 `json:"newReplicas,omitempty"`

	// Priority determines execution order (lower = higher priority)
	Priority int `json:"priority"`
	// Reason explains the scaling decision
	Reason string `json:"reason"`

	// PostponeUntil for deferred scheduling (traffic prediction)
	PostponeUntil *time.Time `json:"postponeUntil,omitempty"`

	// FallbackToNode for GPU unavailability fallback
	FallbackToNode string `json:"fallbackToNode,omitempty"`
}

// ClusterState represents available resources across the cluster
type ClusterState struct {
	// Nodes is the list of node states
	Nodes []NodeState `json:"nodes"`
	// TotalAvailable is the aggregate available resources
	TotalAvailable Resources `json:"totalAvailable"`
	// Timestamp of the state snapshot
	Timestamp time.Time `json:"timestamp"`
}

// Node label constants for workload placement
const (
	// NodeLabelGPU indicates node has GPU resources
	NodeLabelGPU = "gpu"
	// NodeLabelRAN indicates node is designated for RAN workloads
	NodeLabelRAN = "ran"
	// NodeLabelComputing indicates node is for general computing/AI workloads
	NodeLabelComputing = "computing"
)

// NodeState represents a single node's resource state
type NodeState struct {
	// Name is the node name
	Name string `json:"name"`
	// Labels are the node labels (gpu, ran, computing)
	Labels map[string]string `json:"labels"`
	// Available resources
	Available Resources `json:"available"`
	// Allocatable is the total allocatable resources
	Allocatable Resources `json:"allocatable"`
	// HasGPU indicates if node has GPU (derived from labels)
	HasGPU bool `json:"hasGpu"`
	// GPUAvailable indicates if GPU is currently available
	GPUAvailable bool `json:"gpuAvailable"`
	// MIGEnabled indicates if MIG is available
	MIGEnabled bool `json:"migEnabled"`
	// MIGPartitions lists available GPU partitions
	MIGPartitions []MIGPartition `json:"migPartitions,omitempty"`
	// RunningWorkloads lists workload IDs on this node
	RunningWorkloads []string `json:"runningWorkloads"`
}

// IsGPUNode returns true if node is labeled as GPU node
func (n *NodeState) IsGPUNode() bool {
	_, ok := n.Labels[NodeLabelGPU]
	return ok || n.HasGPU
}

// IsRANNode returns true if node is labeled for RAN workloads
func (n *NodeState) IsRANNode() bool {
	_, ok := n.Labels[NodeLabelRAN]
	return ok
}

// IsComputingNode returns true if node is labeled for computing/AI workloads
func (n *NodeState) IsComputingNode() bool {
	_, ok := n.Labels[NodeLabelComputing]
	return ok
}

// MatchesLabels checks if node matches required labels
func (n *NodeState) MatchesLabels(required map[string]string) bool {
	for k, v := range required {
		if nodeVal, ok := n.Labels[k]; !ok || nodeVal != v {
			return false
		}
	}
	return true
}

// DeferredWorkload tracks a workload that was postponed due to traffic prediction
type DeferredWorkload struct {
	// WorkloadID is the workload identifier
	WorkloadID string `json:"workloadId"`
	// Namespace of the workload
	Namespace string `json:"namespace"`
	// DeferredAt is when the deferral was triggered
	DeferredAt time.Time `json:"deferredAt"`
	// DeferUntil is when the workload should be reconsidered
	DeferUntil time.Time `json:"deferUntil"`
	// Reason for deferral
	Reason string `json:"reason"`
	// OriginalAction is the action that was deferred
	OriginalAction ScalingAction `json:"originalAction"`
	// OriginalReplicas is the target replicas (for scale-up)
	OriginalReplicas *int32 `json:"originalReplicas,omitempty"`
	// PredictedPeakEnd is when the traffic peak is expected to end
	PredictedPeakEnd *time.Time `json:"predictedPeakEnd,omitempty"`
}

// MIGPartition represents a GPU partition
type MIGPartition struct {
	// ID is the partition identifier
	ID string `json:"id"`
	// Profile is the MIG profile (e.g., "1g.5gb")
	Profile string `json:"profile"`
	// Available indicates if the partition is free
	Available bool `json:"available"`
	// Memory is the partition memory
	Memory resource.Quantity `json:"memory"`
}

// TrafficPrediction from Digital Twin
type TrafficPrediction struct {
	// Timestamp of the prediction
	Timestamp time.Time `json:"timestamp"`
	// PredictedLoad normalized 0-1
	PredictedLoad float64 `json:"predictedLoad"`
	// Confidence score 0-1
	Confidence float64 `json:"confidence"`
	// HorizonMinutes is the prediction horizon
	HorizonMinutes int `json:"horizonMinutes"`
	// PeakExpected indicates if a traffic peak is predicted
	PeakExpected bool `json:"peakExpected"`
	// PeakStartTime when the peak is expected to start
	PeakStartTime *time.Time `json:"peakStartTime,omitempty"`
	// PeakEndTime when the peak is expected to end
	PeakEndTime *time.Time `json:"peakEndTime,omitempty"`
	// PeakMagnitude relative increase during peak (e.g., 1.5 = 50% increase)
	PeakMagnitude float64 `json:"peakMagnitude,omitempty"`
}

// IsTrafficPeakImminent returns true if a traffic peak is expected soon
func (p *TrafficPrediction) IsTrafficPeakImminent(threshold time.Duration) bool {
	if !p.PeakExpected || p.PeakStartTime == nil {
		return false
	}
	return time.Until(*p.PeakStartTime) <= threshold
}

// IsInTrafficPeak returns true if currently in a traffic peak
func (p *TrafficPrediction) IsInTrafficPeak() bool {
	if !p.PeakExpected || p.PeakStartTime == nil || p.PeakEndTime == nil {
		return false
	}
	now := time.Now()
	return now.After(*p.PeakStartTime) && now.Before(*p.PeakEndTime)
}

// AllocationConfig contains tunable algorithm parameters
type AllocationConfig struct {
	// ScalingSensitivity (α) controls scaling aggressiveness [0.5, 2.0]
	ScalingSensitivity float64 `json:"scalingSensitivity"`
	// PredictionWeight (β) weights prediction influence [0, 1]
	PredictionWeight float64 `json:"predictionWeight"`

	// ScaleDownThreshold triggers scale-down when deviation < -threshold
	ScaleDownThreshold float64 `json:"scaleDownThreshold"`
	// ScaleUpThreshold triggers scale-up when deviation > threshold
	ScaleUpThreshold float64 `json:"scaleUpThreshold"`

	// CPUSaturationThreshold triggers reactive scaling (e.g., 0.85 = 85%)
	CPUSaturationThreshold float64 `json:"cpuSaturationThreshold"`

	// ReservationFactor (γ) for prediction-based reservation
	ReservationFactor float64 `json:"reservationFactor"`
	// MaxReservationRatio (ρ_max) caps reserved resources
	MaxReservationRatio float64 `json:"maxReservationRatio"`
	// MinConfidenceThreshold for using predictions
	MinConfidenceThreshold float64 `json:"minConfidenceThreshold"`

	// TrafficPeakPostponeWindow: don't schedule non-RAN if peak within this window
	TrafficPeakPostponeWindow time.Duration `json:"trafficPeakPostponeWindow"`

	// TrafficHighLoadThreshold is the predicted load level above which:
	//   - GPU is withheld from non-RAN workloads (computing node used instead)
	//   - The scheduler gate activates and defers all non-RAN workload scheduling
	// Typical range: 0.6-0.8
	TrafficHighLoadThreshold float64 `json:"trafficHighLoadThreshold"`

	// MaxScheduleLookAheadMinutes is how many future hours to check when finding
	// the next low-traffic window for deferred non-RAN workloads
	MaxScheduleLookAheadMinutes int `json:"maxScheduleLookAheadHours"`

	// ReconciliationPeriod between allocation cycles
	ReconciliationPeriod time.Duration `json:"reconciliationPeriod"`
	// MetricsStalenessLimit marks metrics as stale
	MetricsStalenessLimit time.Duration `json:"metricsStalenessLimit"`
}

// DefaultConfig returns default configuration values
func DefaultConfig() AllocationConfig {
	return AllocationConfig{
		ScalingSensitivity:          1.0,
		PredictionWeight:            0.5,
		ScaleDownThreshold:          0.3,
		ScaleUpThreshold:            0.0,
		CPUSaturationThreshold:      0.85,
		ReservationFactor:           0.2,
		MaxReservationRatio:         0.3,
		MinConfidenceThreshold:      0.5,
		TrafficPeakPostponeWindow:   30 * time.Minute,
		TrafficHighLoadThreshold:    0.35, // 850 cdrs/10min
		MaxScheduleLookAheadMinutes: 30,
		ReconciliationPeriod:        30 * time.Second,
		MetricsStalenessLimit:       60 * time.Second,
	}
}
