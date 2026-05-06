package allocation

import (
	"context"
	"time"
)

// MetricsClient provides access to the observability system
type MetricsClient interface {
	// GetMetrics retrieves current metrics for specified workloads
	GetMetrics(ctx context.Context, workloadIDs []string) (map[string]WorkloadMetrics, error)

	// GetNodeMetrics retrieves metrics for cluster nodes
	GetNodeMetrics(ctx context.Context) ([]NodeMetrics, error)
}

// DigitalTwinClient provides traffic prediction via gRPC
type DigitalTwinClient interface {
	// GetPrediction retrieves traffic prediction for the specified horizon
	GetPrediction(ctx context.Context, horizon time.Duration) (*TrafficPrediction, error)

	// GetHistoricalPattern retrieves historical traffic patterns
	GetHistoricalPattern(ctx context.Context, period time.Duration) ([]TrafficDataPoint, error)
}

// ClusterStateProvider provides cluster resource information
type ClusterStateProvider interface {
	// GetState retrieves current cluster state
	GetState(ctx context.Context) (*ClusterState, error)

	// GetNodeState retrieves state for a specific node
	GetNodeState(ctx context.Context, nodeName string) (*NodeState, error)

	// GetMIGPartitions retrieves MIG partition information
	GetMIGPartitions(ctx context.Context, nodeName string) ([]MIGPartition, error)
}

// DirectiveExecutor applies scaling directives to the cluster
type DirectiveExecutor interface {
	// Execute applies a single scaling directive
	Execute(ctx context.Context, directive ScalingDirective) error

	// ExecuteBatch applies multiple directives in priority order
	ExecuteBatch(ctx context.Context, directives []ScalingDirective) error
}

// WorkloadRegistry provides access to workload definitions
type WorkloadRegistry interface {
	// ListWorkloads returns all managed workloads
	ListWorkloads(ctx context.Context) ([]WorkloadSpec, error)

	// GetWorkload retrieves a specific workload
	GetWorkload(ctx context.Context, id string) (*WorkloadSpec, error)

	// UpdateWorkloadStatus updates the workload's status
	UpdateWorkloadStatus(ctx context.Context, id string, status WorkloadStatus) error
}

// NodeMetrics contains node-level metrics from observability
type NodeMetrics struct {
	NodeName          string  `json:"nodeName"`
	CPUUtilization    float64 `json:"cpuUtilization"`
	GPUUtilization    float64 `json:"gpuUtilization"`
	MemoryUsage       int64   `json:"memoryUsage"`
	MemoryTotal       int64   `json:"memoryTotal"`
	MemoryUtilization float64 `json:"memoryUtilization"`
	GPUAvailable      bool    `json:"gpuAvailable"`
	GPUCount          int     `json:"gpuCount"`
}

// TrafficDataPoint represents a historical traffic measurement
type TrafficDataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Load      float64   `json:"load"`
}

// WorkloadStatus represents the current status of a workload
type WorkloadStatus struct {
	Phase              WorkloadPhase       `json:"phase"`
	CurrentLatency     time.Duration       `json:"currentLatency"`
	LastScalingAction  ScalingAction       `json:"lastScalingAction"`
	LastScalingTime    time.Time           `json:"lastScalingTime"`
	AllocatedResources Resources           `json:"allocatedResources"`
	Replicas           int32               `json:"replicas"`
	Conditions         []WorkloadCondition `json:"conditions"`
}

// WorkloadPhase represents the lifecycle phase
type WorkloadPhase string

const (
	PhasePending  WorkloadPhase = "Pending"
	PhaseRunning  WorkloadPhase = "Running"
	PhaseScaling  WorkloadPhase = "Scaling"
	PhaseDegraded WorkloadPhase = "Degraded"
	PhaseError    WorkloadPhase = "Error"
)

// WorkloadCondition represents a condition of the workload
type WorkloadCondition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
	Reason             string    `json:"reason"`
	Message            string    `json:"message"`
}
