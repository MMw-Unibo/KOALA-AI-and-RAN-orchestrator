// Package v1alpha1 contains API Schema definitions for the airan v1alpha1 API group
// +kubebuilder:object:generate=true
// +groupName=airan.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "airan.io", Version: "v1alpha1"}
)

// QoSAwareWorkloadSpec defines the desired state of QoSAwareWorkload
type QoSAwareWorkloadSpec struct {
	// WorkloadRef references the target workload (Deployment or StatefulSet)
	WorkloadRef WorkloadReference `json:"workloadRef"`

	// QoSLevel is the priority level: high, medium, or low
	// +kubebuilder:validation:Enum=high;medium;low
	QoSLevel string `json:"qosLevel"`

	// SLATarget defines the SLA requirements
	SLATarget SLATarget `json:"slaTarget,omitempty"`

	// Placement defines node placement constraints
	Placement PlacementConfig `json:"placement,omitempty"`

	// Scaling defines scaling configuration
	Scaling ScalingConfig `json:"scaling,omitempty"`

	// GPURequirements defines GPU requirements
	GPURequirements *GPURequirements `json:"gpuRequirements,omitempty"`

	// ProactiveConfig defines proactive orchestration settings
	ProactiveConfig *ProactiveConfig `json:"proactiveConfig,omitempty"`

	// DigitalTwin defines Digital Twin integration settings
	DigitalTwin *DigitalTwinConfig `json:"digitalTwin,omitempty"`

	// MetricsSource defines where to get metrics from
	MetricsSource *MetricsSource `json:"metricsSource,omitempty"`
}

// WorkloadReference identifies the target workload
type WorkloadReference struct {
	// Kind is Deployment or StatefulSet
	// +kubebuilder:validation:Enum=Deployment;StatefulSet
	Kind string `json:"kind"`
	// Name is the workload name
	Name string `json:"name"`
}

// SLATarget defines SLA requirements
type SLATarget struct {
	// LatencyMs is the target latency in milliseconds
	LatencyMs int32 `json:"latencyMs,omitempty"`
	// ThroughputRps is the target throughput in requests per second
	ThroughputRps int32 `json:"throughputRps,omitempty"`
	// Availability is the target availability (e.g., "99.99%")
	Availability string `json:"availability,omitempty"`
}

// PlacementConfig defines node placement constraints
type PlacementConfig struct {
	// RequiredNodeLabels are labels that nodes must have (ran, gpu, computing)
	RequiredNodeLabels []string `json:"requiredNodeLabels,omitempty"`
	// PreferredNodeLabels are labels that are preferred but not required
	PreferredNodeLabels []string `json:"preferredNodeLabels,omitempty"`
	// AntiAffinityLabels are labels to avoid
	AntiAffinityLabels []string `json:"antiAffinityLabels,omitempty"`
	// NodeSelector is a key-value map for node selection
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// ScalingConfig defines scaling settings
type ScalingConfig struct {
	// Vertical defines vertical scaling (for RAN workloads)
	Vertical VerticalScaling `json:"vertical,omitempty"`
	// Horizontal defines horizontal scaling (for non-RAN workloads)
	Horizontal HorizontalScaling `json:"horizontal,omitempty"`
}

// VerticalScaling defines vertical scaling settings
type VerticalScaling struct {
	// Enabled indicates if vertical scaling is enabled
	Enabled bool `json:"enabled,omitempty"`
	// MinResources defines minimum resources
	MinResources ResourceSpec `json:"minResources,omitempty"`
	// MaxResources defines maximum resources
	MaxResources ResourceSpec `json:"maxResources,omitempty"`
}

// HorizontalScaling defines horizontal scaling settings
type HorizontalScaling struct {
	// Enabled indicates if horizontal scaling is enabled
	Enabled bool `json:"enabled,omitempty"`
	// MinReplicas is the minimum number of replicas
	MinReplicas int32 `json:"minReplicas,omitempty"`
	// MaxReplicas is the maximum number of replicas
	MaxReplicas int32 `json:"maxReplicas,omitempty"`
}

// ResourceSpec defines resource specifications
type ResourceSpec struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// GPURequirements defines GPU requirements
type GPURequirements struct {
	// MIGProfile is the MIG partition profile
	MIGProfile string `json:"migProfile,omitempty"`
	// Dedicated indicates if a dedicated GPU is required
	Dedicated bool `json:"dedicated,omitempty"`
	// Count is the number of GPUs required
	Count int32 `json:"count,omitempty"`
}

// ProactiveConfig defines proactive orchestration settings
type ProactiveConfig struct {
	// GPUFallbackToCPU allows fallback to CPU when GPU is unavailable
	GPUFallbackToCPU bool `json:"gpuFallbackToCPU,omitempty"`
	// PostponeDuringPeak defers scaling during RAN traffic peaks
	PostponeDuringPeak bool `json:"postponeDuringPeak,omitempty"`
}

// DigitalTwinConfig defines Digital Twin integration settings
type DigitalTwinConfig struct {
	// Enabled indicates if Digital Twin integration is enabled
	Enabled bool `json:"enabled,omitempty"`
	// PredictionHorizonMinutes is the prediction horizon
	PredictionHorizonMinutes int32 `json:"predictionHorizonMinutes,omitempty"`
	// MinConfidence is the minimum confidence threshold
	MinConfidence string `json:"minConfidence,omitempty"`
}

// MetricsSource defines metrics source configuration
type MetricsSource struct {
	// Type is prometheus or custom
	Type string `json:"type,omitempty"`
	// Endpoint is the custom metrics endpoint
	Endpoint string `json:"endpoint,omitempty"`
	// LatencyMetricName is the name of the latency metric
	LatencyMetricName string `json:"latencyMetricName,omitempty"`
}

// QoSAwareWorkloadStatus defines the observed state of QoSAwareWorkload
type QoSAwareWorkloadStatus struct {
	// Phase is the current phase (Pending, Running, Scaling, Degraded, Error)
	Phase string `json:"phase,omitempty"`
	// CurrentLatencyMs is the current observed latency
	CurrentLatencyMs int32 `json:"currentLatencyMs,omitempty"`
	// SLACompliant indicates if the workload meets SLA
	SLACompliant bool `json:"slaCompliant,omitempty"`
	// LastScalingAction is the last scaling action taken
	LastScalingAction string `json:"lastScalingAction,omitempty"`
	// LastScalingTime is when the last scaling occurred
	LastScalingTime *metav1.Time `json:"lastScalingTime,omitempty"`
	// CurrentReplicas is the current number of replicas
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`
	// AllocatedResources shows current resource allocation
	AllocatedResources ResourceSpec `json:"allocatedResources,omitempty"`
	// Deferred indicates if the workload is deferred due to traffic peak
	Deferred bool `json:"deferred,omitempty"`
	// DeferredUntil is when the deferral ends
	DeferredUntil *metav1.Time `json:"deferredUntil,omitempty"`
	// Conditions are the workload conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="QoS",type=string,JSONPath=`.spec.qosLevel`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Latency",type=integer,JSONPath=`.status.currentLatencyMs`
// +kubebuilder:printcolumn:name="SLA",type=boolean,JSONPath=`.status.slaCompliant`
// +kubebuilder:printcolumn:name="Deferred",type=boolean,JSONPath=`.status.deferred`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// QoSAwareWorkload is the Schema for the qosawareworkloads API
type QoSAwareWorkload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   QoSAwareWorkloadSpec   `json:"spec,omitempty"`
	Status QoSAwareWorkloadStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// QoSAwareWorkloadList contains a list of QoSAwareWorkload
type QoSAwareWorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QoSAwareWorkload `json:"items"`
}
