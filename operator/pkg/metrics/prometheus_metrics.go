package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Allocation metrics
	AllocationDecisionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_orchestrator_allocation_decisions_total",
			Help: "Total number of allocation decisions made",
		},
		[]string{"action", "qos_level", "scaling_type"},
	)

	AllocationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "airan_orchestrator_allocation_latency_seconds",
			Help:    "Time taken for allocation decisions",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{},
	)

	AllocationCyclesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "airan_orchestrator_allocation_cycles_total",
			Help: "Total number of allocation cycles completed",
		},
	)

	// Workload metrics
	WorkloadLatency = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "airan_workload_latency_ms",
			Help: "Current workload latency in milliseconds",
		},
		[]string{"workload_id", "namespace", "qos_level"},
	)

	WorkloadSLATarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "airan_workload_sla_target_ms",
			Help: "Workload SLA target latency in milliseconds",
		},
		[]string{"workload_id", "namespace"},
	)

	WorkloadSLACompliance = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "airan_workload_sla_compliance_ratio",
			Help: "Ratio of current latency to SLA target (< 1 is compliant)",
		},
		[]string{"workload_id", "namespace"},
	)

	WorkloadSLAViolations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_workload_sla_violations_total",
			Help: "Total number of SLA violations detected",
		},
		[]string{"workload_id", "namespace", "qos_level"},
	)

	WorkloadReplicas = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "airan_workload_replicas",
			Help: "Current number of workload replicas",
		},
		[]string{"workload_id", "namespace"},
	)

	// Scaling metrics
	ScalingOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_scaling_operations_total",
			Help: "Total number of scaling operations",
		},
		[]string{"workload_id", "direction", "type"},
	)

	ScalingOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "airan_scaling_operation_duration_seconds",
			Help:    "Duration of scaling operations",
			Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"workload_id", "type"},
	)

	ScalingErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_scaling_errors_total",
			Help: "Total number of scaling errors",
		},
		[]string{"workload_id", "type", "reason"},
	)

	// Preemption metrics
	PreemptionEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_preemption_events_total",
			Help: "Total number of preemption events",
		},
		[]string{"preempted_workload", "beneficiary_workload"},
	)

	// Deferred workload metrics
	DeferredWorkloadsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "airan_deferred_workloads_total",
			Help: "Current number of deferred workloads",
		},
	)

	DeferredWorkloadResumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_deferred_workload_resumed_total",
			Help: "Total number of deferred workloads that were resumed",
		},
		[]string{"workload_id", "namespace"},
	)

	// Digital Twin metrics
	DigitalTwinPredictionRequests = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "airan_digital_twin_prediction_requests_total",
			Help: "Total number of prediction requests to Digital Twin",
		},
	)

	DigitalTwinPredictionLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "airan_digital_twin_prediction_latency_seconds",
			Help:    "Latency of Digital Twin prediction requests",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		},
	)

	DigitalTwinPredictedLoad = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "airan_digital_twin_predicted_load",
			Help: "Current predicted traffic load from Digital Twin (0-1)",
		},
	)

	DigitalTwinPeakExpected = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "airan_digital_twin_peak_expected",
			Help: "1 if traffic peak is expected, 0 otherwise",
		},
	)

	DigitalTwinPredictionConfidence = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "airan_digital_twin_prediction_confidence",
			Help: "Confidence score of the latest prediction (0-1)",
		},
	)

	// Node placement metrics
	PlacementValidationFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "airan_placement_validation_failures_total",
			Help: "Total number of placement validation failures",
		},
		[]string{"workload_id", "reason"},
	)

	// Controller metrics
	ManagedWorkloads = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "airan_controller_managed_workloads",
			Help: "Number of workloads managed by the controller",
		},
		[]string{"qos_level", "namespace"},
	)

	ReconciliationLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "airan_controller_reconciliation_latency_seconds",
			Help:    "Latency of reconciliation cycles",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)

	ReconciliationErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "airan_controller_reconciliation_errors_total",
			Help: "Total number of reconciliation errors",
		},
	)
)

// RecordAllocationDecision records an allocation decision metric
func RecordAllocationDecision(action, qosLevel, scalingType string) {
	AllocationDecisionsTotal.WithLabelValues(action, qosLevel, scalingType).Inc()
}

// RecordWorkloadLatency records the current workload latency
func RecordWorkloadLatency(workloadID, namespace, qosLevel string, latencyMs float64) {
	WorkloadLatency.WithLabelValues(workloadID, namespace, qosLevel).Set(latencyMs)
}

// RecordSLACompliance records SLA compliance ratio
func RecordSLACompliance(workloadID, namespace string, ratio float64) {
	WorkloadSLACompliance.WithLabelValues(workloadID, namespace).Set(ratio)
}

// RecordSLAViolation records an SLA violation
func RecordSLAViolation(workloadID, namespace, qosLevel string) {
	WorkloadSLAViolations.WithLabelValues(workloadID, namespace, qosLevel).Inc()
}

// RecordScalingOperation records a scaling operation
func RecordScalingOperation(workloadID, direction, scalingType string) {
	ScalingOperationsTotal.WithLabelValues(workloadID, direction, scalingType).Inc()
}

// RecordPreemption records a preemption event
func RecordPreemption(preempted, beneficiary string) {
	PreemptionEventsTotal.WithLabelValues(preempted, beneficiary).Inc()
}

// RecordPrediction records Digital Twin prediction metrics
func RecordPrediction(load, confidence float64, peakExpected bool) {
	DigitalTwinPredictedLoad.Set(load)
	DigitalTwinPredictionConfidence.Set(confidence)
	if peakExpected {
		DigitalTwinPeakExpected.Set(1)
	} else {
		DigitalTwinPeakExpected.Set(0)
	}
}

// UpdateDeferredWorkloads updates the count of deferred workloads
func UpdateDeferredWorkloads(count int) {
	DeferredWorkloadsTotal.Set(float64(count))
}

// RecordDeferredWorkloadResumed records when a deferred workload is resumed
func RecordDeferredWorkloadResumed(workloadID, namespace string) {
	DeferredWorkloadResumed.WithLabelValues(workloadID, namespace).Inc()
}

// UpdateManagedWorkloads updates the count of managed workloads
func UpdateManagedWorkloads(qosLevel, namespace string, count int) {
	ManagedWorkloads.WithLabelValues(qosLevel, namespace).Set(float64(count))
}
