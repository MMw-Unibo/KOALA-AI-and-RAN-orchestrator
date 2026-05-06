package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/airan/orchestrator/pkg/allocation"
	"github.com/airan/orchestrator/pkg/digitaltwin"
	"github.com/airan/orchestrator/pkg/executor"
	"github.com/airan/orchestrator/pkg/metrics"
)

var (
	qawGVR = schema.GroupVersionResource{
		Group:    "airan.io",
		Version:  "v1alpha1",
		Resource: "qosawareworkloads",
	}
)

func main() {
	var (
		digitalTwinAddr      string
		prometheusURL        string
		reconciliationPeriod time.Duration
		kubeconfig           string
		dryRun               bool
	)

	flag.StringVar(&digitalTwinAddr, "digital-twin-address", "localhost:50051", "Digital Twin gRPC service address")
	flag.StringVar(&prometheusURL, "prometheus-url", "http://prometheus-k8s.monitoring:9090", "Prometheus URL")
	flag.DurationVar(&reconciliationPeriod, "reconciliation-period", 30*time.Second, "Period between allocation cycles")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file (optional, uses in-cluster config if not set)")
	flag.BoolVar(&dryRun, "dry-run", false, "Dry run mode - log actions but don't execute")

	klog.InitFlags(nil)
	flag.Parse()

	klog.Info("Starting AI-RAN QoS-Aware Orchestrator")
	klog.Infof("Digital Twin address: %s", digitalTwinAddr)
	klog.Infof("Prometheus URL: %s", prometheusURL)
	klog.Infof("Reconciliation period: %s", reconciliationPeriod)
	klog.Infof("Dry run: %v", dryRun)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		klog.Infof("Received signal %s, shutting down", sig)
		cancel()
	}()

	// Build Kubernetes config
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		klog.Fatalf("Failed to build kubernetes config: %v", err)
	}

	// Create clients
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create kubernetes client: %v", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create dynamic client: %v", err)
	}

	// Initialize components
	allocConfig := allocation.DefaultConfig()
	allocConfig.ReconciliationPeriod = reconciliationPeriod

	// Initialize Digital Twin client
	dtConfig := digitaltwin.DefaultClientConfig()
	dtConfig.Address = digitalTwinAddr
	dtClient, err := digitaltwin.NewClient(dtConfig)
	if err != nil {
		klog.Warningf("Failed to create Digital Twin client, continuing without: %v", err)
		dtClient = nil
	}
	if dtClient != nil {
		defer dtClient.Close()
	}

	// Initialize Metrics client
	metricsConfig := metrics.DefaultClientConfig()
	metricsConfig.PrometheusURL = prometheusURL
	metricsClient, err := metrics.NewClient(metricsConfig)
	if err != nil {
		klog.Fatalf("Failed to create Metrics client: %v", err)
	}

	// Initialize Cluster State Provider
	clusterProvider := metrics.NewClusterStateProvider(metricsClient)

	// Initialize Allocator
	allocator := allocation.NewAllocator(allocConfig, metricsClient, dtClient, clusterProvider)

	// Initialize Executor
	execConfig := executor.DefaultConfig()
	execConfig.DryRun = dryRun
	exec := executor.NewExecutor(execConfig, clientset)

	// Main reconciliation loop
	klog.Info("Starting reconciliation loop")
	ticker := time.NewTicker(reconciliationPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.Info("Orchestrator shutdown complete")
			return
		case <-ticker.C:
			if err := runReconciliation(ctx, dynamicClient, allocator, exec); err != nil {
				klog.Errorf("Reconciliation failed: %v", err)
			}
		}
	}
}

func runReconciliation(ctx context.Context, dynamicClient dynamic.Interface, allocator *allocation.Allocator, exec *executor.Executor) error {
	klog.V(2).Info("Starting reconciliation cycle")

	// Fetch QoSAwareWorkloads from Kubernetes
	workloads, err := fetchWorkloads(ctx, dynamicClient)
	if err != nil {
		return fmt.Errorf("failed to fetch workloads: %w", err)
	}

	if len(workloads) == 0 {
		klog.V(3).Info("No workloads to reconcile")
		return nil
	}

	klog.Infof("Found %d QoSAwareWorkloads to process", len(workloads))

	// Log workload summary
	highCount, mediumCount, lowCount := 0, 0, 0
	for _, w := range workloads {
		switch w.QoSLevel {
		case allocation.QoSHigh:
			highCount++
		case allocation.QoSMedium:
			mediumCount++
		case allocation.QoSLow:
			lowCount++
		}
	}
	klog.Infof("Workload breakdown: HIGH=%d, MEDIUM=%d, LOW=%d", highCount, mediumCount, lowCount)

	// Run allocation algorithm
	directives, err := allocator.Allocate(ctx, workloads)
	if err != nil {
		return fmt.Errorf("allocation failed: %w", err)
	}

	klog.Infof("Allocation produced %d directives", len(directives))

	// Execute directives
	for _, directive := range directives {
		klog.Infof("Directive: workload=%s action=%s type=%s reason=%s",
			directive.WorkloadID, directive.Action, directive.ScalingType, directive.Reason)

		if err := exec.Execute(ctx, directive); err != nil {
			klog.Errorf("Failed to execute directive for %s: %v", directive.WorkloadID, err)
		}
	}

	return nil
}

func fetchWorkloads(ctx context.Context, dynamicClient dynamic.Interface) ([]allocation.WorkloadSpec, error) {
	// List all QoSAwareWorkloads across all namespaces
	list, err := dynamicClient.Resource(qawGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list QoSAwareWorkloads: %w", err)
	}

	var workloads []allocation.WorkloadSpec

	for _, item := range list.Items {
		workload, err := convertToWorkloadSpec(&item)
		if err != nil {
			klog.Warningf("Failed to convert workload %s/%s: %v",
				item.GetNamespace(), item.GetName(), err)
			continue
		}
		workloads = append(workloads, *workload)
	}

	return workloads, nil
}

func convertToWorkloadSpec(obj *unstructured.Unstructured) (*allocation.WorkloadSpec, error) {
	name := obj.GetName()
	namespace := obj.GetNamespace()

	// Extract spec fields
	spec, found, err := unstructured.NestedMap(obj.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("spec not found")
	}

	// Get QoS level
	qosLevelStr, _, _ := unstructured.NestedString(spec, "qosLevel")
	qosLevel := allocation.GetQoSLevelFromNamespace(namespace)
	if qosLevelStr == "high" {
		qosLevel = allocation.QoSHigh
	} else if qosLevelStr == "medium" {
		qosLevel = allocation.QoSMedium
	} else if qosLevelStr == "low" {
		qosLevel = allocation.QoSLow
	}

	// Get SLA target
	slaTarget, _, _ := unstructured.NestedMap(spec, "slaTarget")
	latencyMs, _, _ := unstructured.NestedInt64(slaTarget, "latencyMs")

	// Get workload reference
	workloadRef, _, _ := unstructured.NestedMap(spec, "workloadRef")
	workloadName, _, _ := unstructured.NestedString(workloadRef, "name")
	if workloadName == "" {
		workloadName = name
	}

	// Get scaling config
	scaling, _, _ := unstructured.NestedMap(spec, "scaling")

	// Horizontal scaling
	horizontal, _, _ := unstructured.NestedMap(scaling, "horizontal")
	minReplicas, _, _ := unstructured.NestedInt64(horizontal, "minReplicas")
	maxReplicas, _, _ := unstructured.NestedInt64(horizontal, "maxReplicas")

	if minReplicas == 0 {
		minReplicas = 1
	}
	if maxReplicas == 0 {
		maxReplicas = 10
	}

	// Get placement config
	placement, _, _ := unstructured.NestedMap(spec, "placement")
	requiredLabels, _, _ := unstructured.NestedStringSlice(placement, "requiredNodeLabels")

	// Get proactive config
	proactive, _, _ := unstructured.NestedMap(spec, "proactiveConfig")
	gpuFallback, _, _ := unstructured.NestedBool(proactive, "gpuFallbackToCPU")
	postponeDuringPeak, _, _ := unstructured.NestedBool(proactive, "postponeDuringPeak")

	// Get GPU requirements
	gpuReq, _, _ := unstructured.NestedMap(spec, "gpuRequirements")
	gpuCount, _, _ := unstructured.NestedInt64(gpuReq, "count")

	workload := &allocation.WorkloadSpec{
		ID:                 fmt.Sprintf("%s/%s", namespace, name),
		Name:               workloadName,
		Namespace:          namespace,
		QoSLevel:           qosLevel,
		TargetLatency:      time.Duration(latencyMs) * time.Millisecond,
		MinReplicas:        int32(minReplicas),
		MaxReplicas:        int32(maxReplicas),
		CurrentReplicas:    1,
		RequiredNodeLabels: requiredLabels,
		RequiresGPU:        gpuCount > 0,
		GPUFallbackCPU:     gpuFallback,
		PostponeDuringPeak: postponeDuringPeak,
	}

	klog.V(3).Infof("Loaded workload: %s (QoS=%d, Namespace=%s, TargetLatency=%dms)",
		workload.ID, workload.QoSLevel, namespace, latencyMs)

	return workload, nil
}

// Helper to print workload as JSON for debugging
func workloadToJSON(w allocation.WorkloadSpec) string {
	b, _ := json.MarshalIndent(w, "", "  ")
	return string(b)
}
