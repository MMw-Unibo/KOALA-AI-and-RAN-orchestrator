// Package executor implements the DirectiveExecutor that applies
// scaling directives to Kubernetes workloads.
package executor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/airan/orchestrator/pkg/allocation"
)

// Config holds configuration for the directive executor
type Config struct {
	// DryRun mode - log actions but don't execute
	DryRun bool

	// MaxConcurrentExecutions limits parallel executions
	MaxConcurrentExecutions int

	// ExecutionTimeout for individual operations
	ExecutionTimeout time.Duration

	// RetryAttempts for failed operations
	RetryAttempts int

	// RetryDelay between retry attempts
	RetryDelay time.Duration
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		DryRun:                  false,
		MaxConcurrentExecutions: 5,
		ExecutionTimeout:        30 * time.Second,
		RetryAttempts:           3,
		RetryDelay:              time.Second,
	}
}

// WorkloadInfo contains information about a workload for execution
type WorkloadInfo struct {
	ID         string
	Name       string
	Namespace  string
	Kind       string // Deployment or StatefulSet
	Containers []string
}

// ExecutionRecord tracks execution history
type ExecutionRecord struct {
	DirectiveID string
	WorkloadID  string
	Action      allocation.ScalingAction
	ScalingType allocation.ScalingType
	StartTime   time.Time
	EndTime     time.Time
	Success     bool
	Error       string
}

// Executor applies scaling directives to Kubernetes workloads
type Executor struct {
	config           Config
	clientset        kubernetes.Interface
	workloadRegistry map[string]WorkloadInfo
	registryMutex    sync.RWMutex
	history          []ExecutionRecord
	historyMutex     sync.Mutex
	semaphore        chan struct{}
}

// NewExecutor creates a new Executor instance
func NewExecutor(config Config, clientset kubernetes.Interface) *Executor {
	return &Executor{
		config:           config,
		clientset:        clientset,
		workloadRegistry: make(map[string]WorkloadInfo),
		history:          make([]ExecutionRecord, 0),
		semaphore:        make(chan struct{}, config.MaxConcurrentExecutions),
	}
}

// RegisterWorkload registers a workload for execution
func (e *Executor) RegisterWorkload(info WorkloadInfo) {
	e.registryMutex.Lock()
	defer e.registryMutex.Unlock()
	e.workloadRegistry[info.ID] = info
}

// Execute applies a single scaling directive
func (e *Executor) Execute(ctx context.Context, directive allocation.ScalingDirective) error {
	record := ExecutionRecord{
		DirectiveID: fmt.Sprintf("%s-%d", directive.WorkloadID, time.Now().UnixNano()),
		WorkloadID:  directive.WorkloadID,
		Action:      directive.Action,
		ScalingType: directive.ScalingType,
		StartTime:   time.Now(),
	}

	defer func() {
		record.EndTime = time.Now()
		e.recordExecution(record)
	}()

	// Acquire semaphore
	select {
	case e.semaphore <- struct{}{}:
		defer func() { <-e.semaphore }()
	case <-ctx.Done():
		record.Success = false
		record.Error = "context cancelled while waiting for semaphore"
		return ctx.Err()
	}

	// Get workload info from registry or construct from directive
	e.registryMutex.RLock()
	info, ok := e.workloadRegistry[directive.WorkloadID]
	e.registryMutex.RUnlock()

	if !ok {
		// Construct WorkloadInfo from directive
		info = WorkloadInfo{
			ID:        directive.WorkloadID,
			Name:      directive.WorkloadID,
			Namespace: directive.Namespace,
			Kind:      "Deployment",
		}
	}

	klog.V(2).Infof("Executing directive: workload=%s, action=%s, type=%s, reason=%s",
		directive.WorkloadID, directive.Action, directive.ScalingType, directive.Reason)

	if e.config.DryRun {
		klog.Infof("[DRY-RUN] Would execute: %+v", directive)
		record.Success = true
		return nil
	}

	// Execute with retry
	var lastErr error
	for attempt := 0; attempt <= e.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(e.config.RetryDelay)
			klog.V(3).Infof("Retrying directive execution (attempt %d)", attempt)
		}

		err := e.executeDirective(ctx, info, directive)
		if err == nil {
			record.Success = true
			return nil
		}
		lastErr = err
	}

	record.Success = false
	record.Error = lastErr.Error()
	return fmt.Errorf("failed to execute directive after %d attempts: %w", e.config.RetryAttempts, lastErr)
}

// ExecuteBatch applies multiple directives in priority order
func (e *Executor) ExecuteBatch(ctx context.Context, directives []allocation.ScalingDirective) error {
	sorted := make([]allocation.ScalingDirective, len(directives))
	copy(sorted, directives)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})

	var errors []error
	for _, directive := range sorted {
		if err := e.Execute(ctx, directive); err != nil {
			klog.Errorf("Failed to execute directive for %s: %v", directive.WorkloadID, err)
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch execution had %d errors", len(errors))
	}
	return nil
}

func (e *Executor) executeDirective(ctx context.Context, info WorkloadInfo, directive allocation.ScalingDirective) error {
	switch directive.Action {
	case allocation.ActionScaleUp, allocation.ActionScaleDown:
		switch directive.ScalingType {
		case allocation.ScalingVertical:
			return e.executeVerticalScale(ctx, info, directive)
		case allocation.ScalingHorizontal:
			return e.executeHorizontalScale(ctx, info, directive)
		default:
			return fmt.Errorf("unknown scaling type: %s", directive.ScalingType)
		}
	case allocation.ActionPostpone:
		klog.Infof("Workload %s scaling postponed until %v: %s",
			directive.WorkloadID, directive.PostponeUntil, directive.Reason)
		return nil
	case allocation.ActionPreempt:
		return e.executePreemption(ctx, info, directive)
	default:
		return fmt.Errorf("unknown action: %s", directive.Action)
	}
}

func (e *Executor) executeVerticalScale(ctx context.Context, info WorkloadInfo, directive allocation.ScalingDirective) error {
	if directive.NewResources == nil {
		return fmt.Errorf("vertical scaling requires NewResources")
	}

	switch info.Kind {
	case "Deployment":
		return e.scaleDeploymentVertically(ctx, info, directive.NewResources)
	case "StatefulSet":
		return e.scaleStatefulSetVertically(ctx, info, directive.NewResources)
	default:
		return fmt.Errorf("unsupported workload kind for vertical scaling: %s", info.Kind)
	}
}

func (e *Executor) scaleDeploymentVertically(ctx context.Context, info WorkloadInfo, resources *allocation.Resources) error {
	deployment, err := e.clientset.AppsV1().Deployments(info.Namespace).Get(ctx, info.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]

		if len(info.Containers) > 0 && !containsString(info.Containers, container.Name) {
			continue
		}

		container.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resources.CPU,
				corev1.ResourceMemory: resources.Memory,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resources.CPU,
				corev1.ResourceMemory: resources.Memory,
			},
		}

		if resources.GPU.Count > 0 {
			container.Resources.Limits["nvidia.com/gpu"] = *resource.NewQuantity(int64(resources.GPU.Count), resource.DecimalSI)
		}
	}

	_, err = e.clientset.AppsV1().Deployments(info.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update deployment: %w", err)
	}

	klog.V(2).Infof("Updated deployment %s/%s with new resources: cpu=%s, memory=%s",
		info.Namespace, info.Name, resources.CPU.String(), resources.Memory.String())
	return nil
}

func (e *Executor) scaleStatefulSetVertically(ctx context.Context, info WorkloadInfo, resources *allocation.Resources) error {
	statefulSet, err := e.clientset.AppsV1().StatefulSets(info.Namespace).Get(ctx, info.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get statefulset: %w", err)
	}

	for i := range statefulSet.Spec.Template.Spec.Containers {
		container := &statefulSet.Spec.Template.Spec.Containers[i]

		if len(info.Containers) > 0 && !containsString(info.Containers, container.Name) {
			continue
		}

		container.Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resources.CPU,
				corev1.ResourceMemory: resources.Memory,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resources.CPU,
				corev1.ResourceMemory: resources.Memory,
			},
		}
	}

	_, err = e.clientset.AppsV1().StatefulSets(info.Namespace).Update(ctx, statefulSet, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update statefulset: %w", err)
	}

	klog.V(2).Infof("Updated statefulset %s/%s with new resources", info.Namespace, info.Name)
	return nil
}

func (e *Executor) executeHorizontalScale(ctx context.Context, info WorkloadInfo, directive allocation.ScalingDirective) error {
	if directive.NewReplicas == nil {
		return fmt.Errorf("horizontal scaling requires NewReplicas")
	}

	switch info.Kind {
	case "Deployment":
		return e.scaleDeploymentHorizontally(ctx, info, *directive.NewReplicas)
	case "StatefulSet":
		return e.scaleStatefulSetHorizontally(ctx, info, *directive.NewReplicas)
	default:
		return fmt.Errorf("unsupported workload kind for horizontal scaling: %s", info.Kind)
	}
}

func (e *Executor) scaleDeploymentHorizontally(ctx context.Context, info WorkloadInfo, replicas int32) error {
	deployment, err := e.clientset.AppsV1().Deployments(info.Namespace).Get(ctx, info.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	oldReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		oldReplicas = *deployment.Spec.Replicas
	}

	deployment.Spec.Replicas = &replicas

	_, err = e.clientset.AppsV1().Deployments(info.Namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update deployment replicas: %w", err)
	}

	klog.V(2).Infof("Scaled deployment %s/%s from %d to %d replicas",
		info.Namespace, info.Name, oldReplicas, replicas)
	return nil
}

func (e *Executor) scaleStatefulSetHorizontally(ctx context.Context, info WorkloadInfo, replicas int32) error {
	statefulSet, err := e.clientset.AppsV1().StatefulSets(info.Namespace).Get(ctx, info.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get statefulset: %w", err)
	}

	oldReplicas := int32(1)
	if statefulSet.Spec.Replicas != nil {
		oldReplicas = *statefulSet.Spec.Replicas
	}

	statefulSet.Spec.Replicas = &replicas

	_, err = e.clientset.AppsV1().StatefulSets(info.Namespace).Update(ctx, statefulSet, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update statefulset replicas: %w", err)
	}

	klog.V(2).Infof("Scaled statefulset %s/%s from %d to %d replicas",
		info.Namespace, info.Name, oldReplicas, replicas)
	return nil
}

func (e *Executor) executePreemption(ctx context.Context, info WorkloadInfo, directive allocation.ScalingDirective) error {
	klog.Infof("Executing preemption for high-priority workload %s", directive.WorkloadID)
	// Preemption is handled by scaling down low-priority workloads
	// The actual scale-down directives are generated by the allocator
	return nil
}

func (e *Executor) recordExecution(record ExecutionRecord) {
	e.historyMutex.Lock()
	defer e.historyMutex.Unlock()

	e.history = append(e.history, record)

	// Keep only last 1000 records
	if len(e.history) > 1000 {
		e.history = e.history[len(e.history)-1000:]
	}
}

// GetHistory returns execution history
func (e *Executor) GetHistory() []ExecutionRecord {
	e.historyMutex.Lock()
	defer e.historyMutex.Unlock()

	result := make([]ExecutionRecord, len(e.history))
	copy(result, e.history)
	return result
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// Ensure appsv1 is used (for the import)
var _ = appsv1.SchemeGroupVersion
