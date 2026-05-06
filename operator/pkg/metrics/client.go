package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/airan/orchestrator/pkg/allocation"
)

// ClientConfig holds configuration for the Prometheus metrics client
type ClientConfig struct {
	PrometheusURL string
	Timeout       time.Duration
	CacheEnabled  bool
	CacheTTL      time.Duration

	// Default metric names
	LatencyMetricName    string
	CPUUtilizationMetric string
	GPUUtilizationMetric string
	MemoryUsageMetric    string
}

// DefaultClientConfig returns default configuration
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		PrometheusURL:        "http://prometheus:9090",
		Timeout:              10 * time.Second,
		CacheEnabled:         true,
		CacheTTL:             15 * time.Second,
		LatencyMetricName:    "workload_request_latency_ms",
		CPUUtilizationMetric: "container_cpu_usage_seconds_total",
		GPUUtilizationMetric: "nvidia_gpu_duty_cycle",
		MemoryUsageMetric:    "container_memory_usage_bytes",
	}
}

// Client implements MetricsClient using Prometheus
type Client struct {
	config     ClientConfig
	httpClient *http.Client

	cache      map[string]*cachedMetrics
	cacheMutex sync.RWMutex
}

type cachedMetrics struct {
	metrics map[string]allocation.WorkloadMetrics
	expiry  time.Time
}

// NewClient creates a new Prometheus metrics client
func NewClient(config ClientConfig) (*Client, error) {
	return &Client{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		cache: make(map[string]*cachedMetrics),
	}, nil
}

// GetMetrics retrieves metrics for specified workloads
func (c *Client) GetMetrics(ctx context.Context, workloadIDs []string) (map[string]allocation.WorkloadMetrics, error) {
	// Check cache
	if c.config.CacheEnabled {
		if cached := c.getFromCache("workloads"); cached != nil {
			return cached, nil
		}
	}

	result := make(map[string]allocation.WorkloadMetrics)

	for _, id := range workloadIDs {
		metrics, err := c.getWorkloadMetrics(ctx, id)
		if err != nil {
			klog.Warningf("Failed to get metrics for %s: %v", id, err)
			continue
		}
		result[id] = *metrics
	}

	// Update cache
	if c.config.CacheEnabled {
		c.putInCache("workloads", result)
	}

	return result, nil
}

func (c *Client) getWorkloadMetrics(ctx context.Context, workloadID string) (*allocation.WorkloadMetrics, error) {
	// Parse workloadID (format: "namespace/name")
	namespace, name := parseWorkloadID(workloadID)

	// Query latency - try custom metric first, fall back to request duration
	var latency float64
	var err error

	// Try custom latency metric
	latencyQuery := fmt.Sprintf(`avg(rate(%s{namespace="%s",pod=~"%s.*"}[5m]))`,
		c.config.LatencyMetricName, namespace, name)
	latency, err = c.queryPrometheus(ctx, latencyQuery)
	if err != nil {
		// Fall back to request duration histogram if available
		latencyQuery = fmt.Sprintf(`histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{namespace="%s",pod=~"%s.*"}[5m])) by (le)) * 1000`,
			namespace, name)
		latency, err = c.queryPrometheus(ctx, latencyQuery)
		if err != nil {
			klog.V(4).Infof("Latency query failed for %s: %v", workloadID, err)
		}
	}

	// Query CPU utilization - use namespace and pod name pattern
	cpuQuery := fmt.Sprintf(`sum(rate(%s{namespace="%s",pod=~"%s.*",container!="",container!="POD"}[5m])) by (pod)`,
		c.config.CPUUtilizationMetric, namespace, name)
	cpu, err := c.queryPrometheus(ctx, cpuQuery)
	if err != nil {
		klog.V(4).Infof("CPU query failed for %s: %v", workloadID, err)
	}

	// Query GPU utilization using DCGM metrics
	gpuQuery := fmt.Sprintf(`avg(DCGM_FI_DEV_GPU_UTIL{namespace="%s",pod=~"%s.*"})`, namespace, name)
	gpu, err := c.queryPrometheus(ctx, gpuQuery)
	if err != nil {
		// Try alternate GPU metric name
		gpuQuery = fmt.Sprintf(`avg(%s{namespace="%s",pod=~"%s.*"})`,
			c.config.GPUUtilizationMetric, namespace, name)
		gpu, err = c.queryPrometheus(ctx, gpuQuery)
		if err != nil {
			klog.V(4).Infof("GPU query failed for %s: %v", workloadID, err)
		}
	}

	// Query memory usage
	memQuery := fmt.Sprintf(`sum(%s{namespace="%s",pod=~"%s.*",container!="",container!="POD"}) by (pod)`,
		c.config.MemoryUsageMetric, namespace, name)
	mem, err := c.queryPrometheus(ctx, memQuery)
	if err != nil {
		klog.V(4).Infof("Memory query failed for %s: %v", workloadID, err)
	}

	klog.V(3).Infof("Metrics for %s: latency=%.2fms, cpu=%.4f, gpu=%.2f%%, mem=%.0f bytes",
		workloadID, latency, cpu, gpu, mem)

	return &allocation.WorkloadMetrics{
		WorkloadID:     workloadID,
		CurrentLatency: time.Duration(latency * float64(time.Millisecond)),
		CPUUtilization: cpu,
		GPUUtilization: gpu,
		MemoryUsage:    int64(mem),
		Timestamp:      time.Now(),
	}, nil
}

// parseWorkloadID splits "namespace/name" into components
func parseWorkloadID(workloadID string) (namespace, name string) {
	parts := strings.SplitN(workloadID, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "default", workloadID
}

// GetNodeMetrics retrieves metrics for cluster nodes
func (c *Client) GetNodeMetrics(ctx context.Context) ([]allocation.NodeMetrics, error) {
	// Query node CPU utilization using node_exporter metrics
	// Use instance label which contains the node IP/hostname
	cpuQuery := `100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)`

	resp, err := c.queryPrometheusRaw(ctx, cpuQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query node CPU: %w", err)
	}

	var nodeMetrics []allocation.NodeMetrics
	for _, result := range resp.Data.Result {
		// Try to get node name from different labels
		nodeName := ""
		if n, ok := result.Metric["node"]; ok {
			nodeName = n
		} else if n, ok := result.Metric["instance"]; ok {
			nodeName = n
		}

		if nodeName == "" {
			continue
		}

		cpu := 0.0
		if len(result.Value) > 1 {
			if v, ok := result.Value[1].(string); ok {
				fmt.Sscanf(v, "%f", &cpu)
			}
		}

		// Query memory for this node
		memQuery := fmt.Sprintf(`100 - ((node_memory_MemAvailable_bytes{instance="%s"} / node_memory_MemTotal_bytes{instance="%s"}) * 100)`, nodeName, nodeName)
		memUtil, _ := c.queryPrometheus(ctx, memQuery)

		// Query GPU availability for this node
		gpuQuery := fmt.Sprintf(`count(DCGM_FI_DEV_GPU_UTIL{instance=~"%s.*"}) or vector(0)`, nodeName)
		gpuCount, _ := c.queryPrometheus(ctx, gpuQuery)

		nodeMetrics = append(nodeMetrics, allocation.NodeMetrics{
			NodeName:          nodeName,
			CPUUtilization:    cpu / 100.0, // Convert to 0-1 range
			MemoryUtilization: memUtil / 100.0,
			GPUAvailable:      int(gpuCount) > 0,
			GPUCount:          int(gpuCount),
		})

		klog.V(3).Infof("Node %s: CPU=%.1f%%, Mem=%.1f%%, GPUs=%d",
			nodeName, cpu, memUtil, int(gpuCount))
	}

	return nodeMetrics, nil
}

func (c *Client) queryPrometheus(ctx context.Context, query string) (float64, error) {
	resp, err := c.queryPrometheusRaw(ctx, query)
	if err != nil {
		return 0, err
	}

	if len(resp.Data.Result) == 0 {
		return 0, fmt.Errorf("no data returned")
	}

	if len(resp.Data.Result[0].Value) < 2 {
		return 0, fmt.Errorf("invalid response format")
	}

	valueStr, ok := resp.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("invalid value format")
	}

	var value float64
	fmt.Sscanf(valueStr, "%f", &value)
	return value, nil
}

type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (c *Client) queryPrometheusRaw(ctx context.Context, query string) (*prometheusResponse, error) {
	u, _ := url.Parse(c.config.PrometheusURL + "/api/v1/query")
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var promResp prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&promResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if promResp.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed: %s", promResp.Status)
	}

	return &promResp, nil
}

func (c *Client) getFromCache(key string) map[string]allocation.WorkloadMetrics {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	cached, ok := c.cache[key]
	if !ok || time.Now().After(cached.expiry) {
		return nil
	}
	return cached.metrics
}

func (c *Client) putInCache(key string, metrics map[string]allocation.WorkloadMetrics) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	c.cache[key] = &cachedMetrics{
		metrics: metrics,
		expiry:  time.Now().Add(c.config.CacheTTL),
	}
}

// ClusterStateProvider implements allocation.ClusterStateProvider
type ClusterStateProvider struct {
	metricsClient *Client
}

// NewClusterStateProvider creates a new cluster state provider
func NewClusterStateProvider(client *Client) *ClusterStateProvider {
	return &ClusterStateProvider{metricsClient: client}
}

// GetState retrieves current cluster state
func (p *ClusterStateProvider) GetState(ctx context.Context) (*allocation.ClusterState, error) {
	nodeMetrics, err := p.metricsClient.GetNodeMetrics(ctx)
	if err != nil {
		klog.Warningf("Failed to get node metrics: %v", err)
	}

	var nodes []allocation.NodeState
	for _, nm := range nodeMetrics {
		nodes = append(nodes, allocation.NodeState{
			Name:   nm.NodeName,
			Labels: make(map[string]string), // TODO: Get from Kubernetes API
		})
	}

	return &allocation.ClusterState{
		Nodes:     nodes,
		Timestamp: time.Now(),
	}, nil
}

// GetNodeState retrieves state for a specific node
func (p *ClusterStateProvider) GetNodeState(ctx context.Context, nodeName string) (*allocation.NodeState, error) {
	state, err := p.GetState(ctx)
	if err != nil {
		return nil, err
	}

	for _, node := range state.Nodes {
		if node.Name == nodeName {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("node %s not found", nodeName)
}

// GetMIGPartitions retrieves MIG partition information
func (p *ClusterStateProvider) GetMIGPartitions(ctx context.Context, nodeName string) ([]allocation.MIGPartition, error) {
	// TODO: Implement MIG partition discovery
	return nil, nil
}
