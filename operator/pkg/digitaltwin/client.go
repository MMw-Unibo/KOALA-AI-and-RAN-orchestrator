// Package digitaltwin provides a gRPC client for the Network Digital Twin (NDT)
// orchestrator deployed in the `ndt` Kubernetes namespace.
//
// The NDT orchestrator exposes the Inference service (proto/inference.proto):
//
//	rpc make_inference(InferenceRequest{datetime}) returns (InferenceResult{predictions})
//
// where `predictions` is an ordered list of scaled load values, one per
// prediction step (PREDICTION_STEP_SECONDS on the server, default 600 s).
// On every response, the orchestrator attaches trailing metadata:
//
//	x-active-model        — active model tag (e.g. "cnn1d-5060-1h")
//	x-current-rmse        — rolling RMSE over recent predictions (raw space)
//	x-model-latency-ms    — latency of the backend model RPC
//
// Historical patterns are fetched directly from the InfluxDB 3 Core instance
// that the NDT orchestrator itself uses for ground-truth (`internet` table in
// the `network_traffic` database).
package digitaltwin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"

	"github.com/airan/orchestrator/pkg/allocation"
	ndtpb "github.com/airan/orchestrator/pkg/digitaltwin/ndtpb"
)

// ClientConfig holds configuration for the Digital Twin client.
type ClientConfig struct {
	// Address is the gRPC endpoint of the NDT orchestrator
	// (e.g. "ndt-orchestrator.ndt.svc.cluster.local:50051" in-cluster,
	// "<node-ip>:32351" via NodePort).
	Address string

	// InfluxURL is the base URL of the NDT InfluxDB 3 Core instance used
	// for historical queries (e.g. "http://influxdb.ndt.svc.cluster.local:8181").
	InfluxURL string

	// InfluxDB is the database name (matches INFLUX_DB on the NDT pods).
	InfluxDB string

	// PredictionStep is the step between consecutive predictions returned by
	// the model. Must match PREDICTION_STEP_SECONDS on the NDT orchestrator.
	PredictionStep time.Duration

	// Timeout for individual RPC / HTTP calls.
	Timeout time.Duration

	// EnableTLS enables TLS for the gRPC connection.
	EnableTLS bool
	// CertFile is the path to the TLS certificate (if EnableTLS is true).
	CertFile string

	// RetryAttempts is the number of retry attempts for failed calls.
	RetryAttempts int
	// RetryDelay is the delay between retry attempts.
	RetryDelay time.Duration

	// CacheEnabled enables caching of predictions.
	CacheEnabled bool
	// CacheTTL is the cache time-to-live.
	CacheTTL time.Duration

	// RMSEDegradedAt is the RMSE (raw space) at which confidence drops to 0.
	// The NDT orchestrator alerts at RMSE >= 18.0 (RMSE_THRESHOLD), so a
	// degradation ceiling slightly above that gives a monotonic 0..1 mapping.
	RMSEDegradedAt float64

	// TimeStart is the reference time for predictions. The client will compute
	// the request time as TimeStart + (now - initTime), allowing tests to use
	// a fixed time window.
	TimeStart time.Time
}

// DefaultClientConfig returns sensible defaults for in-cluster use.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Address:        "ndt-api.ndt.svc.cluster.local:50051",
		InfluxURL:      "http://influxdb.ndt.svc.cluster.local:8181",
		InfluxDB:       "network_traffic",
		PredictionStep: 10 * time.Minute,
		Timeout:        5 * time.Second,
		EnableTLS:      false,
		RetryAttempts:  3,
		RetryDelay:     500 * time.Millisecond,
		CacheEnabled:   false,
		CacheTTL:       30 * time.Second,
		RMSEDegradedAt: 90.0,
		TimeStart:      time.Date(2013, 12, 30, 11, 20, 0, 0, time.UTC), //timestamp 1388405400
	}
}

// Client is a gRPC client for the NDT inference service.
type Client struct {
	config ClientConfig
	conn   *grpc.ClientConn
	stub   ndtpb.InferenceClient
	http   *http.Client

	cache      map[string]*cachedPrediction
	cacheMutex sync.RWMutex

	requestCount  atomic.Int64
	cacheHitCount atomic.Int64
	errorCount    atomic.Int64

	initTime time.Time
}

type cachedPrediction struct {
	prediction *allocation.TrafficPrediction
	expiry     time.Time
}

// NewClient creates a new Digital Twin client.
func NewClient(config ClientConfig) (*Client, error) {
	if config.PredictionStep <= 0 {
		config.PredictionStep = 10 * time.Minute
	}
	if config.RMSEDegradedAt <= 0 {
		config.RMSEDegradedAt = 25.0
	}

	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if config.EnableTLS {
		klog.Warning("Digital Twin TLS requested but not implemented; falling back to insecure")
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(config.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Digital Twin service at %s: %w", config.Address, err)
	}

	return &Client{
		config:   config,
		conn:     conn,
		stub:     ndtpb.NewInferenceClient(conn),
		http:     &http.Client{Timeout: config.Timeout},
		cache:    make(map[string]*cachedPrediction),
		initTime: time.Now(),
	}, nil
}

// GetPrediction retrieves traffic prediction for the specified horizon.
// Implements allocation.DigitalTwinClient.
func (c *Client) GetPrediction(ctx context.Context, horizon time.Duration) (*allocation.TrafficPrediction, error) {
	stepIdx := int(horizon/c.config.PredictionStep) - 1
	if stepIdx < 0 {
		stepIdx = 0
	}
	cacheKey := fmt.Sprintf("prediction_%d", stepIdx)

	if c.config.CacheEnabled {
		if cached := c.getFromCache(cacheKey); cached != nil {
			c.cacheHitCount.Add(1)
			return cached, nil
		}
	}
	c.requestCount.Add(1)

	var lastErr error
	for attempt := 0; attempt <= c.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.config.RetryDelay):
			}
			klog.V(3).Infof("Retrying Digital Twin prediction request (attempt %d)", attempt)
		}

		prediction, err := c.doGetPrediction(ctx, horizon, stepIdx)
		if err == nil {
			if c.config.CacheEnabled {
				c.putInCache(cacheKey, prediction)
			}
			return prediction, nil
		}
		lastErr = err
	}

	c.errorCount.Add(1)
	return nil, fmt.Errorf("failed to get prediction after %d attempts: %w", c.config.RetryAttempts, lastErr)
}

func (c *Client) doGetPrediction(ctx context.Context, horizon time.Duration, stepIdx int) (*allocation.TrafficPrediction, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	//now := time.Now()
	//req := &ndtpb.InferenceRequest{Datetime: timestamppb.New(now)}

	// Determine the base request time
	reqTime := time.Now()
	if !c.config.TimeStart.IsZero() {
		// If we have a historical start, offset it by the process uptime
		reqTime = c.config.TimeStart.Add(time.Since(c.initTime))
	}
	req := &ndtpb.InferenceRequest{Datetime: timestamppb.New(reqTime)}

	var trailer metadata.MD
	resp, err := c.stub.MakeInference(callCtx, req, grpc.Trailer(&trailer))
	if err != nil {
		return nil, fmt.Errorf("MakeInference: %w", err)
	}
	preds := resp.GetPredictions()
	if len(preds) == 0 {
		return nil, fmt.Errorf("digital twin returned empty prediction vector")
	}

	idx := stepIdx
	if idx >= len(preds) {
		idx = len(preds) - 1
	}
	load := clamp01(float64(preds[idx]))

	confidence := c.confidenceFromTrailer(trailer)
	modelVersion := firstMeta(trailer, "x-active-model")
	if modelVersion != "" {
		klog.V(4).Infof("DT prediction: load=%.3f conf=%.2f model=%s horizon=%s idx=%d/%d",
			load, confidence, modelVersion, horizon, idx, len(preds))
	}

	return &allocation.TrafficPrediction{
		Timestamp:      reqTime.Add(horizon),
		PredictedLoad:  load,
		Confidence:     confidence,
		HorizonMinutes: int(horizon.Minutes()),
	}, nil
}

// confidenceFromTrailer maps the rolling RMSE (raw space) reported by the NDT
// orchestrator into a [0,1] confidence score.
func (c *Client) confidenceFromTrailer(md metadata.MD) float64 {
	raw := firstMeta(md, "x-current-rmse")
	if raw == "" {
		return 0.85
	}
	rmse, err := strconv.ParseFloat(raw, 64)
	if err != nil || rmse < 0 {
		return 0.85
	}
	return clamp01(1.0 - rmse/c.config.RMSEDegradedAt)
}

func firstMeta(md metadata.MD, key string) string {
	if md == nil {
		return ""
	}
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func clamp01(v float64) float64 {
	if math.IsNaN(v) || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// influxRow is one row of the InfluxDB 3 Core /api/v3/query_sql JSON response.
// `time` is serialized as an RFC3339 string by default; `internet` is numeric.
type influxRow struct {
	Time     string  `json:"time"`
	Internet float64 `json:"internet"`
}

// GetHistoricalPattern retrieves historical traffic from the NDT InfluxDB.
// Implements allocation.DigitalTwinClient.
func (c *Client) GetHistoricalPattern(ctx context.Context, period time.Duration) ([]allocation.TrafficDataPoint, error) {
	if c.config.InfluxURL == "" {
		return nil, fmt.Errorf("InfluxURL not configured")
	}
	callCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	currTime := time.Now()
	if !c.config.TimeStart.IsZero() {
		// If we have a historical start, offset it by the process uptime
		currTime = c.config.TimeStart.Add(time.Since(c.initTime))
	}

	start := currTime.Add(-period).UTC().Format(time.RFC3339)
	q := fmt.Sprintf(
		"SELECT time, internet FROM internet WHERE time >= cast('%s' as timestamp) ORDER BY time ASC",
		start,
	)

	u, err := url.Parse(c.config.InfluxURL)
	if err != nil {
		return nil, fmt.Errorf("invalid InfluxURL %q: %w", c.config.InfluxURL, err)
	}
	u.Path = "/api/v3/query_sql"
	qs := u.Query()
	qs.Set("db", c.config.InfluxDB)
	qs.Set("q", q)
	qs.Set("format", "json")
	u.RawQuery = qs.Encode()

	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build InfluxDB request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("InfluxDB query: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read InfluxDB response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("InfluxDB query failed: %s: %s", resp.Status, string(body))
	}

	var rows []influxRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode InfluxDB response: %w (body=%s)", err, truncate(string(body), 200))
	}

	out := make([]allocation.TrafficDataPoint, 0, len(rows))
	for _, r := range rows {
		ts, perr := parseInfluxTime(r.Time)
		if perr != nil {
			klog.V(4).Infof("skipping row with unparsable time %q: %v", r.Time, perr)
			continue
		}
		out = append(out, allocation.TrafficDataPoint{
			Timestamp: ts,
			Load:      r.Internet,
		})
	}
	return out, nil
}

func parseInfluxTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time format: %q", s)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// GetCellPredictions returns a per-cell view of the aggregate prediction.
// The deployed NDT is single-cell (cell 5060), so this just mirrors the
// aggregate prediction for each requested cell ID.
func (c *Client) GetCellPredictions(ctx context.Context, cellIDs []string, horizon time.Duration) (map[string]*CellPrediction, error) {
	base, err := c.GetPrediction(ctx, horizon)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*CellPrediction, len(cellIDs))
	for _, id := range cellIDs {
		out[id] = &CellPrediction{
			CellID:        id,
			PredictedLoad: base.PredictedLoad,
			Confidence:    base.Confidence,
			PredictedUEs:  int(base.PredictedLoad * 100),
		}
	}
	return out, nil
}

// CellPrediction contains prediction for a specific cell.
type CellPrediction struct {
	CellID        string
	PredictedLoad float64
	Confidence    float64
	PredictedUEs  int
}

// ReportActualLoad is a no-op against this NDT: the orchestrator computes
// RMSE autonomously by reading actuals from InfluxDB. Kept for interface
// parity so callers don't have to branch on backend.
func (c *Client) ReportActualLoad(ctx context.Context, load float64, cellLoads map[string]float64) error {
	klog.V(4).Infof("ReportActualLoad noop: load=%.3f cells=%d", load, len(cellLoads))
	return nil
}

func (c *Client) getFromCache(key string) *allocation.TrafficPrediction {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	cached, ok := c.cache[key]
	if !ok {
		return nil
	}
	if time.Now().After(cached.expiry) {
		return nil
	}
	return cached.prediction
}

func (c *Client) putInCache(key string, prediction *allocation.TrafficPrediction) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	c.cache[key] = &cachedPrediction{
		prediction: prediction,
		expiry:     time.Now().Add(c.config.CacheTTL),
	}
}

// ClearCache clears the prediction cache.
func (c *Client) ClearCache() {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.cache = make(map[string]*cachedPrediction)
}

// Stats returns client statistics.
func (c *Client) Stats() ClientStats {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()

	return ClientStats{
		RequestCount:  c.requestCount.Load(),
		CacheHitCount: c.cacheHitCount.Load(),
		ErrorCount:    c.errorCount.Load(),
		CacheSize:     len(c.cache),
	}
}

// ClientStats contains client statistics.
type ClientStats struct {
	RequestCount  int64
	CacheHitCount int64
	ErrorCount    int64
	CacheSize     int
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// HealthCheck performs a liveness probe by issuing a minimal inference call.
func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := c.doGetPrediction(ctx, c.config.PredictionStep, 0)
	return err
}
