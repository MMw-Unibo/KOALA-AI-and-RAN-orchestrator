import inference_pb2
import inference_pb2_grpc
import os
import time
import json
import pickle
import threading
import logging
from collections import deque
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

import grpc
from concurrent import futures
import numpy as np
from influxdb_client_3 import InfluxDBClient3, write_client_options, SYNCHRONOUS

# Optional: Kubernetes client for circuit-break-scaling mechanism
try:
    from kubernetes import client as k8s_client, config as k8s_config
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False

# Optional: Prometheus metrics
try:
    from prometheus_client import (
        Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
    )
    PROM_AVAILABLE = True
except ImportError:
    PROM_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ndt-orchestrator")

# ── Environment variables ────────────────────────────────────────────────────
MODEL_HOST = os.getenv("MODEL_HOST", "ndt-model:50051")
INFLUX_HOST = os.getenv("INFLUX_HOST", "http://influxdb:8181")
INFLUX_DB = os.getenv("INFLUX_DB", "network_traffic")
RMSE_WINDOW_SIZE = int(os.getenv("RMSE_WINDOW_SIZE", "6"))
PREDICTION_STEP_SECONDS = int(os.getenv("PREDICTION_STEP_SECONDS", "600"))
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))

NDT_NAMESPACE = os.getenv("NDT_NAMESPACE", "ndt")


# ── Prometheus metrics ───────────────────────────────────────────────────────
if PROM_AVAILABLE:
    PROM_LATENCY = Histogram(
        "ndt_inference_latency_seconds",
        "Inference request latency in seconds",
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    )
    PROM_RMSE = Gauge("ndt_rmse_current", "Current rolling RMSE value")
    PROM_ACTIVE_MODEL = Gauge(
        "ndt_active_model", "Active model (0=multi-step, 1=single-step)"
    )
    PROM_SWITCH_TOTAL = Counter(
        "ndt_switch_total", "Total number of model switches"
    )


# ── RMSE Monitor ─────────────────────────────────────────────────────────────
class RMSEMonitor:
    """Tracks predictions vs InfluxDB ground truth using a sliding window."""

    def __init__(self, scaler_path, influx_host, influx_db,
                 window_size=6,
                 step_seconds=600):
        self.window_size = window_size
        self.step_seconds = step_seconds
        self.buffer = deque(maxlen=window_size)
        self.last_request_timestamp=-1
        self._lock = threading.Lock()


        log.info("Loading scaler from %s", scaler_path)
        with open(scaler_path, "rb") as f:
            self.scaler = pickle.load(f)

        log.info("Connecting to InfluxDB at %s (db=%s)", influx_host, influx_db)
        wco = write_client_options(write_options=SYNCHRONOUS)
        self.influx_client = InfluxDBClient3(
            host=influx_host, database=influx_db, write_client_options=wco,
        )

    def add_prediction(self, request_timestamp_seconds, predictions):
        """Store a prediction and compare with actuals from InfluxDB."""
        if(request_timestamp_seconds> self.last_request_timestamp):
            self.last_request_timestamp=request_timestamp_seconds
            n_steps = len(predictions)
            actuals = self._fetch_actuals(request_timestamp_seconds, n_steps)
            if actuals is None or len(actuals) == 0:
                log.warning("No actuals available for timestamp %s", request_timestamp_seconds)
                return

            # Inverse-transform predictions (they are in scaled space)
            pred_arr = np.array(predictions).reshape(-1, 1)
            pred_unscaled = self.scaler.inverse_transform(pred_arr).flatten()

            # Actuals are raw from InfluxDB, no need to inverse-transform
            actual_arr = np.array(actuals[:len(pred_unscaled)])
            pred_unscaled = pred_unscaled[:len(actual_arr)]

            if len(actual_arr) == 0:
                return

            rmse = np.sqrt(np.mean((pred_unscaled - actual_arr) ** 2))
            with self._lock:
                self.buffer.append(rmse)
            log.info("RMSE for ts=%d: %.4f (buffer size: %d)",
                    request_timestamp_seconds, rmse, len(self.buffer))

    def _fetch_actuals(self, request_timestamp_seconds, n_steps):
        """Query InfluxDB for actual values at future timestamps."""
        try:
            start_time = datetime.fromtimestamp(
                request_timestamp_seconds + self.step_seconds
            )
            end_time = datetime.fromtimestamp(
                request_timestamp_seconds + n_steps * self.step_seconds
            )
            result = self.influx_client.query(
                f"""SELECT * FROM internet
                    WHERE time >= to_timestamp('{start_time}')
                      AND time <= to_timestamp('{end_time}')
                    ORDER BY time ASC
                    LIMIT {n_steps}""",
                mode="pandas",
            )
            if result is not None and len(result) > 0:
                return result["internet"].values.tolist()
        except Exception as e:
            log.error("Error fetching actuals: %s", e)
        return None

    def compute_rolling_rmse(self):
        with self._lock:
            if len(self.buffer) == 0:
                return 0.0
            return float(np.mean(list(self.buffer)))

# ── gRPC Orchestrator Servicer ───────────────────────────────────────────────
class OrchestratorServicer(inference_pb2_grpc.InferenceServicer):

    def __init__(self, rmse_monitor):
        super().__init__()
        self.rmse_monitor = rmse_monitor
        self._lock = threading.Lock()

        # gRPC channels to backends
        self.model_channel = grpc.insecure_channel(MODEL_HOST)
        self.model_stub = inference_pb2_grpc.InferenceStub(self.model_channel)

    def make_inference(self, request, context):
        start_time = time.time()
        request_ts = int(request.datetime.seconds/PREDICTION_STEP_SECONDS)*PREDICTION_STEP_SECONDS

        try:
            response = self.model_stub.make_inference(request)
        except grpc.RpcError as e:
            log.error("Backend unavailable")
        elapsed = time.time() - start_time

        # Record Prometheus metrics
        if PROM_AVAILABLE:
            PROM_LATENCY.observe(elapsed)

        # Feed predictions to RMSE monitor (in background to not block response)
        predictions = list(response.predictions)
        threading.Thread(
            target=self.rmse_monitor.add_prediction,
            args=(request_ts, predictions),
            daemon=True,
        ).start()
        
        # Add metadata for benchmark client
        context.set_trailing_metadata([
            ("x-active-model", "lstm-5060-30m"),
            # ("x-current-rmse", str(self.rmse_monitor.compute_rolling_rmse())),
            ("x-current-rmse", str(0.9)),
            ("x-model-latency-ms", str(elapsed * 1000)),
        ])

        return response

    def get_status(self):
        return {
            "current_rmse": round(self.rmse_monitor.compute_rolling_rmse(), 4),
            "rmse_buffer_size": len(self.rmse_monitor.buffer),
        }


# ── HTTP Status Server ───────────────────────────────────────────────────────
_orchestrator_ref = None  # set after OrchestratorServicer is created


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/status":
            self._json_response(200, _orchestrator_ref.get_status())
        elif self.path == "/healthz":
            self._json_response(200, {"status": "ok"})
        elif self.path == "/metrics" and PROM_AVAILABLE:
            # Update gauge before serving
            if _orchestrator_ref:
                PROM_RMSE.set(_orchestrator_ref.rmse_monitor.compute_rolling_rmse())
            data = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(data)
        else:
            self._json_response(404, {"error": "not found"})

    def _json_response(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        pass  # suppress default HTTP logs


def start_http_server(port):
    server = HTTPServer(("0.0.0.0", port), StatusHandler)
    log.info("HTTP status server listening on :%d", port)
    server.serve_forever()


# ── Main ─────────────────────────────────────────────────────────────────────
def serve():
    global _orchestrator_ref

    rmse_monitor = RMSEMonitor(
        scaler_path="scaler.pkl",
        influx_host=INFLUX_HOST,
        influx_db=INFLUX_DB,
        window_size=RMSE_WINDOW_SIZE,
        step_seconds=PREDICTION_STEP_SECONDS,
    )

    orchestrator = OrchestratorServicer(rmse_monitor)
    _orchestrator_ref = orchestrator

    # Start HTTP server in background
    http_thread = threading.Thread(
        target=start_http_server, args=(HTTP_PORT,), daemon=True
    )
    http_thread.start()

    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inference_pb2_grpc.add_InferenceServicer_to_server(orchestrator, server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
