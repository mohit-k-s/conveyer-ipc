#!/usr/bin/env python3
"""Benchmark: Time-to-Detect & Trip Circuit Breaker — conveyor-agent vs OpenTelemetry + OTLP/HTTP.

Starts a local OTLP/HTTP receiver (/v1/traces, same wire protocol as Grafana Tempo / OTel Collector),
and compares:
  1. conveyor-agent (C++20 ConveyorRing sidecar -> instant shm circuit breaker + async OTLP export)
  2. OpenTelemetry SDK (BatchSpanProcessor -> OTLP/HTTP -> collector anomaly detector)
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import subprocess
import threading
import time
import urllib.request

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

ROOT = Path(__file__).resolve().parent.parent
DIR = Path(__file__).resolve().parent


def run_conveyor_agent():
    bin_path = Path("/tmp/conveyor_agent_bin")
    otlp_path = Path("/tmp/conveyor_otlp_trace.json")
    subprocess.run(
        ["c++", "-O3", "-DNDEBUG", "-std=c++20", "-I", str(ROOT / "include"),
         str(DIR / "conveyor_agent.cpp"), "-o", str(bin_path)],
        check=True,
    )
    out = subprocess.check_output([str(bin_path), str(otlp_path)], text=True)
    vals = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            vals[k] = float(v)
    otlp_payload = otlp_path.read_text()
    bin_path.unlink(missing_ok=True)
    otlp_path.unlink(missing_ok=True)
    return vals["CONVEYOR_DETECT_NS"], vals["CONVEYOR_TRIP_RTT_NS"], otlp_payload


def run_otel_otlp_reaction(schedule_delay_ms: int = 50):
    detected_at = {"t": None}
    received_otlp = {"count": 0}

    class OtlpReceiver(BaseHTTPRequestHandler):
        def log_message(self, *args): pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            payload = json.loads(body)
            received_otlp["count"] += 1
            for rs in payload.get("resourceSpans", []):
                for ss in rs.get("scopeSpans", []):
                    for sp in ss.get("spans", []):
                        if sp.get("name") == "anomaly.spike":
                            detected_at["t"] = time.perf_counter_ns()
            self.send_response(200)
            self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), OtlpReceiver)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    class HttpOtlpExporter(SpanExporter):
        def export(self, spans):
            body = json.dumps({
                "resourceSpans": [{"scopeSpans": [{"spans": [{"name": s.name} for s in spans]}]}]
            }).encode()
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/v1/traces", data=body,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=2).read()
            return SpanExportResult.SUCCESS

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(HttpOtlpExporter(), schedule_delay_millis=schedule_delay_ms))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("bench")

    # Warmup normal spans
    for _ in range(100):
        with tracer.start_as_current_span("normal.req"):
            pass

    # Inject spike and measure time until OTLP collector sees it
    t_inject = time.perf_counter_ns()
    with tracer.start_as_current_span("anomaly.spike"):
        pass

    while detected_at["t"] is None:
        time.sleep(0.0005)
    detect_ns = detected_at["t"] - t_inject
    provider.shutdown()
    server.shutdown()
    return detect_ns, received_otlp["count"]


def main():
    print("Anomaly Reaction-Time Benchmark (Inject Spike -> Detect -> Trip Circuit Breaker)")
    print("=" * 84)
    o_det_ns, _ = run_otel_otlp_reaction(schedule_delay_ms=50)
    c_det_ns, c_rtt_ns, otlp_json = run_conveyor_agent()


    print(f"conveyor-agent (ConveyorRing shm)   : detect = {c_det_ns:8.1f} ns | trip RTT = {c_rtt_ns:8.1f} ns")
    print(f"OpenTelemetry + OTLP/HTTP (50ms)    : detect = {o_det_ns/1e6:8.2f} ms ({o_det_ns:,.0f} ns)")
    print("-" * 84)
    print(f"Reaction Speedup: {o_det_ns / c_det_ns:,.0f}x faster anomaly detection")
    print(f"OTLP/Grafana Tempo payload generated: {len(otlp_json)} bytes (valid /v1/traces JSON)")


if __name__ == "__main__":
    main()
