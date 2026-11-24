#!/usr/bin/env python3
"""Compile & run the ConveyorRing Black-Box Flight Recorder and generate dashboard.html."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
DIR = Path(__file__).resolve().parent
BIN = Path("/tmp/conveyor_blackbox")
DUMP = Path("/tmp/blackbox_dump.json")
HTML_OUT = DIR / "dashboard.html"

subprocess.run(
    ["c++", "-O3", "-DNDEBUG", "-std=c++20", "-I", str(ROOT / "include"),
     str(DIR / "blackbox.cpp"), "-o", str(BIN)],
    check=True,
)
subprocess.run([str(BIN), str(DUMP)], check=True)
data = json.loads(DUMP.read_text())
BIN.unlink(missing_ok=True)
DUMP.unlink(missing_ok=True)

HTML = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>ConveyorRing Black-Box Flight Recorder</title>
<style>
  :root {{ --bg:#0b0f17; --card:#131a26; --border:#233044; --text:#e6edf7; --muted:#8fa1bc; --ok:#22c55e; --warn:#f59e0b; --crit:#ef4444; --accent:#38bdf8; }}
  * {{ box-sizing:border-box; font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display',Inter,sans-serif; }}
  body {{ margin:0; background:var(--bg); color:var(--text); padding:28px; }}
  .wrap {{ max-width:1100px; margin:0 auto; }}
  h1 {{ margin:0 0 6px; font-size:24px; letter-spacing:-0.3px; }}
  .sub {{ color:var(--muted); margin:0 0 22px; font-size:14px; }}
  .grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:20px; }}
  .card {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:16px; }}
  .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:0.6px; }}
  .val {{ font-size:26px; font-weight:700; margin-top:6px; }}
  .val.ok {{ color:var(--ok); }} .val.crit {{ color:var(--crit); }} .val.accent {{ color:var(--accent); }}
  .panel {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:18px; margin-bottom:20px; }}
  canvas {{ width:100%; height:200px; display:block; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
  th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }}
  th {{ color:var(--muted); font-weight:600; }}
  .badge {{ padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; }}
  .b0 {{ background:rgba(34,197,94,.15); color:var(--ok); }}
  .b1 {{ background:rgba(245,158,11,.18); color:var(--warn); }}
  .b2 {{ background:rgba(239,68,68,.22); color:var(--crit); }}
  .scroll {{ max-height:360px; overflow:auto; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>ConveyorRing Black-Box Flight Recorder</h1>
  <p class="sub">Zero-syscall, 100%-unsampled cross-process telemetry — Hot-Path App (PID {data['app_pid']}) crashed with <b>SIGSEGV ({data['crash_signal']})</b>; Collector Sidecar (PID {data['collector_pid']}) survived and captured 100% of pre-crash events.</p>

  <div class="grid">
    <div class="card"><div class="label">Ingestion Rate</div><div class="val accent">{data['events_per_s_m']:.2f} M/s</div></div>
    <div class="card"><div class="label">Events Captured</div><div class="val ok">{data['captured']:,}</div></div>
    <div class="card"><div class="label">Dropped Events</div><div class="val ok">{data['dropped']}</div></div>
    <div class="card"><div class="label">App Crash Signal</div><div class="val crit">SIGSEGV ({data['crash_signal']})</div></div>
  </div>

  <div class="panel">
    <div class="label" style="margin-bottom:10px;">Last {len(data['tail_events'])} Events Before Crash — Latency Timeline (ns)</div>
    <canvas id="chart" width="1040" height="200"></canvas>
  </div>

  <div class="panel">
    <div class="label" style="margin-bottom:10px;">Pre-Crash Nanosecond Event Log (Most Recent Last)</div>
    <div class="scroll">
      <table>
        <thead><tr><th>Seq</th><th>Time (ms)</th><th>Trace ID</th><th>Event</th><th>Latency</th><th>Status</th></tr></thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
  </div>
</div>
<script>
const DATA = {json.dumps(data)};
const KINDS = ["REQ_START","CACHE_HIT","GPU_INFER","ORDER_EXEC","ANOMALY_SPIKE","PRE_CRASH_FATAL"];
const STATUS = [["OK","b0"],["SPIKE","b1"],["CRITICAL","b2"]];
const evs = DATA.tail_events;

// Draw latency bar/line chart
const cv = document.getElementById("chart"), ctx = cv.getContext("2d");
const maxLat = Math.max(...evs.map(e => e.latency_ns), 1);
const pad = 24, w = cv.width - pad*2, h = cv.height - pad*2;
ctx.strokeStyle = "#233044"; ctx.beginPath();
for (let i = 0; i <= 4; i++) {{ const y = pad + (h/4)*i; ctx.moveTo(pad, y); ctx.lineTo(cv.width-pad, y); }}
ctx.stroke();

const barW = Math.max(2, w / evs.length - 1);
evs.forEach((e, i) => {{
  const x = pad + (i / evs.length) * w;
  const bh = (e.latency_ns / maxLat) * h;
  ctx.fillStyle = e.status === 2 ? "#ef4444" : (e.status === 1 ? "#f59e0b" : "#38bdf8");
  ctx.fillRect(x, pad + h - bh, barW, bh);
}});

// Populate table
const tbody = document.getElementById("rows");
evs.slice(-60).forEach(e => {{
  const [stLabel, stCls] = STATUS[e.status] || STATUS[0];
  tbody.innerHTML += `<tr>
    <td>#${{e.seq.toLocaleString()}}</td>
    <td>${{(e.ts_ns / 1e6).toFixed(3)}} ms</td>
    <td>${{e.trace_id}}</td>
    <td>${{KINDS[e.kind] || e.kind}}</td>
    <td>${{e.latency_ns.toLocaleString()}} ns</td>
    <td><span class="badge ${{stCls}}">${{stLabel}}</span></td>
  </tr>`;
}});
</script>
</body>
</html>
"""
HTML_OUT.write_text(HTML)
print(f"Dashboard generated: {HTML_OUT}")
