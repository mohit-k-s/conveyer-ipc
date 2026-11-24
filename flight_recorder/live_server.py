#!/usr/bin/env python3
"""Live Web Dashboard for ConveyorRing Black-Box Flight Recorder (http://localhost:8765)."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import threading

ROOT = Path(__file__).resolve().parent.parent
DIR = Path(__file__).resolve().parent
BIN = Path("/tmp/conveyor_live_collector")

subprocess.run(
    ["c++", "-O3", "-DNDEBUG", "-std=c++20", "-I", str(ROOT / "include"),
     str(DIR / "live_collector.cpp"), "-o", str(BIN)],
    check=True,
)

proc = subprocess.Popen(
    [str(BIN)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1
)
latest_json = '{"alive":true,"crash_sig":0,"app_pid":0,"collector_pid":0,"captured":0,"rate_m":0,"tail":[]}'
lock = threading.Lock()


def reader_loop():
    global latest_json
    for line in proc.stdout:
        line = line.strip()
        if line.startswith("{"):
            with lock:
                latest_json = line


threading.Thread(target=reader_loop, daemon=True).start()

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>ConveyorRing Live Flight Recorder</title>
<style>
  :root { --bg:#0b0f17; --card:#131a26; --border:#233044; --text:#e6edf7; --muted:#8fa1bc; --ok:#22c55e; --warn:#f59e0b; --crit:#ef4444; --accent:#38bdf8; }
  * { box-sizing:border-box; font-family:-apple-system,BlinkMacSystemFont,Inter,sans-serif; }
  body { margin:0; background:var(--bg); color:var(--text); padding:24px; }
  .wrap { max-width:1100px; margin:0 auto; }
  .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:18px; }
  h1 { margin:0; font-size:22px; }
  .btns button { border:none; border-radius:8px; padding:10px 16px; font-weight:600; cursor:pointer; margin-left:8px; font-size:13px; }
  .btn-crash { background:var(--crit); color:#fff; }
  .btn-restart { background:var(--ok); color:#052e16; }
  .grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:18px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px; }
  .label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.6px; }
  .val { font-size:24px; font-weight:700; margin-top:6px; }
  .panel { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:16px; margin-bottom:16px; }
  canvas { width:100%; height:190px; display:block; }
  table { width:100%; border-collapse:collapse; font-size:12px; font-family:ui-monospace,Menlo,monospace; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--border); }
  th { color:var(--muted); }
</style></head><body><div class="wrap">
  <div class="top">
    <div><h1>ConveyorRing Live Flight Recorder</h1><div id="sub" style="color:var(--muted);font-size:13px;margin-top:4px;">Connecting...</div></div>
    <div class="btns">
      <button class="btn-crash" onclick="fetch('/crash',{method:'POST'})">Trigger App Crash (SIGSEGV)</button>
      <button class="btn-restart" onclick="fetch('/restart',{method:'POST'})">Restart App</button>
    </div>
  </div>
  <div class="grid">
    <div class="card"><div class="label">Live Rate</div><div class="val" id="rate" style="color:var(--accent)">0.00 M/s</div></div>
    <div class="card"><div class="label">Total Captured</div><div class="val" id="cap" style="color:var(--ok)">0</div></div>
    <div class="card"><div class="label">Dropped</div><div class="val" style="color:var(--ok)">0</div></div>
    <div class="card"><div class="label">App Status</div><div class="val" id="st" style="color:var(--ok)">ALIVE</div></div>
  </div>
  <div class="panel"><div class="label" style="margin-bottom:8px;">Live Rolling Window — Latency (ns)</div><canvas id="cv" width="1040" height="190"></canvas></div>
  <div class="panel"><div class="label" style="margin-bottom:8px;">Latest Events (Frozen on Crash)</div>
    <table><thead><tr><th>Seq</th><th>Time (ms)</th><th>Event</th><th>Latency</th><th>Status</th></tr></thead><tbody id="tb"></tbody></table>
  </div>
</div>
<script>
const KINDS = ["REQ_START","CACHE_HIT","GPU_INFER","ORDER_EXEC","ANOMALY","PRE_CRASH_FATAL"];
const ST = ["OK","SPIKE","CRITICAL"];
async function tick() {
  try {
    const d = await (await fetch('/metrics')).json();
    document.getElementById('rate').textContent = d.rate_m.toFixed(2) + ' M/s';
    document.getElementById('cap').textContent = d.captured.toLocaleString();
    const st = document.getElementById('st');
    st.textContent = d.alive ? 'ALIVE' : ('CRASHED (SIG ' + d.crash_sig + ')');
    st.style.color = d.alive ? 'var(--ok)' : 'var(--crit)';
    document.getElementById('sub').textContent = `App PID ${d.app_pid} -> ConveyorRing shm -> Collector PID ${d.collector_pid}`;
    const cv = document.getElementById('cv'), ctx = cv.getContext('2d');
    ctx.clearRect(0, 0, cv.width, cv.height);
    const evs = d.tail || [];
    const maxL = Math.max(...evs.map(e => e.lat), 500);
    const w = cv.width - 20, h = cv.height - 20, bw = Math.max(4, w / Math.max(evs.length, 1) - 2);
    evs.forEach((e, i) => {
      const bh = (e.lat / maxL) * h;
      ctx.fillStyle = e.status === 2 ? '#ef4444' : (e.status === 1 ? '#f59e0b' : '#38bdf8');
      ctx.fillRect(10 + (i / evs.length) * w, 10 + h - bh, bw, bh);
    });
    document.getElementById('tb').innerHTML = evs.slice(-15).reverse().map(e =>
      `<tr><td>#${e.seq.toLocaleString()}</td><td>${(e.ts_ns/1e6).toFixed(2)} ms</td><td>${KINDS[e.kind]||e.kind}</td><td>${e.lat.toLocaleString()} ns</td><td>${ST[e.status]}</td></tr>`
    ).join('');
  } catch (e) {}
  setTimeout(tick, 200);
}
tick();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_GET(self):
        if self.path == "/metrics":
            with lock:
                body = latest_json.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):
        if self.path == "/crash":
            proc.stdin.write("c"); proc.stdin.flush()
        elif self.path == "/restart":
            proc.stdin.write("r"); proc.stdin.flush()
        self.send_response(204)
        self.end_headers()


if __name__ == "__main__":
    print("Live Flight Recorder running at http://localhost:8765", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 8765), Handler).serve_forever()
