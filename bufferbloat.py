#!/usr/bin/env python3
"""Bufferbloat measurement tool — measures latency under load."""

import argparse
import base64
import io
import os
import platform
import random
import re
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from statistics import median
from typing import Optional

# Dependency check
_missing = []
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    _missing.append("matplotlib")

if _missing:
    print(f"Missing dependencies: {', '.join(_missing)}")
    print(f"Install with: pip install {' '.join(_missing)} weasyprint")
    sys.exit(1)

# ─── Constants ────────────────────────────────────────────────────────────────

UPLOAD_CHUNK = 256 * 1024   # 256 KB per chunk
PARALLEL_THREADS = 4

# Load test server presets: name -> (download_url, upload_url, extra_headers, description)
SERVERS = {
    "cloudflare": (
        "https://speed.cloudflare.com/__down?bytes=104857600",
        "https://speed.cloudflare.com/__up",
        {"Origin": "https://speed.cloudflare.com", "Referer": "https://speed.cloudflare.com/"},
        "Cloudflare speed.cloudflare.com (anycast, nearest PoP)",
    ),
    "ovh": (
        "http://proof.ovh.net/files/100Mb.dat",
        "https://httpbin.org/post",
        {},
        "OVH proof.ovh.net (France) + httpbin.org upload",
    ),
}

# Preset targets: name -> (ping_host, description)
TARGETS = {
    # Global / anycast
    "google":         ("8.8.8.8",        "Google DNS (anycast, global)"),
    "cloudflare":     ("1.1.1.1",        "Cloudflare DNS (anycast, global)"),
    "opendns":        ("208.67.222.222", "OpenDNS (anycast, global)"),
    # Europe
    "amsterdam":      ("194.109.6.66",   "AMS-IX Amsterdam, Netherlands"),
    "frankfurt":      ("80.81.192.1",    "DE-CIX Frankfurt, Germany"),
    "london":         ("5.57.80.1",      "LINX London, UK"),
    "paris":          ("193.251.128.1",  "France-IX Paris, France"),
    # Turkey
    "istanbul":       ("195.175.39.39",  "Turk Telekom Istanbul, Turkey"),
    "turkey-google":  ("8.8.8.8",        "Google DNS via Turkey (anycast)"),
    "turkey-cf":      ("1.1.1.1",        "Cloudflare via Turkey (anycast)"),
    "ist-ix":         ("193.140.100.1",  "IXTR Istanbul Internet Exchange, Turkey"),
    # Middle East / nearby
    "dubai":          ("185.120.0.1",    "Emirates IX Dubai, UAE"),
    "sofia":          ("217.16.12.1",    "B-IX Sofia, Bulgaria"),
    "bucharest":      ("185.1.47.1",     "INTERLAN Bucharest, Romania"),
}

GRADE_THRESHOLDS = [
    ("A", 5),
    ("B", 30),
    ("C", 60),
    ("D", 200),
    ("F", float("inf")),
]

GRADE_COLORS = {"A": "#2ecc71", "B": "#f1c40f", "C": "#e67e22", "D": "#e74c3c", "F": "#8e44ad"}

IS_MACOS = platform.system() == "Darwin"


# ─── Data structures ──────────────────────────────────────────────────────────

class SpeedTracker:
    """Thread-safe byte counter for measuring throughput."""
    def __init__(self):
        self._lock = threading.Lock()
        self._down = 0
        self._up = 0

    def add_down(self, n: int):
        with self._lock:
            self._down += n

    def add_up(self, n: int):
        with self._lock:
            self._up += n

    def snapshot(self):
        with self._lock:
            return self._down, self._up

    def reset(self):
        with self._lock:
            self._down = 0
            self._up = 0


@dataclass
class PhaseResult:
    name: str
    rtts: list = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    dl_bytes: int = 0
    ul_bytes: int = 0

    def valid_rtts(self):
        return [r for r in self.rtts if r is not None]

    def packet_loss(self) -> float:
        if not self.rtts:
            return 0.0
        lost = sum(1 for r in self.rtts if r is None)
        return lost / len(self.rtts) * 100.0

    def dl_mbps(self) -> Optional[float]:
        d = self.duration()
        return (self.dl_bytes * 8 / 1_000_000 / d) if d > 0 and self.dl_bytes > 0 else None

    def ul_mbps(self) -> Optional[float]:
        d = self.duration()
        return (self.ul_bytes * 8 / 1_000_000 / d) if d > 0 and self.ul_bytes > 0 else None

    def stats(self):
        v = self.valid_rtts()
        if not v:
            return None
        v_sorted = sorted(v)

        def percentile(data, pct):
            if len(data) == 1:
                return data[0]
            idx = (len(data) - 1) * pct / 100.0
            lo, hi = int(idx), min(int(idx) + 1, len(data) - 1)
            return data[lo] + (data[hi] - data[lo]) * (idx - lo)

        return {
            "min": v_sorted[0],
            "median": median(v_sorted),
            "p90": percentile(v_sorted, 90),
            "p95": percentile(v_sorted, 95),
            "max": v_sorted[-1],
            "count": len(v_sorted),
        }

    def bloat(self, baseline: "PhaseResult") -> Optional[float]:
        s = self.stats()
        b = baseline.stats()
        if s is None or b is None:
            return None
        return s["median"] - b["median"]

    def grade(self, baseline: "PhaseResult") -> str:
        bloat_ms = self.bloat(baseline)
        if bloat_ms is None:
            return "?"
        for letter, threshold in GRADE_THRESHOLDS:
            if bloat_ms < threshold:
                return letter
        return "F"

    def duration(self):
        return self.end_time - self.start_time


# ─── Ping measurement ─────────────────────────────────────────────────────────

def ping_once(host: str) -> Optional[float]:
    """Return RTT in ms, or None on failure."""
    if IS_MACOS:
        cmd = ["ping", "-c", "1", "-W", "1000", host]
    else:
        cmd = ["ping", "-c", "1", "-W", "1", host]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=3
        )
        output = result.stdout + result.stderr
        match = re.search(r"time[=<]([\d.]+)\s*ms", output)
        if match:
            return float(match.group(1))
        return None
    except Exception:
        return None


def measure_phase(
    phase: PhaseResult,
    duration: float,
    ping_host: str,
    verbose: bool,
    stop_event: threading.Event,
) -> None:
    """Ping once per second for `duration` seconds, storing results in phase."""
    phase.start_time = time.time()
    deadline = phase.start_time + duration
    while time.time() < deadline and not stop_event.is_set():
        tick = time.time()
        rtt = ping_once(ping_host)
        phase.rtts.append(rtt)
        label = f"{rtt:.1f} ms" if rtt is not None else "timeout"
        print(f"  [{phase.name}] RTT: {label}", flush=True)
        if verbose and rtt is not None:
            pass  # already printed above
        elapsed = time.time() - tick
        sleep_for = max(0.0, 1.0 - elapsed)
        stop_event.wait(sleep_for)
    phase.end_time = time.time()


# ─── Load generation ──────────────────────────────────────────────────────────

def _download_worker(
    stop_event: threading.Event, tracker: SpeedTracker, url: str, extra_headers: dict
) -> None:
    while not stop_event.is_set():
        try:
            req = urllib.request.Request(url, headers={"Cache-Control": "no-cache", **extra_headers})
            with urllib.request.urlopen(req, timeout=30) as resp:
                while not stop_event.is_set():
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    tracker.add_down(len(chunk))
        except Exception:
            if stop_event.is_set():
                return
            time.sleep(0.2)


def _upload_worker(
    stop_event: threading.Event, tracker: SpeedTracker, url: str, extra_headers: dict
) -> None:
    while not stop_event.is_set():
        try:
            data = random.randbytes(UPLOAD_CHUNK)
            req = urllib.request.Request(
                url,
                data=data,
                method="POST",
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(data)),
                    **extra_headers,
                },
            )
            with urllib.request.urlopen(req, timeout=15):
                tracker.add_up(len(data))
        except Exception:
            if stop_event.is_set():
                return
            time.sleep(0.1)


def start_load(
    download: bool,
    upload: bool,
    stop_event: threading.Event,
    tracker: SpeedTracker,
    dl_url: str,
    ul_url: str,
    extra_headers: dict,
) -> list:
    threads = []
    if download:
        for _ in range(PARALLEL_THREADS):
            t = threading.Thread(
                target=_download_worker, args=(stop_event, tracker, dl_url, extra_headers), daemon=True
            )
            t.start()
            threads.append(t)
    if upload:
        for _ in range(PARALLEL_THREADS):
            t = threading.Thread(
                target=_upload_worker, args=(stop_event, tracker, ul_url, extra_headers), daemon=True
            )
            t.start()
            threads.append(t)
    return threads


# ─── Analysis ─────────────────────────────────────────────────────────────────

def overall_grade(phases: list, baseline: PhaseResult) -> str:
    order = "ABCDF?"
    worst = "A"
    for p in phases:
        g = p.grade(baseline)
        if order.index(g) > order.index(worst):
            worst = g
    return worst


def interpretation(phases: list, baseline: PhaseResult) -> str:
    grade = overall_grade(phases, baseline)
    bs = baseline.stats()
    if bs is None:
        return "Baseline measurement failed; results may be unreliable."

    bloat_values = [p.bloat(baseline) for p in phases if p.bloat(baseline) is not None]
    worst_bloat = max(bloat_values) if bloat_values else 0

    if grade == "A":
        return (
            f"Your connection shows excellent bufferbloat control. "
            f"Latency under load increases by at most {worst_bloat:.1f} ms above the "
            f"{bs['median']:.1f} ms baseline, which is imperceptible in practice. "
            "Real-time applications like video calls and gaming will not be impacted "
            "during heavy downloads or uploads."
        )
    elif grade == "B":
        return (
            f"Your connection has mild bufferbloat. Latency under load rises by up to "
            f"{worst_bloat:.1f} ms above the {bs['median']:.1f} ms baseline. "
            "Most users will notice only slight degradation during heavy transfers. "
            "Consider enabling fq_codel or CAKE QoS on your router to improve further."
        )
    elif grade == "C":
        return (
            f"Your connection has moderate bufferbloat ({worst_bloat:.1f} ms additional "
            f"latency on a {bs['median']:.1f} ms baseline). Video calls and gaming will "
            "experience noticeable quality drops during simultaneous large transfers. "
            "Enabling active queue management (AQM) such as fq_codel or CAKE on your "
            "router or modem is strongly recommended."
        )
    elif grade == "D":
        return (
            f"Your connection has significant bufferbloat ({worst_bloat:.1f} ms additional "
            f"latency). Interactive traffic suffers badly during loads. Check whether your "
            "ISP's modem or router has a large buffer you can reduce, and enable AQM."
        )
    else:
        return (
            f"Your connection has severe bufferbloat ({worst_bloat:.1f} ms additional "
            "latency). This will severely impact any real-time application whenever "
            "the link is under load. Investigate your router/modem buffer settings and "
            "consider a router with proper AQM support (OpenWrt with CAKE, pfSense, etc.)."
        )


# ─── Chart generation ─────────────────────────────────────────────────────────

def _fig_to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=110)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def make_timeline_chart(baseline: PhaseResult, phases: list) -> str:
    all_phases = [baseline] + phases
    fig, ax = plt.subplots(figsize=(11, 4))

    colors = ["#3498db", "#e74c3c", "#2ecc71", "#9b59b6", "#f39c12"]
    t_offset = 0.0

    for i, phase in enumerate(all_phases):
        times = [t_offset + j for j in range(len(phase.rtts))]
        rtts_plot = [r if r is not None else float("nan") for r in phase.rtts]
        ax.plot(times, rtts_plot, color=colors[i % len(colors)], linewidth=1.5,
                label=phase.name, marker="o", markersize=3)
        if i > 0:
            ax.axvline(x=t_offset, color="#95a5a6", linestyle="--", linewidth=0.8)
        t_offset += len(phase.rtts)

    ax.set_xlabel("Time (s)", fontsize=10)
    ax.set_ylabel("RTT (ms)", fontsize=10)
    ax.set_title("Latency Over Time", fontsize=12, fontweight="bold")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    b64 = _fig_to_b64(fig)
    plt.close(fig)
    return b64


def make_boxplot_chart(baseline: PhaseResult, phases: list) -> str:
    all_phases = [baseline] + phases
    labels = [p.name for p in all_phases]
    data = [p.valid_rtts() for p in all_phases]
    colors = ["#3498db", "#e74c3c", "#2ecc71", "#9b59b6", "#f39c12"]

    fig, ax = plt.subplots(figsize=(9, 4))
    bp = ax.boxplot(
        [d if d else [0] for d in data],
        tick_labels=labels,
        patch_artist=True,
        medianprops={"color": "black", "linewidth": 2},
    )
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    ax.set_ylabel("RTT (ms)", fontsize=10)
    ax.set_title("RTT Distribution per Phase", fontsize=12, fontweight="bold")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    b64 = _fig_to_b64(fig)
    plt.close(fig)
    return b64


# ─── Report generation ────────────────────────────────────────────────────────

def _grade_badge(grade: str) -> str:
    color = GRADE_COLORS.get(grade, "#7f8c8d")
    return (
        f'<span style="background:{color};color:white;padding:2px 10px;'
        f'border-radius:4px;font-weight:bold;font-size:0.9em">{grade}</span>'
    )


def _grade_badge_large(grade: str) -> str:
    color = GRADE_COLORS.get(grade, "#7f8c8d")
    return (
        f'<div style="display:inline-block;background:{color};color:white;'
        f'padding:16px 40px;border-radius:8px;font-size:3em;font-weight:bold;'
        f'letter-spacing:4px;margin-top:8px">{grade}</div>'
    )


def make_speed_chart(phases: list) -> Optional[str]:
    speed_phases = [p for p in phases if p.dl_mbps() is not None or p.ul_mbps() is not None]
    if not speed_phases:
        return None

    labels = [p.name for p in speed_phases]
    dl_vals = [p.dl_mbps() or 0 for p in speed_phases]
    ul_vals = [p.ul_mbps() or 0 for p in speed_phases]

    x = range(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(8, 3.5))
    bars_dl = ax.bar([i - width / 2 for i in x], dl_vals, width, label="Download", color="#3498db", alpha=0.85)
    bars_ul = ax.bar([i + width / 2 for i in x], ul_vals, width, label="Upload", color="#e74c3c", alpha=0.85)

    for bar in bars_dl:
        if bar.get_height() > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"{bar.get_height():.0f}", ha="center", va="bottom", fontsize=8)
    for bar in bars_ul:
        if bar.get_height() > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"{bar.get_height():.0f}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Mbps", fontsize=10)
    ax.set_title("Throughput per Phase", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    b64 = _fig_to_b64(fig)
    plt.close(fig)
    return b64


def build_html(
    baseline: PhaseResult,
    phases: list,
    timestamp: str,
    total_duration: float,
    ping_label: str = "8.8.8.8",
    server_label: str = "",
) -> str:
    grade = overall_grade(phases, baseline)
    timeline_b64 = make_timeline_chart(baseline, phases)
    boxplot_b64 = make_boxplot_chart(baseline, phases)
    speed_b64 = make_speed_chart(phases)
    interp = interpretation(phases, baseline)

    baseline_loss = baseline.packet_loss()
    warning_html = ""
    if baseline_loss >= 5:
        warning_html = (
            '<div class="warning">'
            f"<strong>&#9888; High baseline packet loss detected ({baseline_loss:.0f}%)</strong><br>"
            "Packet loss was present before any load was applied, which typically indicates "
            "an unstable Wi-Fi connection, a congested local network, or a flaky ISP link. "
            "Bufferbloat results may be less accurate because load-induced latency spikes are "
            "harder to separate from pre-existing instability. "
            "For best results, test over a wired connection or ping your local router "
            "(<code>--ping-host 192.168.1.1</code>) to isolate where the loss originates."
            "</div>"
        )

    speed_chart_html = ""
    if speed_b64:
        speed_chart_html = (
            '<div class="chart-section">'
            "<h3>Throughput per Phase</h3>"
            f'<img src="data:image/png;base64,{speed_b64}" alt="Throughput chart">'
            "</div>"
        )

    rows = ""
    bs = baseline.stats()
    for p in [baseline] + phases:
        s = p.stats()
        loss = p.packet_loss()
        loss_color = "#e74c3c" if loss >= 5 else ("#e67e22" if loss > 0 else "inherit")
        loss_str = f'<span style="color:{loss_color};font-weight:{"bold" if loss>0 else "normal"}">{loss:.0f}%</span>'
        dl_str = f"{p.dl_mbps():.1f}" if p.dl_mbps() is not None else "—"
        ul_str = f"{p.ul_mbps():.1f}" if p.ul_mbps() is not None else "—"
        if s is None:
            rows += f"<tr><td>{p.name}</td>" + "<td>—</td>" * 8 + "</tr>\n"
            continue
        bloat_str = "—"
        grade_str = "—"
        if p is not baseline:
            b = p.bloat(baseline)
            bloat_str = f"{b:+.1f} ms" if b is not None else "—"
            grade_str = _grade_badge(p.grade(baseline))
        rows += (
            f"<tr>"
            f"<td>{p.name}</td>"
            f"<td>{s['min']:.1f}</td>"
            f"<td>{s['median']:.1f}</td>"
            f"<td>{s['p90']:.1f}</td>"
            f"<td>{s['max']:.1f}</td>"
            f"<td>{loss_str}</td>"
            f"<td>{dl_str}</td>"
            f"<td>{ul_str}</td>"
            f"<td>{bloat_str}</td>"
            f"<td>{grade_str}</td>"
            f"</tr>\n"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Bufferbloat Test Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 960px; margin: 0 auto; padding: 32px 24px;
         background: #f8f9fa; color: #2c3e50; }}
  h1 {{ font-size: 2em; margin-bottom: 4px; }}
  .meta {{ color: #7f8c8d; font-size: 0.9em; margin-bottom: 24px; }}
  .overall {{ text-align: center; margin: 32px 0; }}
  .overall h2 {{ margin-bottom: 4px; color: #2c3e50; }}
  table {{ width: 100%; border-collapse: collapse; margin: 16px 0 32px; background: white;
           border-radius: 8px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,.1); }}
  th {{ background: #2c3e50; color: white; padding: 10px 14px; text-align: left; font-size: .85em; }}
  td {{ padding: 9px 14px; border-bottom: 1px solid #ecf0f1; font-size: .9em; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f0f4f8; }}
  .chart-section {{ background: white; border-radius: 8px; padding: 16px 20px;
                    margin-bottom: 24px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }}
  .chart-section h3 {{ margin: 0 0 12px; font-size: 1em; color: #2c3e50; }}
  img {{ max-width: 100%; height: auto; display: block; }}
  .interp {{ background: white; border-left: 4px solid #3498db; border-radius: 0 8px 8px 0;
             padding: 16px 20px; margin: 24px 0; box-shadow: 0 1px 4px rgba(0,0,0,.1);
             font-size: .95em; line-height: 1.6; }}
  .interp h3 {{ margin: 0 0 8px; color: #2c3e50; }}
  .warning {{ background: #fef9e7; border-left: 4px solid #f39c12; border-radius: 0 8px 8px 0;
              padding: 14px 20px; margin: 0 0 24px; box-shadow: 0 1px 4px rgba(0,0,0,.1);
              font-size: .92em; line-height: 1.6; }}
  .warning strong {{ color: #d68910; }}
</style>
</head>
<body>
<h1>Bufferbloat Test Report</h1>
<p class="meta">Generated: {timestamp} &nbsp;|&nbsp; Ping target: {ping_label} &nbsp;|&nbsp; Load server: {server_label} &nbsp;|&nbsp; Total duration: {total_duration:.0f}s</p>

{warning_html}
<div class="overall">
  <h2>Overall Grade</h2>
  {_grade_badge_large(grade)}
</div>

<h2>Results by Phase</h2>
<table>
  <thead>
    <tr>
      <th>Phase</th><th>Min RTT</th><th>Median RTT</th>
      <th>P90 RTT</th><th>Max RTT</th><th>Loss</th>
      <th>↓ Mbps</th><th>↑ Mbps</th><th>Bloat</th><th>Grade</th>
    </tr>
  </thead>
  <tbody>
{rows}  </tbody>
</table>

<div class="chart-section">
  <h3>Latency Over Time</h3>
  <img src="data:image/png;base64,{timeline_b64}" alt="Latency timeline">
</div>

<div class="chart-section">
  <h3>RTT Distribution per Phase</h3>
  <img src="data:image/png;base64,{boxplot_b64}" alt="RTT distributions">
</div>

{speed_chart_html}

<div class="interp">
  <h3>Interpretation</h3>
  <p>{interp}</p>
</div>

</body>
</html>"""
    return html


def save_report(html: str, output_path: str) -> None:
    if output_path.endswith(".pdf"):
        try:
            import weasyprint
            weasyprint.HTML(string=html).write_pdf(output_path)
            print(f"\nReport saved to {output_path}")
            return
        except ImportError:
            fallback = output_path.replace(".pdf", ".html")
            print(
                "\nWarning: weasyprint is not installed. "
                f"Saving as HTML instead: {fallback}"
                "\nInstall with: pip install weasyprint"
            )
            output_path = fallback

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nReport saved to {output_path}")


# ─── Main test runner ─────────────────────────────────────────────────────────

def run_test(
    duration: int,
    ping_host: str,
    no_upload: bool,
    verbose: bool,
    dl_url: str,
    ul_url: str,
    extra_headers: dict,
) -> tuple:
    stop_event = threading.Event()

    phases_to_run = [
        ("Baseline", False, False),
        ("Download", True, False),
    ]
    if not no_upload:
        phases_to_run.append(("Upload", False, True))
        phases_to_run.append(("Bidirectional", True, True))
    phases_to_run.append(("Recovery", False, False))

    recovery_duration = max(5, duration // 2)

    baseline = None
    loaded_phases = []
    all_results = []

    t_start = time.time()

    for i, (name, dl, ul) in enumerate(phases_to_run):
        phase_duration = recovery_duration if name == "Recovery" else duration
        print(f"\n{'─'*50}")
        print(f"Phase {i+1}/{len(phases_to_run)}: {name} ({phase_duration}s)")
        if dl or ul:
            dirs = []
            if dl:
                dirs.append("download")
            if ul:
                dirs.append("upload")
            print(f"  Loading: {', '.join(dirs)}")
        print(f"{'─'*50}")

        phase = PhaseResult(name=name)
        tracker = SpeedTracker()
        stop_load = threading.Event()
        load_threads = start_load(dl, ul, stop_load, tracker, dl_url, ul_url, extra_headers)

        measure_stop = threading.Event()
        measure_phase(phase, phase_duration, ping_host, verbose, measure_stop)

        stop_load.set()
        for t in load_threads:
            t.join(timeout=2)

        phase.dl_bytes, phase.ul_bytes = tracker.snapshot()

        all_results.append(phase)
        if name == "Baseline":
            baseline = phase
            if phase.packet_loss() >= 5:
                print(
                    f"\n  ⚠  WARNING: {phase.packet_loss():.0f}% packet loss at baseline "
                    "(before any load). Results may reflect unstable Wi-Fi or local network "
                    "issues rather than WAN bufferbloat.\n"
                )
        elif name != "Recovery":
            loaded_phases.append(phase)

        s = phase.stats()
        loss = phase.packet_loss()
        if s:
            speed_parts = []
            if phase.dl_mbps() is not None:
                speed_parts.append(f"↓{phase.dl_mbps():.1f} Mbps")
            if phase.ul_mbps() is not None:
                speed_parts.append(f"↑{phase.ul_mbps():.1f} Mbps")
            speed_str = "  " + "  ".join(speed_parts) if speed_parts else ""
            loss_str = f"  loss={loss:.0f}%" if loss > 0 else ""
            print(
                f"  → median={s['median']:.1f}ms  p90={s['p90']:.1f}ms  "
                f"max={s['max']:.1f}ms  samples={s['count']}{loss_str}{speed_str}"
            )
            if baseline and phase is not baseline:
                b = phase.bloat(baseline)
                g = phase.grade(baseline)
                if b is not None:
                    print(f"  → bloat={b:+.1f}ms  grade={g}")

    total_duration = time.time() - t_start
    return baseline, loaded_phases, all_results, total_duration


# ─── Entry point ─────────────────────────────────────────────────────────────

def list_targets() -> None:
    print("\nAvailable --target presets:\n")
    groups = [
        ("Global / anycast", ["google", "cloudflare", "opendns"]),
        ("Europe",           ["amsterdam", "frankfurt", "london", "paris", "sofia", "bucharest"]),
        ("Turkey",           ["istanbul", "ist-ix", "turkey-cf", "turkey-google"]),
        ("Middle East",      ["dubai"]),
    ]
    for group_name, keys in groups:
        print(f"  {group_name}:")
        for k in keys:
            host, desc = TARGETS[k]
            print(f"    {k:<18}  {host:<18}  {desc}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Measure bufferbloat: latency spike under network load."
    )
    parser.add_argument("--output", default="report.html",
                        help="Output file path (.html or .pdf). Default: report.html")
    parser.add_argument("--duration", type=int, default=10,
                        help="Seconds per phase (default: 10)")
    parser.add_argument("--ping-host", default=None,
                        help="Host/IP to ping for latency measurements")
    parser.add_argument("--target", default=None,
                        help=(
                            "Preset target name (e.g. cloudflare, istanbul, amsterdam). "
                            "Use --list-targets to see all options. "
                            "Overridden by --ping-host if both are given."
                        ))
    parser.add_argument("--list-targets", action="store_true",
                        help="List all available --target presets and exit")
    parser.add_argument("--server", default="cloudflare",
                        help=(
                            "Load test server preset: cloudflare (default) or ovh. "
                            "Cloudflare is anycast and hits your nearest PoP."
                        ))
    parser.add_argument("--no-upload", action="store_true",
                        help="Skip upload phases (useful on networks that block outbound POST)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print each RTT value as it is collected")
    args = parser.parse_args()

    if args.list_targets:
        list_targets()
        sys.exit(0)

    # Resolve load server
    server_key = args.server.lower()
    if server_key not in SERVERS:
        print(f"Unknown server '{args.server}'. Available: {', '.join(SERVERS)}")
        sys.exit(1)
    dl_url, ul_url, extra_headers, server_desc = SERVERS[server_key]

    # Resolve ping host: explicit --ping-host wins, then --target, then default
    target_label = None
    if args.ping_host:
        ping_host = args.ping_host
    elif args.target:
        key = args.target.lower()
        if key not in TARGETS:
            print(f"Unknown target '{args.target}'. Use --list-targets to see options.")
            sys.exit(1)
        ping_host, target_label = TARGETS[key]
    else:
        ping_host = "8.8.8.8"

    print("=" * 50)
    print("  Bufferbloat Measurement Tool")
    print("=" * 50)
    if target_label:
        print(f"  Target    : {args.target} ({target_label})")
    print(f"  Ping host : {ping_host}")
    print(f"  Server    : {server_key} — {server_desc}")
    print(f"  Duration  : {args.duration}s per phase")
    print(f"  Output    : {args.output}")
    if args.no_upload:
        print("  Upload    : disabled")
    print("=" * 50)

    baseline, loaded_phases, all_results, total_duration = run_test(
        duration=args.duration,
        ping_host=ping_host,
        no_upload=args.no_upload,
        verbose=args.verbose,
        dl_url=dl_url,
        ul_url=ul_url,
        extra_headers=extra_headers,
    )

    grade = overall_grade(loaded_phases, baseline)
    print(f"\n{'='*50}")
    print(f"  Overall grade: {grade}")
    print(f"{'='*50}\n")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ping_label = f"{args.target} ({target_label})" if target_label else ping_host
    html = build_html(baseline, loaded_phases, timestamp, total_duration, ping_label, server_desc)
    save_report(html, args.output)


if __name__ == "__main__":
    main()
