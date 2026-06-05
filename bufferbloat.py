#!/usr/bin/env python3
"""Bufferbloat measurement tool — measures latency under load."""

# ─── Auto-venv bootstrap ──────────────────────────────────────────────────────
# If not already running inside a virtual environment, create one next to this
# script, install required packages into it, and re-launch automatically.
import sys, os as _os

def _bootstrap():
    if sys.prefix != sys.base_prefix:
        return  # already in a venv

    import subprocess, pathlib
    script = pathlib.Path(__file__).resolve()
    venv_dir = script.parent / ".venv"
    pip = venv_dir / ("Scripts" if sys.platform == "win32" else "bin") / "pip"
    python = venv_dir / ("Scripts" if sys.platform == "win32" else "bin") / "python"

    if not venv_dir.exists():
        print("Bufferbloat requires matplotlib and flask.")
        print(f"A virtual environment will be created at: {venv_dir}")
        try:
            answer = input("Set up automatically? [Y/n]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(0)
        if answer not in ("", "y", "yes"):
            print("\nManual install: pip install matplotlib flask")
            print("Or activate your own venv first, then re-run.")
            sys.exit(0)
        print("Creating virtual environment...", flush=True)
        subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])
        print("Installing dependencies...", flush=True)
        subprocess.check_call([str(pip), "install", "--quiet", "matplotlib", "flask"])
        print("Setup complete.\n", flush=True)

    _os.execv(str(python), [str(python)] + sys.argv)

if __name__ == "__main__":
    _bootstrap()
# ─────────────────────────────────────────────────────────────────────────────

import argparse
import base64
import io
import json
import os
import platform
import queue as _queue
import random
import re
import subprocess
import sys
import threading
import time
import urllib.request
import uuid as _uuid
import webbrowser
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
    # Global / anycast (nearest PoP, good baseline)
    "cloudflare":     ("1.1.1.1",          "Cloudflare DNS (anycast, global)"),
    "google":         ("8.8.8.8",          "Google DNS (anycast, global)"),
    "quad9":          ("9.9.9.9",          "Quad9 DNS (anycast, global)"),
    "opendns":        ("208.67.222.222",   "OpenDNS (anycast, global)"),
    # Western Europe
    "amsterdam":      ("185.102.218.1",   "CDN77 Amsterdam, Netherlands"),
    "frankfurt":      ("185.102.219.93",  "CDN77 Frankfurt, Germany"),
    "london":         ("185.59.221.51",   "CDN77 London, UK"),
    "paris":          ("185.93.2.193",    "CDN77 Paris, France"),
    "madrid":         ("185.93.3.50",     "CDN77 Madrid, Spain"),
    "milan":          ("84.17.59.129",    "CDN77 Milan, Italy"),
    "zurich":         ("89.187.165.1",    "CDN77 Zurich, Switzerland"),
    "lisbon":         ("109.61.94.65",    "CDN77 Lisbon, Portugal"),
    "brussels":       ("207.211.214.65",  "CDN77 Brussels, Belgium"),
    # Northern Europe
    "stockholm":      ("185.76.9.135",    "CDN77 Stockholm, Sweden"),
    "helsinki":       ("65.21.54.60",     "Hetzner Helsinki, Finland"),
    "oslo":           ("95.173.205.1",    "CDN77 Oslo, Norway"),
    "copenhagen":     ("121.127.45.65",   "CDN77 Copenhagen, Denmark"),
    # Eastern / Central Europe
    "warsaw":         ("185.246.208.67",  "CDN77 Warsaw, Poland"),
    "prague":         ("185.152.65.113",  "CDN77 Prague, Czech Republic"),
    "vienna":         ("185.180.12.40",   "CDN77 Vienna, Austria"),
    "budapest":       ("79.127.133.1",    "CDN77 Budapest, Hungary"),
    "sofia":          ("37.19.203.1",     "CDN77 Sofia, Bulgaria"),
    "bucharest":      ("185.102.217.170", "CDN77 Bucharest, Romania"),
    # Turkey
    "istanbul":       ("156.146.52.1",    "CDN77 Istanbul, Turkey"),
    # Middle East
    "dubai":          ("89.222.117.129",  "CDN77 Fujairah/UAE"),
    # North America
    "newyork":        ("185.59.223.8",    "CDN77 New York, USA"),
    "ashburn":        ("37.19.206.20",    "CDN77 Ashburn (DC), USA"),
    # Asia Pacific
    "singapore":      ("89.187.162.1",    "CDN77 Singapore"),
    "tokyo":          ("89.187.160.1",    "CDN77 Tokyo, Japan"),
    "sydney":         ("143.244.63.144",  "CDN77 Sydney, Australia"),
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
IS_WINDOWS = platform.system() == "Windows"


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
    if IS_WINDOWS:
        cmd = ["ping", "-n", "1", "-w", "1000", host]
    elif IS_MACOS:
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
    event_callback=None,
) -> None:
    """Ping once per second for `duration` seconds, storing results in phase."""
    phase.start_time = time.time()
    deadline = phase.start_time + duration
    seq = 0
    while time.time() < deadline and not stop_event.is_set():
        tick = time.time()
        rtt = ping_once(ping_host)
        phase.rtts.append(rtt)
        label = f"{rtt:.1f} ms" if rtt is not None else "timeout"
        if verbose:
            print(f"  [{phase.name}] RTT: {label}", flush=True)
        if event_callback:
            event_callback({"type": "rtt", "phase": phase.name, "rtt": rtt, "seq": seq})
        seq += 1
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
    if not phases:
        return "?"
    worst = "A"
    for p in phases:
        g = p.grade(baseline)
        if order.index(g) > order.index(worst):
            worst = g
    return worst


def remediation_html(grade: str) -> str:
    if grade in ("A", "B"):
        return """
<div class="remediation good">
  <h3>&#x2705; Your connection looks healthy</h3>
  <p>Your router or modem is already managing its queues well. Here's what's keeping it that way:</p>
  <ul>
    <li><strong>Active Queue Management (AQM)</strong> — likely already enabled on your router (fq_codel, CAKE, or similar). Keep it on.</li>
    <li><strong>Keep firmware up to date</strong> — router vendors regularly improve queue management in updates.</li>
    <li>Re-run this test occasionally, especially after ISP modem swaps or router firmware changes.</li>
  </ul>
</div>"""
    elif grade == "C":
        return """
<div class="remediation warn">
  <h3>&#x26A0;&#xFE0F; Moderate bufferbloat — fixable with AQM</h3>
  <p>Your router or modem is not managing its send queue aggressively enough. The most effective fix is enabling <strong>Active Queue Management (AQM)</strong>:</p>
  <ul>
    <li><strong>OpenWrt / DD-WRT routers</strong> — go to <em>Network &rarr; SQM QoS</em> and enable <code>fq_codel</code> or <code>CAKE</code>. Set the bandwidth slightly below your actual line speed (e.g. 95%).</li>
    <li><strong>pfSense / OPNsense</strong> — enable HFSC or CAKE shaper under <em>Firewall &rarr; Traffic Shaper</em>.</li>
    <li><strong>Consumer routers (ASUS, TP-Link, Netgear)</strong> — look for "Adaptive QoS", "QoS", or "Traffic Control" in the web UI. Enable it and set your line speed.</li>
    <li><strong>ISP-supplied modem/router combos</strong> — these often have oversized buffers with no AQM option. Consider putting it in bridge mode and adding your own router with AQM.</li>
  </ul>
  <p><strong>Background:</strong> <a href="https://www.bufferbloat.net/projects/bloat/wiki/What_can_I_do_about_Bufferbloat/">bufferbloat.net — What can I do about Bufferbloat?</a></p>
</div>"""
    else:  # D or F
        return """
<div class="remediation bad">
  <h3>&#x1F6A8; Significant bufferbloat — action recommended</h3>
  <p>Your connection has a large queue somewhere between your computer and the internet. This causes bad latency spikes during any load. Fix options, from easiest to most effective:</p>
  <ol>
    <li>
      <strong>Enable AQM on your router</strong> — the single most impactful change.
      <ul>
        <li><strong>OpenWrt</strong>: install <code>luci-app-sqm</code>, enable CAKE on your WAN interface, set bandwidth to ~95% of your line speed.</li>
        <li><strong>OPNsense/pfSense</strong>: enable CAKE or fq_codel traffic shaper.</li>
        <li><strong>Consumer router</strong>: find "QoS" or "Traffic Control" in the admin panel and enable it with your actual line speeds.</li>
      </ul>
    </li>
    <li>
      <strong>Check your ISP modem</strong> — many ISP-supplied devices have large fixed buffers and no AQM. Put the modem in bridge/passthrough mode and use your own router for everything.</li>
    <li>
      <strong>Replace the router</strong> — if your router has no AQM option, consider a device that supports OpenWrt. Almost any modern router running OpenWrt with CAKE will dramatically improve results.</li>
    <li>
      <strong>Contact your ISP</strong> — some ISPs have bufferbloat issues in their own infrastructure (DSLAM, CMTS, etc.). If your baseline RTT is high and you've already enabled AQM locally, ask your ISP.</li>
  </ol>
  <p>
    <strong>Learn more:</strong><br>
    <a href="https://www.bufferbloat.net/projects/bloat/wiki/What_can_I_do_about_Bufferbloat/">bufferbloat.net — What can I do about Bufferbloat?</a><br>
    <a href="https://openwrt.org/docs/guide-user/network/traffic_shaping/sqm">OpenWrt SQM / CAKE setup guide</a><br>
    <a href="https://www.waveform.com/tools/bufferbloat">Waveform bufferbloat test (cross-check)</a>
  </p>
</div>"""


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
    remediation = remediation_html(grade)

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

    import html as _html
    ping_label_escaped = _html.escape(ping_label)
    server_label_escaped = _html.escape(server_label)

    worst_bloat_ms: int | None = None
    max_dl_mbps: float | None = None
    max_ul_mbps: float | None = None
    for p in phases:
        b = p.bloat(baseline)
        if b is not None:
            worst_bloat_ms = int(round(b)) if worst_bloat_ms is None else max(worst_bloat_ms, int(round(b)))
        if p.dl_mbps() is not None:
            max_dl_mbps = p.dl_mbps() if max_dl_mbps is None else max(max_dl_mbps, p.dl_mbps())
        if p.ul_mbps() is not None:
            max_ul_mbps = p.ul_mbps() if max_ul_mbps is None else max(max_ul_mbps, p.ul_mbps())
    bloat_js = "null" if worst_bloat_ms is None else str(worst_bloat_ms)
    dl_js = "null" if max_dl_mbps is None else f"{max_dl_mbps:.2f}"
    ul_js = "null" if max_ul_mbps is None else f"{max_ul_mbps:.2f}"

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
  .remediation {{ border-radius: 8px; padding: 20px 24px; margin: 24px 0;
                  box-shadow: 0 1px 4px rgba(0,0,0,.1); font-size: .93em; line-height: 1.7; }}
  .remediation h3 {{ margin: 0 0 10px; font-size: 1.05em; }}
  .remediation ul, .remediation ol {{ margin: 8px 0 8px 20px; padding: 0; }}
  .remediation li {{ margin-bottom: 6px; }}
  .remediation a {{ color: #2980b9; }}
  .remediation code {{ background: #f4f4f4; padding: 1px 5px; border-radius: 3px; font-size: .9em; }}
  .remediation.good {{ background: #eafaf1; border-left: 4px solid #2ecc71; }}
  .remediation.good h3 {{ color: #1e8449; }}
  .remediation.warn {{ background: #fef9e7; border-left: 4px solid #f39c12; }}
  .remediation.warn h3 {{ color: #b7770d; }}
  .remediation.bad {{ background: #fdf2f2; border-left: 4px solid #e74c3c; }}
  .remediation.bad h3 {{ color: #c0392b; }}
  .submit-card {{ background: white; border-radius: 8px; padding: 20px 24px; margin: 24px 0;
                  box-shadow: 0 1px 4px rgba(0,0,0,.1); font-size: .93em; }}
  .submit-card h3 {{ margin: 0 0 6px; color: #2c3e50; }}
  .submit-card p {{ color: #7f8c8d; margin: 0 0 14px; font-size: .9em; }}
  .submit-card .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }}
  .submit-card input, .submit-card select {{ width: 100%; padding: 8px 10px; border: 1px solid #d5dbdf;
                                              border-radius: 4px; font-size: .9em; font-family: inherit; }}
  .submit-card button {{ background: #3498db; color: white; border: 0; padding: 10px 18px;
                          border-radius: 4px; font-size: .95em; cursor: pointer; font-weight: 600; }}
  .submit-card button:hover {{ background: #2980b9; }}
  .submit-card button:disabled {{ background: #95a5a6; cursor: not-allowed; }}
  .submit-status {{ margin-top: 10px; font-size: .9em; }}
  .submit-status.ok {{ color: #1e8449; }}
  .submit-status.err {{ color: #c0392b; }}
  .submit-card .leaderboard-link {{ display: inline-block; margin-top: 12px; color: #2980b9;
                                     text-decoration: none; font-size: .9em; }}
  .submit-card .leaderboard-link:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<h1>Bufferbloat Test Report</h1>
<p class="meta">Generated: {timestamp} &nbsp;|&nbsp; Ping target: {ping_label_escaped} &nbsp;|&nbsp; Load server: {server_label_escaped} &nbsp;|&nbsp; Total duration: {total_duration:.0f}s</p>

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

{remediation}

<div class="submit-card">
  <h3>📊 Submit your result to the leaderboard</h3>
  <p>Help build a global picture of bufferbloat. All fields optional except country.</p>
  <form id="bbSubmitForm" onsubmit="return bbSubmit(event)">
    <div class="row">
      <input type="text" id="bbCountry" placeholder="Country (e.g. Netherlands)" required maxlength="100">
      <input type="text" id="bbIsp" placeholder="ISP (e.g. KPN)" maxlength="200">
    </div>
    <div class="row">
      <input type="text" id="bbRouter" placeholder="Router / firewall (e.g. OPNsense)" maxlength="200">
      <select id="bbConn">
        <option value="">Connection type…</option>
        <option value="Fiber">Fiber</option>
        <option value="Cable">Cable</option>
        <option value="DSL">DSL</option>
        <option value="5G">5G / mobile</option>
        <option value="Satellite">Satellite</option>
        <option value="Starlink">Starlink</option>
        <option value="Other">Other</option>
      </select>
    </div>
    <button type="submit" id="bbSubmitBtn">Submit result</button>
    <a href="https://bufferbloat.kroesneger.nl" target="_blank" class="leaderboard-link">View leaderboard →</a>
    <div id="bbStatus" class="submit-status"></div>
  </form>
</div>
<script>
  const BB_RESULT = {{
    grade: "{grade}",
    bloat_ms: {bloat_js},
    download_mbps: {dl_js},
    upload_mbps: {ul_js}
  }};
  async function bbSubmit(e) {{
    e.preventDefault();
    const btn = document.getElementById('bbSubmitBtn');
    const status = document.getElementById('bbStatus');
    btn.disabled = true; status.className = 'submit-status'; status.textContent = 'Submitting…';
    const payload = Object.assign({{}}, BB_RESULT, {{
      country: document.getElementById('bbCountry').value.trim(),
      isp: document.getElementById('bbIsp').value.trim(),
      router: document.getElementById('bbRouter').value.trim(),
      connection_type: document.getElementById('bbConn').value
    }});
    try {{
      const r = await fetch('https://bufferbloat.kroesneger.nl/submit', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload)
      }});
      if (r.ok) {{
        status.className = 'submit-status ok';
        status.innerHTML = '✓ Submitted. <a href="https://bufferbloat.kroesneger.nl" target="_blank">See leaderboard</a>';
        document.getElementById('bbSubmitForm').querySelectorAll('input,select,button').forEach(el => el.disabled = true);
      }} else {{
        const j = await r.json().catch(() => ({{}}));
        status.className = 'submit-status err';
        status.textContent = 'Error: ' + (j.error || r.statusText);
        btn.disabled = false;
      }}
    }} catch (err) {{
      status.className = 'submit-status err';
      status.textContent = 'Network error: ' + err.message;
      btn.disabled = false;
    }}
    return false;
  }}
</script>

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
    event_callback=None,
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

        if event_callback:
            event_callback({"type": "phase_start", "phase": name, "index": i,
                            "total": len(phases_to_run), "duration": phase_duration,
                            "load": {"dl": dl, "ul": ul}})

        phase = PhaseResult(name=name)
        tracker = SpeedTracker()
        stop_load = threading.Event()
        load_threads = start_load(dl, ul, stop_load, tracker, dl_url, ul_url, extra_headers)

        measure_stop = threading.Event()
        measure_phase(phase, phase_duration, ping_host, verbose, measure_stop, event_callback)

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

        if event_callback and name != "Recovery":
            s2 = phase.stats()
            event_callback({"type": "phase_complete", "phase": name,
                            "stats": s2,
                            "bloat": phase.bloat(baseline) if baseline and phase is not baseline else None,
                            "grade": phase.grade(baseline) if baseline and phase is not baseline else None,
                            "loss": phase.packet_loss(),
                            "dl_mbps": phase.dl_mbps(), "ul_mbps": phase.ul_mbps()})

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
        ("Global / anycast",      ["cloudflare", "google", "quad9", "opendns"]),
        ("Western Europe",        ["amsterdam", "frankfurt", "london", "paris", "madrid", "milan", "zurich", "lisbon", "brussels"]),
        ("Northern Europe",       ["stockholm", "helsinki", "oslo", "copenhagen"]),
        ("Eastern/Central Europe",["warsaw", "prague", "vienna", "budapest", "sofia", "bucharest"]),
        ("Turkey",                ["istanbul"]),
        ("Middle East",           ["dubai"]),
        ("North America",         ["newyork", "ashburn"]),
        ("Asia Pacific",          ["singapore", "tokyo", "sydney"]),
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
    parser.add_argument("--gui", action="store_true",
                        help="Launch browser-based GUI (requires: pip install flask)")
    parser.add_argument("--port", type=int, default=5757,
                        help="Port for --gui web server (default: 5757)")
    args = parser.parse_args()

    if args.gui:
        try:
            import flask  # noqa: F401
        except ImportError:
            print("Flask is required for --gui mode.\nInstall with: pip install flask")
            sys.exit(1)
        app = create_app()
        url = f"http://127.0.0.1:{args.port}"
        print(f"\nBufferbloat GUI running at {url}")
        print("Opening browser... (press Ctrl+C here or click Quit in the browser to stop)\n")
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
        app.run(host="127.0.0.1", port=args.port, debug=False, threaded=True, use_reloader=False)
        sys.exit(0)

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

    print(f"  Checking reachability of {ping_host}...", end=" ", flush=True)
    probe = ping_once(ping_host)
    if probe is None:
        print("UNREACHABLE")
        print(f"\n  ⚠  WARNING: {ping_host} did not respond to ping.")
        print("  Results will show timeouts instead of real latency.")
        print("  Consider choosing a different target with --target.\n")
    else:
        print(f"{probe:.0f} ms")

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


# ─── GUI (Flask web interface) ────────────────────────────────────────────────

@dataclass
class TestSession:
    id: str = ""
    running: bool = False
    event_queue: object = field(default_factory=_queue.Queue)
    report_html: str = ""

_session = TestSession()
_session_lock = threading.Lock()

GUI_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bufferbloat Test</title>
<style>
:root {
  --navy: #2c3e50;
  --navy-light: #34495e;
  --bg: #f0f2f5;
  --card: #ffffff;
  --text: #2c3e50;
  --muted: #7f8c8d;
  --border: #dde1e7;
  --grade-a: #2ecc71;
  --grade-b: #f1c40f;
  --grade-c: #e67e22;
  --grade-d: #e74c3c;
  --grade-f: #8e44ad;
  --radius: 12px;
  --shadow: 0 2px 12px rgba(0,0,0,0.08);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: var(--bg); color: var(--text); min-height: 100vh; }

/* Header */
.header { background: var(--navy); color: white; padding: 18px 32px;
          display: flex; align-items: center; gap: 12px; }
.header h1 { font-size: 1.3em; font-weight: 600; letter-spacing: 0.5px; }
.header .subtitle { font-size: 0.82em; opacity: 0.65; margin-top: 2px; }

/* Main layout */
.main { max-width: 780px; margin: 0 auto; padding: 32px 20px; }

/* Cards */
.card { background: var(--card); border-radius: var(--radius);
        box-shadow: var(--shadow); padding: 28px; margin-bottom: 20px; }

/* Form */
.form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
.form-group { display: flex; flex-direction: column; gap: 7px; }
.form-group.full { grid-column: 1 / -1; }
label { font-size: 0.82em; font-weight: 600; color: var(--muted);
        text-transform: uppercase; letter-spacing: 0.5px; }
select, input[type=range] { width: 100%; }
select { padding: 10px 12px; border: 1.5px solid var(--border);
         border-radius: 8px; font-size: 0.95em; background: white;
         color: var(--text); appearance: none;
         background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%237f8c8d' d='M6 8L1 3h10z'/%3E%3C/svg%3E");
         background-repeat: no-repeat; background-position: right 12px center;
         padding-right: 32px; cursor: pointer; }
select:focus { outline: none; border-color: var(--navy); }

input[type=range] { -webkit-appearance: none; height: 6px;
                    border-radius: 3px; background: var(--border); cursor: pointer; }
input[type=range]::-webkit-slider-thumb { -webkit-appearance: none;
  width: 18px; height: 18px; border-radius: 50%; background: var(--navy); cursor: pointer; }
.range-row { display: flex; align-items: center; gap: 12px; }
.range-row input { flex: 1; }
.range-val { font-size: 0.95em; font-weight: 600; color: var(--navy);
             min-width: 36px; text-align: right; }

/* Toggle switch */
.toggle-row { display: flex; align-items: center; gap: 12px; padding: 4px 0; }
.toggle { position: relative; width: 44px; height: 24px; flex-shrink: 0; }
.toggle input { opacity: 0; width: 0; height: 0; }
.slider-sw { position: absolute; inset: 0; background: var(--border);
             border-radius: 24px; cursor: pointer; transition: .25s; }
.slider-sw:before { content: ""; position: absolute; width: 18px; height: 18px;
                    left: 3px; bottom: 3px; background: white; border-radius: 50%;
                    transition: .25s; }
input:checked + .slider-sw { background: var(--navy); }
input:checked + .slider-sw:before { transform: translateX(20px); }
.toggle-label { font-size: 0.92em; color: var(--text); }

/* Radio group */
.radio-group { display: flex; gap: 12px; }
.radio-option { display: flex; align-items: center; gap: 7px;
                padding: 9px 16px; border: 1.5px solid var(--border);
                border-radius: 8px; cursor: pointer; flex: 1;
                transition: border-color .15s, background .15s; }
.radio-option:has(input:checked) { border-color: var(--navy); background: #f0f4f8; }
.radio-option input { accent-color: var(--navy); }
.radio-option span { font-size: 0.9em; font-weight: 500; }

/* Start button */
.btn-start { width: 100%; padding: 16px; background: var(--navy); color: white;
             border: none; border-radius: 10px; font-size: 1.05em; font-weight: 600;
             cursor: pointer; letter-spacing: 0.5px; transition: background .15s;
             margin-top: 4px; }
.btn-start:hover { background: var(--navy-light); }
.btn-start:disabled { opacity: 0.5; cursor: not-allowed; }

/* Phase indicator */
.phase-indicator { display: none; }
.body-running .phase-indicator { display: block; }
.phase-name { font-size: 1.1em; font-weight: 600; margin-bottom: 10px; }
.phase-meta { font-size: 0.85em; color: var(--muted); margin-bottom: 14px; }
.progress-bar { height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; }
.progress-fill { height: 100%; background: var(--navy); border-radius: 3px;
                 transition: width 0.9s linear; width: 0%; }

/* Canvas chart */
.chart-wrap { display: none; margin-top: 20px; }
.body-running .chart-wrap { display: block; }
canvas { width: 100%; border-radius: 8px; background: #fafbfc;
         border: 1px solid var(--border); display: block; }

/* Phase cards */
.phase-cards { display: flex; flex-direction: column; gap: 10px; margin-top: 16px; }
.phase-card { display: flex; align-items: center; justify-content: space-between;
              padding: 12px 16px; background: var(--bg); border-radius: 8px;
              border: 1px solid var(--border); animation: fadeIn .3s ease; }
.phase-card .pc-name { font-weight: 600; font-size: 0.9em; }
.phase-card .pc-stats { font-size: 0.82em; color: var(--muted); margin-top: 2px; }
.phase-card .pc-grade { font-size: 1.3em; font-weight: 700; margin-left: 16px; flex-shrink: 0; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; } }

/* Done section */
.done-section { display: none; text-align: center; }
.body-done .done-section { display: block; }
.body-done .config-card { display: none; }
.grade-circle { width: 120px; height: 120px; border-radius: 50%;
                display: flex; align-items: center; justify-content: center;
                font-size: 3em; font-weight: 700; color: white;
                margin: 0 auto 16px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }
.done-interp { font-size: 0.95em; line-height: 1.65; color: var(--text);
               max-width: 560px; margin: 0 auto 24px; }
.done-buttons { display: flex; gap: 12px; justify-content: center; flex-wrap: wrap; }
.btn { padding: 11px 24px; border-radius: 8px; font-size: 0.92em; font-weight: 600;
       cursor: pointer; border: none; transition: opacity .15s; }
.btn:hover { opacity: 0.85; }
.btn-primary { background: var(--navy); color: white; }
.btn-secondary { background: var(--border); color: var(--text); }
.btn-danger { background: #e74c3c; color: white; }

/* Warning banner */
.warning-banner { display: none; background: #fef9e7;
                  border-left: 4px solid #f39c12; border-radius: 0 8px 8px 0;
                  padding: 14px 18px; margin-bottom: 16px; font-size: 0.88em;
                  line-height: 1.6; }
.warning-banner.visible { display: block; }
.warning-banner strong { color: #d68910; }


/* Error */
.error-msg { display: none; background: #fdecea; border-left: 4px solid #e74c3c;
             border-radius: 0 8px 8px 0; padding: 14px 18px; color: #c0392b;
             font-size: 0.9em; margin-bottom: 16px; }
.error-msg.visible { display: block; }

.field-hint { font-size: 0.8em; color: var(--muted); margin-top: 4px; min-height: 1.2em; }
details summary { font-size: 0.82em; font-weight: 600; color: var(--muted);
                  text-transform: uppercase; letter-spacing: 0.5px; cursor: pointer;
                  user-select: none; padding: 4px 0; }
details summary:hover { color: var(--navy); }
.adv-inner { margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--border); }

@media (max-width: 560px) {
  .form-grid { grid-template-columns: 1fr; }
  .header { padding: 14px 18px; }
  .main { padding: 20px 14px; }
  .card { padding: 20px; }
}
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Bufferbloat Test</h1>
    <div class="subtitle">Measure latency under load &mdash; detect bufferbloat</div>
  </div>
</div>

<div class="main">
  <div id="errorMsg" class="error-msg"></div>
  <div id="warningBanner" class="warning-banner"></div>

  <!-- Config card (idle) -->
  <div class="card config-card">
    <div class="form-grid">

      <div class="form-group full">
        <label>Test Target</label>
        <select id="targetSelect" onchange="updatePingHint()"></select>
        <div class="field-hint" id="pingHint"></div>
      </div>

      <div class="form-group">
        <label>Duration per phase</label>
        <div class="range-row">
          <input type="range" id="durationSlider" min="5" max="60" value="10"
                 oninput="document.getElementById('durationVal').textContent=this.value+'s'">
          <span class="range-val" id="durationVal">10s</span>
        </div>
      </div>

      <div class="form-group">
        <label>Upload test</label>
        <div class="toggle-row">
          <label class="toggle">
            <input type="checkbox" id="uploadToggle" checked>
            <span class="slider-sw"></span>
          </label>
          <span class="toggle-label">Include upload &amp; bidirectional</span>
        </div>
      </div>

      <div class="form-group full adv-group">
        <details>
          <summary>Advanced options</summary>
          <div class="adv-inner">
            <div class="form-group">
              <label>Load server</label>
              <div class="radio-group">
                <label class="radio-option">
                  <input type="radio" name="server" value="cloudflare" checked>
                  <span>Cloudflare (anycast)</span>
                </label>
                <label class="radio-option">
                  <input type="radio" name="server" value="ovh">
                  <span>OVH (France)</span>
                </label>
              </div>
            </div>
            <div class="form-group" style="margin-top:14px">
              <label>Custom ping host <span style="font-weight:400;text-transform:none">(overrides target)</span></label>
              <input type="text" id="customPingHost" placeholder="e.g. 192.168.1.1 or 1.1.1.1"
                     style="padding:10px 12px;border:1.5px solid var(--border);border-radius:8px;font-size:.95em;width:100%">
            </div>
          </div>
        </details>
      </div>

      <div class="form-group full">
        <button class="btn-start" id="startBtn" onclick="startTest()">Start Test</button>
      </div>
    </div>
  </div>

  <!-- Live phase indicator (running) -->
  <div class="card phase-indicator" id="phaseIndicator">
    <div class="phase-name" id="phaseName">Initializing...</div>
    <div class="phase-meta" id="phaseMeta"></div>
    <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
    <div class="phase-cards" id="phaseCards"></div>
  </div>

  <!-- Live RTT chart (running) -->
  <div class="card chart-wrap">
    <canvas id="rttCanvas" height="200"></canvas>
  </div>

  <!-- Done section -->
  <div class="card done-section" id="doneSection">
    <div class="grade-circle" id="gradeCircle">?</div>
    <p class="done-interp" id="doneInterp"></p>
    <div class="done-buttons">
      <button class="btn btn-primary" onclick="openReport()">Open Full Report</button>
      <button class="btn btn-primary" onclick="downloadReport()">Download Report</button>
      <button class="btn btn-secondary" onclick="runAgain()">Run Again</button>
      <button class="btn btn-danger" onclick="quitServer()">Quit</button>
    </div>
  </div>
</div>

<script id="targets-data" type="application/json">{{ targets_json|safe }}</script>
<script>
// ── Target population ──────────────────────────────────────────────────────
const TARGETS = JSON.parse(document.getElementById('targets-data').textContent);
function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
const GROUPS = [
  ["Global / anycast",       ["cloudflare","google","quad9","opendns"]],
  ["Western Europe",         ["amsterdam","frankfurt","london","paris","madrid","milan","zurich","lisbon","brussels"]],
  ["Northern Europe",        ["stockholm","helsinki","oslo","copenhagen"]],
  ["Eastern/Central Europe", ["warsaw","prague","vienna","budapest","sofia","bucharest"]],
  ["Turkey",                 ["istanbul"]],
  ["Middle East",            ["dubai"]],
  ["North America",          ["newyork","ashburn"]],
  ["Asia Pacific",           ["singapore","tokyo","sydney"]],
];
(function populateTargets() {
  const sel = document.getElementById('targetSelect');
  GROUPS.forEach(([groupName, keys]) => {
    const og = document.createElement('optgroup');
    og.label = groupName;
    keys.forEach(k => {
      if (!TARGETS[k]) return;
      const [ip, desc] = TARGETS[k];
      const opt = document.createElement('option');
      opt.value = k;
      opt.textContent = k + ' — ' + desc;
      if (k === 'cloudflare') opt.selected = true;
      og.appendChild(opt);
    });
    sel.appendChild(og);
  });
  updatePingHint();
})();

function updatePingHint() {
  const k = document.getElementById('targetSelect').value;
  const hint = document.getElementById('pingHint');
  if (TARGETS[k]) {
    const [ip, desc] = TARGETS[k];
    hint.textContent = 'Pinging ' + ip + ' — ' + desc;
  } else {
    hint.textContent = '';
  }
}

// ── Chart ──────────────────────────────────────────────────────────────────
const canvas = document.getElementById('rttCanvas');
const ctx = canvas.getContext('2d');
const GRADE_COLORS = {A:'#2ecc71',B:'#f1c40f',C:'#e67e22',D:'#e74c3c',F:'#8e44ad'};
const PHASE_COLORS = ['#3498db','#e74c3c','#2ecc71','#9b59b6','#f39c12'];

let chartData = []; // [{rtt, phaseIdx, phaseName}]
let phaseNames = [];
let phaseStarts = []; // chartData index where each phase starts

function resizeCanvas() {
  canvas.width = canvas.offsetWidth * window.devicePixelRatio;
  canvas.height = 200 * window.devicePixelRatio;
  drawChart();
}

function drawChart() {
  const w = canvas.width, h = canvas.height;
  const dpr = window.devicePixelRatio || 1;
  ctx.clearRect(0, 0, w, h);

  if (chartData.length === 0) return;

  const validRtts = chartData.filter(d => d.rtt !== null).map(d => d.rtt);
  const maxRtt = validRtts.length ? Math.max(...validRtts) * 1.15 : 200;
  const pad = {top: 20*dpr, right: 20*dpr, bottom: 30*dpr, left: 50*dpr};
  const pw = w - pad.left - pad.right;
  const ph = h - pad.top - pad.bottom;

  // Grid lines
  ctx.strokeStyle = '#e8eaed'; ctx.lineWidth = dpr;
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (ph * i / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke();
    ctx.fillStyle = '#95a5a6'; ctx.font = (10*dpr)+'px sans-serif'; ctx.textAlign = 'right';
    ctx.fillText(Math.round(maxRtt * (1 - i/4)) + 'ms', pad.left - 6*dpr, y + 4*dpr);
  }

  const total = chartData.length;
  const xOf = i => pad.left + (i / Math.max(total - 1, 1)) * pw;
  const yOf = rtt => rtt === null ? pad.top : pad.top + ph * (1 - Math.min(rtt, maxRtt) / maxRtt);

  // Phase boundary lines
  phaseStarts.forEach((si, pi) => {
    if (pi === 0) return;
    const x = xOf(si);
    ctx.strokeStyle = '#bdc3c7'; ctx.lineWidth = dpr;
    ctx.setLineDash([4*dpr, 4*dpr]);
    ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, h - pad.bottom); ctx.stroke();
    ctx.setLineDash([]);
    // Phase label
    if (phaseNames[pi]) {
      ctx.fillStyle = '#7f8c8d'; ctx.font = (9*dpr)+'px sans-serif'; ctx.textAlign = 'left';
      ctx.fillText(phaseNames[pi], x + 3*dpr, pad.top - 4*dpr);
    }
  });

  // Lines per phase
  let prevX = null, prevY = null, prevPhase = null;
  chartData.forEach((d, i) => {
    const x = xOf(i);
    const y = yOf(d.rtt);
    const color = PHASE_COLORS[d.phaseIdx % PHASE_COLORS.length];
    if (d.rtt === null) {
      // Timeout: red X
      ctx.strokeStyle = '#e74c3c'; ctx.lineWidth = 1.5*dpr;
      const s = 5*dpr;
      const ty = pad.top + ph * 0.1;
      ctx.beginPath(); ctx.moveTo(x-s, ty-s); ctx.lineTo(x+s, ty+s); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(x+s, ty-s); ctx.lineTo(x-s, ty+s); ctx.stroke();
      prevX = null; prevY = null;
      return;
    }
    if (prevX !== null && prevPhase === d.phaseIdx) {
      ctx.strokeStyle = color; ctx.lineWidth = 2*dpr; ctx.setLineDash([]);
      ctx.beginPath(); ctx.moveTo(prevX, prevY); ctx.lineTo(x, y); ctx.stroke();
    }
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(x, y, 3.5*dpr, 0, Math.PI*2); ctx.fill();
    prevX = x; prevY = y; prevPhase = d.phaseIdx;
  });
}
window.addEventListener('resize', resizeCanvas);

// ── Test state ─────────────────────────────────────────────────────────────
let currentPhaseIdx = -1;
let progressTimer = null;
let phaseStartTime = 0;
let phaseDuration = 0;
let es = null;

function startTest() {
  const target = document.getElementById('targetSelect').value;
  const server = document.querySelector('input[name=server]:checked').value;
  const duration = parseInt(document.getElementById('durationSlider').value);
  const noUpload = !document.getElementById('uploadToggle').checked;
  const customPing = document.getElementById('customPingHost').value.trim();

  const pingHost = customPing || (TARGETS[target] ? TARGETS[target][0] : null);

  document.getElementById('startBtn').disabled = true;
  document.getElementById('startBtn').textContent = 'Checking target…';
  document.getElementById('errorMsg').classList.remove('visible');
  document.getElementById('warningBanner').classList.remove('visible');

  const probe = pingHost
    ? fetch('/probe', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({host: pingHost})}).then(r => r.json())
    : Promise.resolve({reachable: true});

  probe.then(result => {
    if (!result.reachable) {
      return new Promise((resolve, reject) => {
        const ok = confirm(
          `⚠ ${pingHost} did not respond to ping.\n\nThis target may not be reachable from your network — the test will likely show timeouts instead of real latency.\n\nRun the test anyway?`
        );
        ok ? resolve() : reject(new Error('cancelled'));
      });
    }
  }).then(() => {
    document.getElementById('startBtn').textContent = 'Start Test';
    document.body.className = 'body-running';
    chartData = []; phaseNames = []; phaseStarts = [];
    currentPhaseIdx = -1;
    document.getElementById('phaseCards').innerHTML = '';
    resizeCanvas();

    return fetch('/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({target, server, duration, no_upload: noUpload, custom_ping: customPing})
    }).then(r => {
      if (!r.ok) return r.json().then(d => { throw new Error(d.error || 'Start failed'); });
      return r.json();
    });
  }).then(() => {
    es = new EventSource('/stream');
    es.onmessage = handleEvent;
    es.onerror = () => showError('Connection to test server lost.');
  }).catch(err => {
    document.getElementById('startBtn').textContent = 'Start Test';
    document.getElementById('startBtn').disabled = false;
    document.body.className = '';
    if (err.message !== 'cancelled') showError(err.message);
  });
}

function handleEvent(e) {
  const msg = JSON.parse(e.data);
  if (msg.type === 'phase_start') {
    currentPhaseIdx++;
    phaseNames.push(msg.phase);
    phaseStarts.push(chartData.length);
    phaseDuration = msg.duration;
    phaseStartTime = Date.now();
    document.getElementById('phaseName').textContent =
      'Phase ' + (msg.index+1) + '/' + msg.total + ': ' + msg.phase;
    const dirs = [];
    if (msg.load.dl) dirs.push('download');
    if (msg.load.ul) dirs.push('upload');
    document.getElementById('phaseMeta').textContent =
      dirs.length ? 'Load: ' + dirs.join(' + ') : 'No load (measuring baseline)';
    clearInterval(progressTimer);
    progressTimer = setInterval(() => {
      const pct = Math.min(100, (Date.now() - phaseStartTime) / (phaseDuration * 1000) * 100);
      document.getElementById('progressFill').style.width = pct + '%';
      if (pct >= 100) clearInterval(progressTimer);
    }, 200);
  } else if (msg.type === 'rtt') {
    chartData.push({rtt: msg.rtt, phaseIdx: currentPhaseIdx, phaseName: msg.phase});
    drawChart();
  } else if (msg.type === 'phase_complete' && msg.phase !== 'Recovery') {
    const card = document.createElement('div');
    card.className = 'phase-card';
    const grade = msg.grade || '—';
    const gradeColor = GRADE_COLORS[grade] || '#7f8c8d';
    const median = msg.stats ? msg.stats.median.toFixed(1) + 'ms' : '—';
    const bloat = msg.bloat != null ? (msg.bloat >= 0 ? '+' : '') + msg.bloat.toFixed(1) + 'ms' : '—';
    const loss = msg.loss > 0 ? ' · loss ' + msg.loss.toFixed(0) + '%' : '';
    const dl = msg.dl_mbps ? ' · ↓' + msg.dl_mbps.toFixed(1) + ' Mbps' : '';
    const ul = msg.ul_mbps ? ' · ↑' + msg.ul_mbps.toFixed(1) + ' Mbps' : '';
    card.innerHTML = `
      <div>
        <div class="pc-name">${escapeHtml(msg.phase)}</div>
        <div class="pc-stats">median ${median} · bloat ${bloat}${loss}${dl}${ul}</div>
      </div>
      <div class="pc-grade" style="color:${gradeColor}">${escapeHtml(grade)}</div>`;
    document.getElementById('phaseCards').appendChild(card);
  } else if (msg.type === 'warning') {
    const b = document.getElementById('warningBanner');
    b.innerHTML = '<strong>&#9888; ' + escapeHtml(msg.title) + '</strong><br>' + escapeHtml(msg.message);
    b.classList.add('visible');
  } else if (msg.type === 'done') {
    clearInterval(progressTimer);
    document.getElementById('progressFill').style.width = '100%';
    es.close();
    showDone(msg);
  } else if (msg.type === 'error') {
    es.close();
    showError(msg.message);
  }
}

function showDone(msg) {
  document.body.className = 'body-running body-done';
  const gc = document.getElementById('gradeCircle');
  gc.textContent = msg.grade;
  gc.style.background = GRADE_COLORS[msg.grade] || '#7f8c8d';
  document.getElementById('doneInterp').textContent = msg.interpretation;
}

function openReport() {
  window.open('/report', '_blank');
}

function showError(msg) {
  document.body.className = '';
  document.getElementById('startBtn').disabled = false;
  const el = document.getElementById('errorMsg');
  el.textContent = 'Error: ' + msg;
  el.classList.add('visible');
}

function downloadReport() {
  window.location.href = '/download';
}

function runAgain() {
  document.body.className = '';
  document.getElementById('startBtn').disabled = false;
  document.getElementById('warningBanner').classList.remove('visible');
  document.getElementById('phaseCards').innerHTML = '';
  chartData = []; phaseNames = []; phaseStarts = [];
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function quitServer() {
  if (!confirm('Stop the bufferbloat server and close this tab?')) return;
  fetch('/quit', {method: 'POST'}).finally(() => window.close());
}
</script>
</body>
</html>"""


def _gui_run_test(params: dict, session: TestSession) -> None:
    try:
        def cb(event: dict) -> None:
            session.event_queue.put(event)

        baseline, loaded_phases, all_results, total_duration = run_test(
            duration=params["duration"],
            ping_host=params["ping_host"],
            no_upload=params["no_upload"],
            verbose=False,
            dl_url=params["dl_url"],
            ul_url=params["ul_url"],
            extra_headers=params["extra_headers"],
            event_callback=cb,
        )

        if baseline and baseline.packet_loss() >= 5:
            session.event_queue.put({
                "type": "warning",
                "title": f"High baseline packet loss ({baseline.packet_loss():.0f}%)",
                "message": (
                    "Packet loss was detected before any load was applied. "
                    "This usually indicates unstable Wi-Fi or a congested local network. "
                    "Results may reflect local instability rather than WAN bufferbloat. "
                    "Try testing over a wired connection for accurate results."
                ),
            })

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ping_label = params.get("ping_label", params["ping_host"])
        server_desc = params.get("server_desc", "")
        html = build_html(baseline, loaded_phases, timestamp, total_duration, ping_label, server_desc)
        session.report_html = html
        grade = overall_grade(loaded_phases, baseline)
        session.event_queue.put({
            "type": "done",
            "grade": grade,
            "grade_color": GRADE_COLORS.get(grade, "#7f8c8d"),
            "interpretation": interpretation(loaded_phases, baseline),
        })
    except Exception as exc:
        session.event_queue.put({"type": "error", "message": str(exc)})
    finally:
        session.running = False


def create_app():
    from flask import Flask, Response, request, jsonify, render_template_string
    from flask import stream_with_context, make_response

    app = Flask(__name__)
    app.logger.disabled = True
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    targets_json = json.dumps({k: list(v) for k, v in TARGETS.items()})

    @app.get("/")
    def index():
        return render_template_string(GUI_TEMPLATE, targets_json=targets_json)

    @app.post("/probe")
    def probe():
        data = request.get_json() or {}
        host = (data.get("host") or "").strip()
        if not host:
            return jsonify({"error": "missing host"}), 400
        if not re.match(r'^[a-zA-Z0-9.\-]{1,253}$', host):
            return jsonify({"error": "invalid host"}), 400
        rtt = ping_once(host)
        return jsonify({"reachable": rtt is not None, "rtt": rtt})

    @app.post("/start")
    def start():
        global _session
        with _session_lock:
            if _session.running:
                return jsonify({"error": "A test is already running"}), 409
            data = request.get_json() or {}
            target_key = data.get("target", "cloudflare")
            server_key = data.get("server", "cloudflare")
            duration = max(5, min(60, int(data.get("duration", 10))))
            no_upload = bool(data.get("no_upload", False))

            if server_key not in SERVERS:
                return jsonify({"error": f"Unknown server: {server_key}"}), 400
            dl_url, ul_url, extra_headers, server_desc = SERVERS[server_key]

            custom_ping = (data.get("custom_ping") or "").strip()
            if custom_ping:
                ping_host = custom_ping
                ping_label = custom_ping
            elif target_key in TARGETS:
                ping_host, ping_label = TARGETS[target_key]
                ping_label = f"{target_key} ({ping_label})"
            else:
                ping_host = "1.1.1.1"
                ping_label = "cloudflare (1.1.1.1)"

            _session = TestSession(id=str(_uuid.uuid4()), running=True)
            params = dict(duration=duration, ping_host=ping_host, ping_label=ping_label,
                          no_upload=no_upload, dl_url=dl_url, ul_url=ul_url,
                          extra_headers=extra_headers, server_desc=server_desc)
            threading.Thread(target=_gui_run_test, args=(params, _session), daemon=True).start()
        return jsonify({"ok": True})

    @app.get("/stream")
    def stream():
        session = _session
        def generate():
            q = session.event_queue
            while True:
                try:
                    event = q.get(timeout=30)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("type") in ("done", "error"):
                        break
                except _queue.Empty:
                    yield ": keepalive\n\n"
        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/report")
    def report():
        if not _session.report_html:
            return "No report available yet", 404
        return Response(_session.report_html, mimetype="text/html")

    @app.get("/download")
    def download():
        if not _session.report_html:
            return "No report available yet", 404
        r = make_response(_session.report_html)
        r.headers["Content-Type"] = "text/html"
        r.headers["Content-Disposition"] = "attachment; filename=bufferbloat-report.html"
        return r

    @app.post("/quit")
    def quit_server():
        import signal
        response = jsonify({"ok": True})
        threading.Thread(target=lambda: (time.sleep(0.3), os.kill(os.getpid(), signal.SIGTERM)), daemon=True).start()
        return response

    return app


if __name__ == "__main__":
    main()
