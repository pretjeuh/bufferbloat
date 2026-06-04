# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.1.1] - 2026-06-04

### Added
- **Quit button** in the GUI done screen — click to stop the Flask server and close the browser tab without needing Ctrl+C in the terminal
- Cleaner terminal startup message shows both ways to stop the server

---

## [1.1.0] - 2026-06-04

### Added
- **Web GUI** — launch with `python3 bufferbloat.py --gui`, opens automatically in your browser
  - Live RTT chart with phase boundaries drawn on canvas (no external JS libraries)
  - Phase summary cards appear as each phase completes
  - Colored grade badge on completion
  - Full report embedded inline, with download button
  - Advanced options panel: custom load server and custom ping host override
- **Auto-venv bootstrap** — on first run, the script detects missing dependencies and offers to create a virtual environment and install them automatically (prompts for confirmation before doing anything)
- **Windows support** — ping now uses correct `-n`/`-w` flags on Windows
- **32 preset ping targets** across 8 regions:
  - Global anycast: Cloudflare, Google, Quad9, OpenDNS
  - Western Europe: Amsterdam, Frankfurt, London, Paris, Madrid, Milan, Zurich, Lisbon, Brussels
  - Northern Europe: Stockholm, Helsinki, Oslo, Copenhagen
  - Eastern/Central Europe: Warsaw, Prague, Vienna, Budapest, Sofia, Bucharest
  - Turkey: Istanbul (IXTR), ist-ix
  - Middle East: Dubai
  - North America: New York (DE-CIX), Ashburn
  - Asia Pacific: Singapore, Tokyo, Sydney
- **`--port`** flag for the GUI web server (default: 5757)

### Changed
- GUI target selector simplified: single dropdown replaces separate ping target + load server controls
- Ping host hint shown below target dropdown so users know what IP is being pinged
- Load server and custom ping host moved to collapsible "Advanced options" section

### Fixed
- GUI dropdown was empty due to Jinja2 HTML-escaping the JSON — fixed by injecting data via `<script type="application/json">` tag

---

## [1.0.0] - 2026-06-03

### Added
- **Core bufferbloat measurement** — five test phases: Baseline, Download, Upload, Bidirectional, Recovery
- **A–F grading** based on worst-case latency increase under load (bloat):
  - A: < 5 ms · B: 5–30 ms · C: 30–60 ms · D: 60–200 ms · F: > 200 ms
- **Speed measurement** — download and upload throughput (Mbps) tracked per phase via thread-safe byte counter
- **Packet loss tracking** — per-phase loss percentage, color-coded in the report (orange > 0%, red ≥ 5%)
- **Baseline packet loss warning** — if loss is detected before any load is applied, a warning banner is shown in both the terminal and the HTML report, explaining that Wi-Fi instability may be skewing results
- **Self-contained HTML report** — inline CSS, base64-encoded PNG charts, works offline
  - Latency over time line chart with phase boundaries
  - RTT distribution box plots per phase
  - Throughput bar chart (download + upload per phase)
  - Per-phase results table: min / median / P90 / max / loss% / ↓ Mbps / ↑ Mbps / bloat / grade
  - Plain-English interpretation of results
- **PDF output** — via `weasyprint` (optional); falls back to HTML with a warning if not installed
- **Load generation** — pure Python HTTP, no iperf3 required; 4 parallel threads per direction
- **Cloudflare speed test endpoints** as default load server (anycast, hits nearest PoP); OVH as alternative (`--server ovh`)
- **CLI flags**: `--output`, `--duration`, `--target`, `--ping-host`, `--list-targets`, `--server`, `--no-upload`, `--verbose`
- Works on macOS and Linux
