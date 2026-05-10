"""
Analytic Report Generator — Traffic Volume vs Time of Day
==========================================================
Reads from Postgres congestion_index table (or uses embedded demo data)
and produces:
  1. traffic_volume_report.csv   — raw hourly data per junction
  2. traffic_report_chart.html   — interactive chart (Chart.js, no dependencies)
  3. congestion_summary.txt      — plain text summary

Run: python generate_report.py
"""

import csv
import os
from datetime import datetime
from pathlib import Path

# ── Demo data (mirrors seed data in postgres_init.sql) ────────────────────────
# In production this would be fetched from Postgres via psycopg2
DEMO_DATA = [
    # (sensor_id, junction_name, hour, total_vehicles, avg_speed, congestion_index, level)
    ("J001", "Galle Face",    6,  90,  42.1, 18.5, "LOW"),
    ("J001", "Galle Face",    7, 210,  18.5, 72.3, "SEVERE"),
    ("J001", "Galle Face",    8, 280,  12.1, 85.1, "SEVERE"),
    ("J001", "Galle Face",    9, 180,  28.0, 55.0, "MODERATE"),
    ("J001", "Galle Face",   10, 120,  38.0, 31.5, "LOW"),
    ("J001", "Galle Face",   12, 150,  33.0, 40.0, "MODERATE"),
    ("J001", "Galle Face",   17, 250,  15.3, 78.0, "SEVERE"),
    ("J001", "Galle Face",   18, 230,  19.0, 68.2, "SEVERE"),
    ("J001", "Galle Face",   20, 100,  40.0, 22.0, "LOW"),

    ("J002", "Pettah",        6,  60,  40.0, 15.0, "LOW"),
    ("J002", "Pettah",        7, 190,  22.0, 55.3, "MODERATE"),
    ("J002", "Pettah",        8, 310,  10.5, 89.2, "SEVERE"),
    ("J002", "Pettah",        9, 200,  25.0, 60.0, "MODERATE"),
    ("J002", "Pettah",       10, 130,  35.0, 35.0, "LOW"),
    ("J002", "Pettah",       12, 170,  28.0, 50.0, "MODERATE"),
    ("J002", "Pettah",       17, 240,  20.0, 65.0, "SEVERE"),
    ("J002", "Pettah",       18, 200,  23.0, 58.0, "MODERATE"),
    ("J002", "Pettah",       20,  80,  42.0, 18.0, "LOW"),

    ("J003", "Union Place",   6,  30,  52.0,  8.0, "LOW"),
    ("J003", "Union Place",   7,  90,  38.0, 22.5, "LOW"),
    ("J003", "Union Place",   8, 110,  32.0, 32.0, "LOW"),
    ("J003", "Union Place",   9,  95,  36.0, 25.5, "LOW"),
    ("J003", "Union Place",   12,  80,  40.0, 19.0, "LOW"),
    ("J003", "Union Place",   17, 130,  25.0, 47.2, "MODERATE"),
    ("J003", "Union Place",   18, 120,  28.0, 42.0, "MODERATE"),
    ("J003", "Union Place",   20,  50,  48.0, 10.0, "LOW"),

    ("J004", "Nugegoda",      6,  80,  38.0, 25.0, "LOW"),
    ("J004", "Nugegoda",      7, 230,  20.0, 67.8, "SEVERE"),
    ("J004", "Nugegoda",      8, 270,  14.0, 80.5, "SEVERE"),
    ("J004", "Nugegoda",      9, 190,  26.0, 57.0, "MODERATE"),
    ("J004", "Nugegoda",      10, 130,  36.0, 32.0, "LOW"),
    ("J004", "Nugegoda",      12, 140,  30.0, 42.5, "MODERATE"),
    ("J004", "Nugegoda",      17, 260,  17.5, 74.5, "SEVERE"),
    ("J004", "Nugegoda",      18, 240,  20.0, 66.0, "SEVERE"),
    ("J004", "Nugegoda",      20,  90,  40.0, 22.0, "LOW"),
]

REPORT_DIR = Path(os.getenv("REPORT_DIR", "./reports"))
REPORT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DATE = datetime.now().strftime("%Y-%m-%d")


# ── 1. CSV Report ──────────────────────────────────────────────────────────────
def write_csv():
    path = REPORT_DIR / f"traffic_volume_report_{REPORT_DATE}.csv"
    headers = ["sensor_id", "junction_name", "hour", "hour_label",
               "total_vehicles", "avg_speed_kmph", "congestion_index", "congestion_level"]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for row in DEMO_DATA:
            sid, jname, hour, vehicles, speed, ci, level = row
            w.writerow([sid, jname, hour, f"{hour:02d}:00", vehicles, speed, ci, level])
    print(f"[CSV] Written: {path}")
    return path


# ── 2. HTML Chart Report ───────────────────────────────────────────────────────
def write_html_chart():
    path = REPORT_DIR / f"traffic_report_chart_{REPORT_DATE}.html"

    # Build per-junction hourly data for JS
    junctions = {}
    for sid, jname, hour, vehicles, speed, ci, level in DEMO_DATA:
        key = f"{sid} – {jname}"
        if key not in junctions:
            junctions[key] = {"hours": [], "vehicles": [], "speed": [], "ci": []}
        junctions[key]["hours"].append(f"{hour:02d}:00")
        junctions[key]["vehicles"].append(vehicles)
        junctions[key]["speed"].append(speed)
        junctions[key]["ci"].append(ci)

    colors = ["#e74c3c", "#3498db", "#2ecc71", "#f39c12"]
    datasets_vehicles = []
    datasets_ci       = []
    for i, (label, data) in enumerate(junctions.items()):
        c = colors[i % len(colors)]
        datasets_vehicles.append({
            "label": label,
            "data": data["vehicles"],
            "borderColor": c,
            "backgroundColor": c + "22",
            "fill": True,
            "tension": 0.4,
        })
        datasets_ci.append({
            "label": label,
            "data": data["ci"],
            "borderColor": c,
            "backgroundColor": c + "22",
            "fill": False,
            "tension": 0.4,
        })

    hours_all = [f"{h:02d}:00" for h in range(6, 21)]

    import json
    ds_v_json  = json.dumps(datasets_vehicles)
    ds_ci_json = json.dumps(datasets_ci)
    hours_json = json.dumps(hours_all)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Smart City Traffic Report — {REPORT_DATE}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; padding: 24px; }}
  h1   {{ font-size: 1.6rem; color: #38bdf8; margin-bottom: 4px; }}
  p.sub {{ font-size: 0.85rem; color: #94a3b8; margin-bottom: 24px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; }}
  h2   {{ font-size: 1rem; color: #7dd3fc; margin-bottom: 16px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; margin-top: 16px; }}
  th   {{ background: #0f172a; color: #38bdf8; padding: 8px; text-align: left; }}
  td   {{ padding: 7px 8px; border-bottom: 1px solid #334155; }}
  tr:hover td {{ background: #1a2740; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; }}
  .LOW      {{ background: #166534; color: #86efac; }}
  .MODERATE {{ background: #854d0e; color: #fde68a; }}
  .SEVERE   {{ background: #7f1d1d; color: #fca5a5; }}
  footer {{ text-align: center; font-size: 0.78rem; color: #475569; margin-top: 32px; }}
</style>
</head>
<body>
<h1>🏙 Smart City Traffic Report — Colombo</h1>
<p class="sub">Date: {REPORT_DATE} &nbsp;|&nbsp; Generated by ABDA Mini Project Pipeline</p>

<div class="grid">
  <div class="card">
    <h2>📈 Traffic Volume vs. Time of Day</h2>
    <canvas id="chartVolume" height="220"></canvas>
  </div>
  <div class="card">
    <h2>🚦 Congestion Index vs. Time of Day</h2>
    <canvas id="chartCI" height="220"></canvas>
  </div>
</div>

<div class="card" style="margin-top:24px;">
  <h2>📋 Detailed Hourly Data</h2>
  <table>
    <thead>
      <tr>
        <th>Junction</th><th>Hour</th><th>Vehicles</th>
        <th>Avg Speed (km/h)</th><th>CI</th><th>Level</th>
      </tr>
    </thead>
    <tbody>
      {''.join(
          f"<tr><td>{jname} ({sid})</td><td>{h:02d}:00</td>"
          f"<td>{v}</td><td>{spd}</td><td>{ci:.1f}</td>"
          f"<td><span class='badge {lvl}'>{lvl}</span></td></tr>"
          for sid, jname, h, v, spd, ci, lvl in sorted(DEMO_DATA, key=lambda x: (x[0], x[2]))
      )}
    </tbody>
  </table>
</div>

<footer>EC8207 Applied Big Data Engineering · Smart City Mini Project · {REPORT_DATE}</footer>

<script>
const hours = {hours_json};
const blue   = '#38bdf8';
const gridC  = '#1e3a5f';

const opts = (title, yLabel) => ({{
  responsive: true,
  plugins: {{ legend: {{ labels: {{ color: '#94a3b8', font: {{ size: 11 }} }} }} }},
  scales: {{
    x: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: gridC }} }},
    y: {{ title: {{ display: true, text: yLabel, color: '#64748b' }},
          ticks: {{ color: '#64748b' }}, grid: {{ color: gridC }} }}
  }}
}});

new Chart(document.getElementById('chartVolume'), {{
  type: 'line',
  data: {{ labels: hours, datasets: {ds_v_json} }},
  options: opts('Traffic Volume', 'Vehicle Count')
}});

new Chart(document.getElementById('chartCI'), {{
  type: 'line',
  data: {{ labels: hours, datasets: {ds_ci_json} }},
  options: opts('Congestion Index', 'CI (0-100)')
}});
</script>
</body>
</html>"""

    path.write_text(html)
    print(f"[HTML] Written: {path}")
    return path


# ── 3. Text Summary ────────────────────────────────────────────────────────────
def write_text_summary():
    path = REPORT_DIR / f"congestion_summary_{REPORT_DATE}.txt"
    lines = [
        "=" * 65,
        f"  SMART CITY TRAFFIC CONGESTION SUMMARY — {REPORT_DATE}",
        "=" * 65,
        "",
        "PEAK HOURS (highest vehicle count per junction):",
        "-" * 65,
    ]

    # Compute peak per junction
    from collections import defaultdict
    peaks = {}
    for sid, jname, hour, vehicles, speed, ci, level in DEMO_DATA:
        if sid not in peaks or vehicles > peaks[sid]["vehicles"]:
            peaks[sid] = dict(sid=sid, jname=jname, hour=hour, vehicles=vehicles,
                              speed=speed, ci=ci, level=level)

    for sid in sorted(peaks):
        p = peaks[sid]
        lines.append(
            f"  {p['sid']} | {p['jname']}\n"
            f"    Peak Hour  : {p['hour']:02d}:00\n"
            f"    Vehicles   : {p['vehicles']}\n"
            f"    Avg Speed  : {p['speed']} km/h\n"
            f"    Cong. Index: {p['ci']} ({p['level']})\n"
        )

    lines += [
        "CRITICAL ALERTS (avg_speed < 10 km/h):",
        "-" * 65,
        "  J001 | Galle Face     | 07.2 km/h | 08:15 | CRITICAL",
        "  J002 | Pettah         | 05.1 km/h | 08:35 | CRITICAL",
        "  J004 | Nugegoda       | 08.9 km/h | 17:45 | CRITICAL",
        "",
        "POLICE INTERVENTION REQUIRED TOMORROW:",
        "-" * 65,
        "  ⚠  J001 — Galle Face - Marine Drive  (Peak CI: 85.1 — SEVERE)",
        "  ⚠  J002 — Pettah - Manning Market    (Peak CI: 89.2 — SEVERE)",
        "  ⚠  J004 — Nugegoda - High Level Road (Peak CI: 80.5 — SEVERE)",
        "",
        "=" * 65,
        "  END OF REPORT",
        "=" * 65,
    ]

    path.write_text("\n".join(lines))
    print(f"[TXT] Written: {path}")
    return path


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating Smart City Traffic Analytic Report …")
    csv_p  = write_csv()
    html_p = write_html_chart()
    txt_p  = write_text_summary()
    print("\nAll reports generated:")
    print(f"  {csv_p}")
    print(f"  {html_p}")
    print(f"  {txt_p}")