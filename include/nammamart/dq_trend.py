"""
include/nammamart/dq_trend.py
-----------------------------
Data-quality trend chart built from dq_results (bonus item).

Every run of either DAG rebuilds:
  * dq_trend table                 one row per run x source file (critical / warning failures)
  * include/reports/dq_trend.svg   stacked columns per run, ETL and ELT panels on one y-scale
  * include/reports/dq_trend.html  the same chart with hover tooltips plus a table view

Pure Python (no plotting library) so it runs in the Airflow image as-is.
Colours are the validated categorical slots, in fixed order, one per source file.
"""

import html
import os

from nammamart import config as cfg
from nammamart.db import connect

REPORT_DIR = os.path.join(cfg.INCLUDE_DIR, "reports")
FILES = ["orders", "payments", "customers", "products", "stores"]   # fixed slot order
DAGS = [("nammamart_etl_priyanka", "ETL"), ("nammamart_elt_priyanka", "ELT")]

TREND_SQL = """
CREATE OR REPLACE TABLE dq_trend AS
WITH runs AS (
    SELECT dag_id, run_id, min(checked_at) AS checked_at FROM dq_results GROUP BY ALL
),
all_runs AS (
    SELECT coalesce(p.dag_id, r.dag_id) AS dag_id, coalesce(p.run_id, r.run_id) AS run_id,
           coalesce(p.start_time, r.checked_at) AS run_start,
           CASE WHEN r.run_id IS NOT NULL THEN coalesce(p.status, 'RUNNING') ELSE 'FAILED' END AS run_status
    FROM pipeline_runs p FULL OUTER JOIN runs r ON p.run_id = r.run_id AND p.dag_id = r.dag_id
),
files AS (SELECT unnest(['orders', 'payments', 'customers', 'products', 'stores']) AS file)
SELECT a.dag_id, a.run_id, a.run_start, a.run_status, f.file,
       coalesce(sum(d.rows_failed) FILTER (WHERE d.severity = 'Critical'), 0) AS critical_failures,
       coalesce(sum(d.rows_failed) FILTER (WHERE d.severity = 'Warning'), 0)  AS warning_failures,
       count(d.check_name) FILTER (WHERE d.status = 'FAIL')                  AS checks_failed
FROM all_runs a CROSS JOIN files f
LEFT JOIN dq_results d
       ON d.run_id = a.run_id AND d.dag_id = a.dag_id AND split_part(d.check_name, '.', 1) = f.file
WHERE a.dag_id IN ('nammamart_etl_priyanka', 'nammamart_elt_priyanka')
GROUP BY ALL
ORDER BY a.dag_id, a.run_start, f.file
"""

# Concrete colours per theme (no CSS custom properties: some SVG renderers, e.g. librsvg
# and image previews, do not support var()). Light is the default; dark overrides it.
THEMES = {
    "light": {"surface": "#fcfcfb", "primary": "#0b0b0b", "secondary": "#52514e", "muted": "#898781",
              "grid": "#e1e0d9", "axis": "#c3c2b7",
              "series": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]},
    "dark": {"surface": "#1a1a19", "primary": "#f0efec", "secondary": "#c3c2b7", "muted": "#898781",
             "grid": "#2c2c2a", "axis": "#383835",
             "series": ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181"]},
}
CRITICAL_INK = "#d03b3b"


def _theme_rules(t, scope):
    rules = [f"{scope} .bg {{ fill: {t['surface']}; }}",
             f"{scope} .title {{ fill: {t['primary']}; }}",
             f"{scope} .sub, {scope} .panel, {scope} .legend, {scope} .total {{ fill: {t['secondary']}; }}",
             f"{scope} .tick, {scope} .xlab {{ fill: {t['muted']}; }}",
             f"{scope} .grid {{ stroke: {t['grid']}; }}",
             f"{scope} .base {{ stroke: {t['axis']}; }}"]
    rules += [f"{scope} .s{i + 1} {{ fill: {c}; }}" for i, c in enumerate(t["series"])]
    return "\n".join(rules)


STYLE = "\n".join([
    ".viz-root { font-family: system-ui, -apple-system, 'Segoe UI', sans-serif; }",
    ".title { font-size: 15px; font-weight: 600; }",
    ".sub, .panel, .legend { font-size: 12px; } .panel { font-weight: 600; }",
    ".tick, .xlab, .total, .fail { font-size: 11px; font-variant-numeric: tabular-nums; }",
    f".fail {{ fill: {CRITICAL_INK}; }} .grid, .base {{ stroke-width: 1; }} .seg:hover {{ opacity: 0.8; }}",
    _theme_rules(THEMES["light"], ".viz-root"),
    "@media (prefers-color-scheme: dark) {",
    _theme_rules(THEMES["dark"], ':root:where(:not([data-theme="light"])) .viz-root'),
    "}",
    _theme_rules(THEMES["dark"], ':root[data-theme="dark"] .viz-root'),
])


def _rows():
    with connect() as con:
        con.execute(TREND_SQL)
        return con.execute("""SELECT dag_id, run_id, run_start, run_status, file,
                                     critical_failures, warning_failures, checks_failed
                              FROM dq_trend ORDER BY dag_id, run_start, file""").fetchall()


def _nice_max(v):
    for step in [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]:
        if v <= step * 5:
            return step * ((int(v) // step) + 1), step
    return v, v / 5


def _svg(runs_by_dag, interactive=False):
    width, left, right = 980, 56, 16
    panel_h, top = 190, 92
    height = top + len(DAGS) * (panel_h + 58) + 10
    ymax = max([sum(r["crit"].values()) for runs in runs_by_dag.values() for r in runs] + [1])
    ymax, step = _nice_max(ymax)
    # The theme class sits on an inner <g>: in a standalone SVG, :root is the <svg> itself,
    # so the dark-mode selector (":root ... .viz-root") needs .viz-root to be a descendant.
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
           f'width="{width}" height="{height}" role="img" aria-labelledby="t d">',
           f'<style>{STYLE}</style>',
           '<g class="viz-root">',
           f'<rect class="bg" width="{width}" height="{height}" rx="8"/>',
           '<text id="t" class="title" x="20" y="28">Rows failing Critical data-quality rules, per pipeline run</text>',
           '<text id="d" class="sub" x="20" y="47">Built from dq_results. A row that breaks two rules counts twice. '
           'Same y-scale for both pipelines; failed runs wrote no results.</text>']
    # legend (always present for >= 2 series; identity never by colour alone: table view + labels)
    lx = 20
    for i, f in enumerate(FILES):
        out.append(f'<rect class="s{i + 1}" x="{lx}" y="62" width="12" height="12" rx="3"/>'
                   f'<text class="legend" x="{lx + 17}" y="72">{f}</text>')
        lx += 30 + 7.5 * len(f) + 18
    plot_w = width - left - right
    for p, (dag_id, label) in enumerate(DAGS):
        runs = runs_by_dag.get(dag_id, [])
        y0 = top + p * (panel_h + 58) + 30          # top of plot area (panel title sits above it)
        yb = y0 + panel_h - 30                      # baseline
        scale = (yb - y0) / ymax
        out.append(f'<text class="panel" x="20" y="{y0 - 16}">{label} pipeline ({dag_id})</text>')
        for k in range(0, int(ymax) + 1, int(step)):
            y = yb - k * scale
            out.append(f'<line class="grid" x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}"/>'
                       f'<text class="tick" x="{left - 6}" y="{y + 4:.1f}" text-anchor="end">{k:,}</text>')
        out.append(f'<line class="base" x1="{left}" x2="{width - right}" y1="{yb}" y2="{yb}"/>')
        n = max(len(runs), 1)
        slot = plot_w / n
        bw = min(34, slot * 0.6)
        for j, r in enumerate(runs):
            cx = left + slot * (j + 0.5)
            x = cx - bw / 2
            stamp = r["start"].strftime("%H:%M") if r["start"] else "?"
            out.append(f'<text class="xlab" x="{cx:.1f}" y="{yb + 16}" text-anchor="middle">#{j + 1}</text>'
                       f'<text class="xlab" x="{cx:.1f}" y="{yb + 29}" text-anchor="middle">{stamp}</text>')
            if r["status"] == "FAILED":
                out.append(f'<text class="fail" x="{cx:.1f}" y="{yb - 6}" text-anchor="middle">✕ failed</text>')
                continue
            total = sum(r["crit"].values())
            segs = [(i, f, r["crit"][f]) for i, f in enumerate(FILES) if r["crit"][f] > 0]
            y = yb
            for s, (i, f, v) in enumerate(segs):
                h = v * scale
                gap = 2 if s > 0 and h > 3 else 0              # 2px surface gap between stacked fills
                is_top = s == len(segs) - 1
                hh = max(h - gap, 1)
                top_y = y - h
                # 4px rounded data-end on the top segment only; the stack is anchored to the baseline
                rr = min(4, hh / 2, bw / 2) if is_top else 0
                path = (f"M{x:.1f},{y - gap:.1f} V{top_y + rr:.1f} Q{x:.1f},{top_y:.1f} {x + rr:.1f},{top_y:.1f} "
                        f"H{x + bw - rr:.1f} Q{x + bw:.1f},{top_y:.1f} {x + bw:.1f},{top_y + rr:.1f} V{y - gap:.1f} Z")
                tip = html.escape(f"{label} run #{j + 1} ({stamp} UTC) · {f}: {v:,} critical failures")
                out.append(f'<path class="seg s{i + 1}" d="{path}"><title>{tip}</title></path>')
                y = top_y
            out.append(f'<text class="total" x="{cx:.1f}" y="{yb - total * scale - 6:.1f}" '
                       f'text-anchor="middle">{total:,}</text>')
    out.append("</g></svg>")
    return "\n".join(out)


def _table(runs_by_dag):
    head = "".join(f"<th>{f}</th>" for f in FILES)
    rows = []
    for dag_id, label in DAGS:
        for j, r in enumerate(runs_by_dag.get(dag_id, [])):
            stamp = r["start"].strftime("%Y-%m-%d %H:%M") if r["start"] else ""
            if r["status"] == "FAILED":
                cells = f'<td colspan="{len(FILES) + 1}">run failed, no dq_results</td>'
            else:
                cells = "".join(f"<td>{r['crit'][f]:,}</td>" for f in FILES) + f"<td>{sum(r['crit'].values()):,}</td>"
            rows.append(f"<tr><td>{label}</td><td>#{j + 1}</td><td>{stamp}</td>"
                        f"<td><code>{html.escape(r['run_id'])}</code></td>{cells}</tr>")
    return (f'<table><thead><tr><th>Pipeline</th><th>Run</th><th>Start (UTC)</th><th>run_id</th>{head}'
            f'<th>Total</th></tr></thead><tbody>{"".join(rows)}</tbody></table>')


def build_dq_trend():
    runs_by_dag = {}
    for dag_id, run_id, start, status, f, crit, warn, _ in _rows():
        runs = runs_by_dag.setdefault(dag_id, [])
        if not runs or runs[-1]["run_id"] != run_id:
            runs.append({"run_id": run_id, "start": start, "status": status, "crit": {}, "warn": {}})
        runs[-1]["crit"][f] = int(crit)
        runs[-1]["warn"][f] = int(warn)

    os.makedirs(REPORT_DIR, exist_ok=True)
    svg = _svg(runs_by_dag)
    with open(os.path.join(REPORT_DIR, "dq_trend.svg"), "w", encoding="utf-8") as fh:
        fh.write(svg)
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>NammaMart DQ Trend</title>
<style>
{STYLE}
body {{ margin: 0; padding: 16px; background: #fcfcfb; color: #0b0b0b; }}
@media (prefers-color-scheme: dark) {{ body {{ background: #1a1a19; color: #f0efec; }} th, td {{ border-color: #2c2c2a !important; }} }}
.wrap {{ max-width: 1000px; margin: 0 auto; }}
svg {{ width: 100%; height: auto; }}
table {{ border-collapse: collapse; font-size: 13px; width: 100%; margin-top: 16px; font-variant-numeric: tabular-nums; }}
th, td {{ border-bottom: 1px solid #e1e0d9; padding: 4px 8px; text-align: right; }}
th:nth-child(-n+4), td:nth-child(-n+4) {{ text-align: left; }}
th {{ font-weight: 600; }}
code {{ font-size: 11px; color: #898781; }}
.scroll {{ overflow-x: auto; }}
</style></head>
<body class="viz-root"><div class="wrap">
{svg}
<p style="font-size:13px">Hover a segment for its value. Table view (same data, rows failing Critical rules per source file):</p>
<div class="scroll">{_table(runs_by_dag)}</div>
</div></body></html>"""
    with open(os.path.join(REPORT_DIR, "dq_trend.html"), "w", encoding="utf-8") as fh:
        fh.write(page)
    counts = {label: len(runs_by_dag.get(d, [])) for d, label in DAGS}
    print(f"[DQ trend] {counts} runs charted -> {REPORT_DIR}/dq_trend.svg and dq_trend.html")
    return counts
