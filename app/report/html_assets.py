CSS = """
:root {
  color-scheme: light dark;
  --bg: #0b0f14;
  --card: #111823;
  --muted: #9bb0c2;
  --text: #e7eef6;
  --border: rgba(255,255,255,0.12);
  --accent: #7dd3fc;
  --warn: #fbbf24;
  --bad: #fb7185;
}

@media (prefers-color-scheme: light) {
  :root {
    --bg: #f6f7f9;
    --card: #ffffff;
    --muted: #4b5563;
    --text: #0f172a;
    --border: rgba(15,23,42,0.14);
    --accent: #0284c7;
    --warn: #b45309;
    --bad: #be123c;
  }
}
html, body { margin:0; padding:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background: var(--bg); color: var(--text); }
header { padding: 22px 24px 6px; }
h1 { margin:0; font-size: 22px; }
small { color: var(--muted); }
main { padding: 12px 24px 40px; display: grid; gap: 16px; max-width: 1200px; }
section { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 14px 12px; }
h2 { margin: 0 0 10px; font-size: 16px; }
.note { color: var(--muted); font-size: 13px; margin-bottom: 10px; }
.kpis { display:flex; gap: 14px; flex-wrap: wrap; }
.kpi { border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; min-width: 160px; }
.kpi .v { font-size: 18px; font-weight: 650; }
.kpi .l { font-size: 12px; color: var(--muted); margin-top: 4px; }
.badge { display:inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); font-size: 12px; color: var(--muted); }
.bad { color: var(--bad); }
.warn { color: var(--warn); }
.table-wrap { max-width: 100%; overflow-x: auto; overflow-y: hidden;}
table { width: 100%;  border-collapse: collapse; font-size: 12px; table-layout: auto;}
th, td { padding: 4px 6px; line-height: 1.25;  border-bottom: 1px solid var(--border); border-right: 1px solid rgba(255,255,255,0.06); white-space: nowrap;}
th { position: sticky; top: 0; background: var(--card); z-index: 1; }
th:last-child, td:last-child { border-right: none; }
tbody tr:nth-child(odd) td { background: rgba(255, 255, 255, 0.03); }
tbody tr:nth-child(even) td { background: transparent; }
@media (prefers-color-scheme: light) {
  tbody tr:nth-child(odd) td { background: rgba(15, 23, 42, 0.04); } 
  th, td { border-right: 1px solid rgba(15,23,42,0.08); }
}
tr:hover td { background: rgba(125,211,252,0.08) !important; }
.grid2 { display:grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 900px){ .grid2 { grid-template-columns: 1fr; } }
.chart { color: var(--accent); }
"""

JS = r"""
function sortTable(tableId, colIndex) {
  const table = document.getElementById(tableId);
  const tbody = table.tBodies[0];
  const rows = Array.from(tbody.rows);
  const asc = table.getAttribute("data-sortdir") !== "asc";
  table.setAttribute("data-sortdir", asc ? "asc" : "desc");

  rows.sort((a, b) => {
    const ax = a.cells[colIndex].innerText.trim();
    const bx = b.cells[colIndex].innerText.trim();
    const an = parseFloat(ax.replace(/[^0-9.\-]/g, ''));
    const bn = parseFloat(bx.replace(/[^0-9.\-]/g, ''));
    const aNum = !Number.isNaN(an) && ax.match(/[0-9]/);
    const bNum = !Number.isNaN(bn) && bx.match(/[0-9]/);
    let cmp = 0;
    if (aNum && bNum) cmp = an - bn;
    else cmp = ax.localeCompare(bx);
    return asc ? cmp : -cmp;
  });

  for (const r of rows) tbody.appendChild(r);
}
"""
