from __future__ import annotations

import sys  # BUGFIX: original script referenced sys.stderr without importing sys
from pathlib import Path

from .insights import validate_inputs
from .render import build_html


def run_report(*, in_dir: Path, out_dir: Path, output_name: str = "cp-report.html", start_time: str | None = None, end_time: str | None = None, quiet: bool = False) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / output_name

    paths = validate_inputs(in_dir)
    html_doc = build_html(str(in_dir), paths, start_time=start_time, end_time=end_time)

    out_path.write_text(html_doc, encoding="utf-8")

    if not out_path.exists() or out_path.stat().st_size == 0:
        print("ERROR: report was not written or is empty", file=sys.stderr)
        return 2

    if not quiet:
        print(f"Wrote HTML report: {out_path} ({out_path.stat().st_size} bytes)")
    return 0