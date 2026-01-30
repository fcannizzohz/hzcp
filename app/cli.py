# hzcp/cli.py

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from . import __version__
from .extract.pipeline import run_extract
from .report.pipeline import run_report


def _add_common_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output (errors still printed).",
    )


# -----------------------
# command implementations
# -----------------------

def _cmd_extract(args: argparse.Namespace) -> int:
    in_dir = Path(args.input).resolve()
    out_dir = Path(args.out).resolve() if args.out else in_dir

    return run_extract(
        root_dir=in_dir,
        out_dir=out_dir,
        base_date=args.base_date,
        window_seconds=args.window_seconds,
        quiet=args.quiet,
    )


def _cmd_report(args: argparse.Namespace) -> int:
    in_dir = Path(args.input).resolve()
    out_dir = Path(args.out).resolve() if args.out else in_dir

    return run_report(
        in_dir=in_dir,
        out_dir=out_dir,
        output_name=args.name,
        start_time=args.start_time,
        end_time=args.end_time,
        quiet=args.quiet,
    )


def _cmd_all(args: argparse.Namespace) -> int:
    in_dir = Path(args.input).resolve()
    out_dir = Path(args.out).resolve() if args.out else in_dir

    rc = run_extract(
        root_dir=in_dir,
        out_dir=out_dir,
        base_date=args.base_date,
        window_seconds=args.window_seconds,
        quiet=args.quiet,
    )
    if rc != 0:
        return rc

    return run_report(
        in_dir=out_dir,
        out_dir=out_dir,
        output_name=args.name,
        start_time=args.start_time,
        end_time=args.end_time,
        quiet=args.quiet,
    )


# -----------------------
# parser wiring
# -----------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="hzcp",
        description="Hazelcast CP log extractor + HTML reporter",
    )
    p.add_argument("--version", action="store_true", help="Print version and exit.")

    sub = p.add_subparsers(dest="cmd")

    # extract
    pe = sub.add_parser("extract", help="Parse worker.log files and write CSVs.")
    pe.add_argument(
        "--in",
        dest="input",
        required=True,
        help="Root directory to scan recursively for worker.log files",
    )
    pe.add_argument(
        "--out",
        default=None,
        help="Directory to write CSVs (defaults to --in)",
    )
    pe.add_argument("--base-date", default=None, help="Anchor date for time-only logs: YYYY-MM-DD")
    pe.add_argument("--window-seconds", type=int, default=60, help="Rollup window size in seconds (default 60)")
    _add_common_flags(pe)
    pe.set_defaults(_fn=_cmd_extract)

    # report
    pr = sub.add_parser("report", help="Generate HTML report from CSVs.")
    pr.add_argument(
        "--in",
        dest="input",
        required=True,
        help="Directory containing cp_*.csv files",
    )
    pr.add_argument(
        "--out",
        default=None,
        help="Directory to write the HTML report (defaults to --in)",
    )
    pr.add_argument("--name", default="cp-report.html", help="Output file name (default: cp-report.html)")
    pr.add_argument("--start-time", dest="start_time", default=None, help="Filter: include only events with ts >= START (e.g. '2026-01-30 15:00' or ISO-8601)")
    pr.add_argument("--end-time", dest="end_time", default=None, help="Filter: include only events with ts < END (same format as --start-time)")
    _add_common_flags(pr)
    pr.set_defaults(_fn=_cmd_report)

    # all
    pa = sub.add_parser("all", help="Run extract then report.")
    pa.add_argument(
        "--in",
        dest="input",
        required=True,
        help="Root directory to scan recursively for worker.log files",
    )
    pa.add_argument(
        "--out",
        default=None,
        help="Directory to write CSVs and the HTML report (defaults to --in)",
    )
    pa.add_argument("--base-date", default=None, help="Anchor date for time-only logs: YYYY-MM-DD")
    pa.add_argument("--window-seconds", type=int, default=60, help="Rollup window size in seconds (default 60)")
    pa.add_argument("--name", default="cp-report.html", help="Output file name (default: cp-report.html)")
    pa.add_argument("--start-time", dest="start_time", default=None, help="Filter: include only events with ts >= START (e.g. '2026-01-30 15:00' or ISO-8601)")
    pa.add_argument("--end-time", dest="end_time", default=None, help="Filter: include only events with ts < END (same format as --start-time)")
    _add_common_flags(pa)
    pa.set_defaults(_fn=_cmd_all)

    return p


# -----------------------
# entrypoint
# -----------------------

def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(__version__)
        return 0

    if not getattr(args, "cmd", None):
        parser.print_help(sys.stderr)
        return 2

    fn = getattr(args, "_fn", None)
    if fn is None:
        parser.print_help(sys.stderr)
        return 2

    try:
        return int(fn(args))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
