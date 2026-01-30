from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path
from typing import Dict

from ..io.csvio import write_csv
from ..model.events import Event
from .intervals import compute_intervals
from .parse import parse_all_events
from .rollups import compute_rollups


def run_extract(
    *,
    root_dir: Path,
    out_dir: Path,
    base_date: str | None,
    window_seconds: int,
    quiet: bool = False,
) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    events, uuid_by_addr, last_seen = parse_all_events(root_dir, base_date, quiet=quiet)
    intervals = compute_intervals(events, last_seen)
    group_rollups, node_rollups = compute_rollups(events, intervals, window_seconds)

    events_header = Event.csv_header()
    intervals_header = [
        "interval_id",
        "group_key",
        "group_id",
        "group_name",
        "leader_uuid",
        "leader_addr",
        "start_ts",
        "end_ts",
        "duration_ms",
        "term_start",
        "start_log_index",
    ]
    group_header = [
        "window_start",
        "window_end",
        "group_key",
        "elections",
        "leader_intervals_started",
        "we_are_leader",
        "mean_leader_tenure_ms",
        "p95_leader_tenure_ms",
        "append_failures",
        "vote_rejections",
        "vote_timeouts",
        "invocation_retries",
        "invocation_timeouts",
        "membership_changes",
        "cluster_suspicions",
        "cp_autoremove_scheduled",
        "cp_autoremove_seconds_sum",
        "pre_vote_requests",
        "pre_vote_rejections",
        "pre_vote_ignored",
        "term_moves",
        "snapshots_installed",
        "tcp_disconnects",
        "tcp_connect_attempts",
        "tcp_connect_timeouts",
        "network_instability_index",
        "cp_stability_index",
    ]
    node_header = [
        "window_start",
        "window_end",
        "node_uuid",
        "node_addr",
        "leadership_time_ms",
        "votes_granted",
        "votes_rejected",
        "pre_vote_rejections",
        "follower_behind_events",
        "snapshots_installed",
        "invocation_retries",
        "invocation_timeouts",
        "suspecting_others",
        "was_suspected",
        "tcp_disconnects",
        "tcp_connect_timeouts",
        "node_risk_score",
        "asymmetry_score",
    ]

    write_csv(out_dir / "cp_events.csv", [asdict(e) for e in events], events_header)
    write_csv(out_dir / "cp_intervals.csv", intervals, intervals_header)
    write_csv(out_dir / "cp_rollups_group.csv", group_rollups, group_header)
    write_csv(out_dir / "cp_rollups_node.csv", node_rollups, node_header)

    written = [
        out_dir / "cp_events.csv",
        out_dir / "cp_intervals.csv",
        out_dir / "cp_rollups_group.csv",
        out_dir / "cp_rollups_node.csv",
    ]
    ok = all(p.exists() and p.stat().st_size > 0 for p in written)

    # summary (kept similar to original)
    if not quiet:
        by_type: Dict[str, int] = {}
        for e in events:
            by_type[e.event_type] = by_type.get(e.event_type, 0) + 1

        missing_groupkey = sum(1 for e in events if not e.group_key)
        missing_actor_uuid = sum(1 for e in events if e.node_addr and not e.node_uuid and e.event_type not in ("role_observed",))

        print("\nsummary:")
        print(f"  events:        {len(events)}")
        print(f"  intervals:     {len(intervals)}")
        print(f"  group rollups: {len(group_rollups)} (window={window_seconds}s)")
        print(f"  node rollups:  {len(node_rollups)} (window={window_seconds}s)")
        print(f"  uuid_by_addr:  {len(uuid_by_addr)}")
        print(f"  events missing group_key: {missing_groupkey}")
        print(f"  events missing actor uuid (addr known): {missing_actor_uuid}")
        print("  top event types:")
        for k, v in sorted(by_type.items(), key=lambda kv: kv[1], reverse=True)[:15]:
            print(f"    {k}: {v}")
        for p in written:
            print(f"  wrote: {p} ({p.stat().st_size} bytes)")

    if not ok:
        print("WARNING: one or more output files missing/empty", file=sys.stderr)
        return 2

    return 0

