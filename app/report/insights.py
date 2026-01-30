from __future__ import annotations

import html
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..io.csvio import read_csv


def to_int(x: str, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def to_float(x: str, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def esc(s: Any) -> str:
    return html.escape("" if s is None else str(s))


def pct(n: float, d: float) -> str:
    if d <= 0:
        return "0.0%"
    return f"{(100.0 * n / d):.1f}%"


def pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = 0.0
    dx2 = 0.0
    dy2 = 0.0
    for x, y in zip(xs, ys):
        dx = x - mx
        dy = y - my
        num += dx * dy
        dx2 += dx * dx
        dy2 += dy * dy
    den = math.sqrt(dx2 * dy2)
    if den == 0:
        return None
    return num / den


@dataclass
class Paths:
    events: Path
    intervals: Path
    roll_group: Path
    roll_node: Path


def validate_inputs(in_dir: Path) -> Paths:
    paths = Paths(
        events=in_dir / "cp_events.csv",
        intervals=in_dir / "cp_intervals.csv",
        roll_group=in_dir / "cp_rollups_group.csv",
        roll_node=in_dir / "cp_rollups_node.csv",
    )
    missing = [p for p in [paths.events, paths.intervals, paths.roll_group, paths.roll_node] if not p.exists()]
    if missing:
        raise SystemExit(f"Missing required CSVs in {in_dir}:\n  " + "\n  ".join(str(m) for m in missing))
    return paths


def load_all(paths: Paths) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    events = read_csv(paths.events)
    intervals = read_csv(paths.intervals)
    rg = read_csv(paths.roll_group)
    rn = read_csv(paths.roll_node)
    return events, intervals, rg, rn


def summarize_event_types(events: List[Dict[str, str]]) -> List[Tuple[str, int]]:
    c: Dict[str, int] = {}
    for e in events:
        t = e.get("event_type", "")
        c[t] = c.get(t, 0) + 1
    return sorted(c.items(), key=lambda kv: kv[1], reverse=True)


def leader_stats(intervals: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    per_group: Dict[str, Dict[str, Any]] = {}
    per_leader: Dict[str, Dict[str, Any]] = {}

    for it in intervals:
        gk = it.get("group_key") or it.get("group_id") or ""
        leader = it.get("leader_uuid", "") or "(unknown-leader-uuid)"
        dur = to_int(it.get("duration_ms", "0"))
        per_group.setdefault(gk, {"group_key": gk, "intervals": 0, "total_ms": 0, "leaders": set(), "durations": []})
        pg = per_group[gk]
        pg["intervals"] += 1
        pg["total_ms"] += dur
        pg["leaders"].add(leader)
        pg["durations"].append(dur)

        per_leader.setdefault(leader, {"leader_uuid": leader, "total_ms": 0, "groups": set(), "intervals": 0})
        pl = per_leader[leader]
        pl["total_ms"] += dur
        pl["groups"].add(gk)
        pl["intervals"] += 1

    group_rows: List[Dict[str, Any]] = []
    for gk, pg in per_group.items():
        durs = sorted(pg["durations"])
        mean = int(sum(durs) / len(durs)) if durs else 0
        p95 = durs[max(0, math.ceil(0.95 * len(durs)) - 1)] if durs else 0
        churn_per_hr = 0.0
        if pg["total_ms"] > 0:
            churn_per_hr = (pg["intervals"] * 3600_000.0) / pg["total_ms"]
        group_rows.append(
            {
                "group_key": gk,
                "leader_intervals_started": max(0, pg["intervals"] - 1),
                "intervals": pg["intervals"],
                "distinct_leaders": len(pg["leaders"]),
                "total_min": round(pg["total_ms"] / 60000.0, 1),
                "mean_s": round(mean / 1000.0, 2),
                "p95_s": round(p95 / 1000.0, 2),
                "churn_per_hr": round(churn_per_hr, 3),
            }
        )

    leader_rows: List[Dict[str, Any]] = []
    total_all = sum(pl["total_ms"] for pl in per_leader.values()) or 1
    for leader, pl in per_leader.items():
        leader_rows.append(
            {
                "leader_uuid": leader,
                "total_min": round(pl["total_ms"] / 60000.0, 1),
                "share": pct(pl["total_ms"], total_all),
                "groups": len(pl["groups"]),
                "intervals": pl["intervals"],
            }
        )

    group_rows.sort(key=lambda r: (r["leader_intervals_started"], r["churn_per_hr"]), reverse=True)
    leader_rows.sort(key=lambda r: r["total_min"], reverse=True)
    return group_rows, leader_rows


def top_bad_windows(group_rollups: List[Dict[str, str]], n: int = 15) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in group_rollups:
        rows.append(
            {
                "window_start": r.get("window_start", ""),
                "window_end": r.get("window_end", ""),
                "group_key": r.get("group_key", ""),
                "network_instability_index": to_int(r.get("network_instability_index", "0")),
                "tcp_connect_timeouts": to_int(r.get("tcp_connect_timeouts", "0")),
                "tcp_disconnects": to_int(r.get("tcp_disconnects", "0")),
                "pre_vote_rejections": to_int(r.get("pre_vote_rejections", "0")),
                "cluster_suspicions": to_int(r.get("cluster_suspicions", "0")),
                "elections": to_int(r.get("elections", "0")),
                "leader_intervals_started": to_int(r.get("leader_intervals_started", "0")),
                "cp_autoremove_scheduled": to_int(r.get("cp_autoremove_scheduled", "0")),
            }
        )
    rows.sort(key=lambda x: x["network_instability_index"], reverse=True)
    return rows[:n]


def correlations_by_group(group_rollups: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    by_g: Dict[str, List[Dict[str, str]]] = {}
    for r in group_rollups:
        gk = r.get("group_key", "")
        if not gk:
            continue
        by_g.setdefault(gk, []).append(r)

    def corr_status(xs: List[float], ys: List[float]) -> str:
        if len(xs) < 3 or len(ys) < 3:
            return "insufficient windows (<3)"
        if len(set(xs)) <= 1 or len(set(ys)) <= 1:
            return "constant series"
        return "ok"

    out: List[Dict[str, Any]] = []

    for gk, rs in by_g.items():
        rs = sorted(rs, key=lambda r: r.get("window_start", ""))

        elections = [to_float(r.get("elections", "0")) for r in rs]
        leader_changes = [to_float(r.get("leader_intervals_started", "0")) for r in rs]
        tcp_timeouts = [to_float(r.get("tcp_connect_timeouts", "0")) for r in rs]
        prev_rej = [to_float(r.get("pre_vote_rejections", "0")) for r in rs]
        susp = [to_float(r.get("cluster_suspicions", "0")) for r in rs]
        append_fail = [to_float(r.get("append_failures", "0")) for r in rs]
        inv_to = [to_float(r.get("invocation_timeouts", "0")) for r in rs]

        c1 = pearson(elections, tcp_timeouts)
        c2 = pearson(elections, prev_rej)
        c3 = pearson(leader_changes, susp)
        c4 = pearson(elections, append_fail)
        c5 = pearson(elections, inv_to)

        s1 = corr_status(elections, tcp_timeouts)
        s2 = corr_status(elections, prev_rej)
        s3 = corr_status(leader_changes, susp)
        statuses = {s1, s2, s3}
        overall = "ok" if "ok" in statuses else ", ".join(sorted(statuses))

        out.append(
            {
                "group_key": gk,
                "windows": len(rs),
                "sum_elections": int(sum(elections)),
                "sum_tcp_timeouts": int(sum(tcp_timeouts)),
                "sum_prevote_rej": int(sum(prev_rej)),
                "sum_suspicions": int(sum(susp)),
                "corr_elections_tcp_timeouts": "" if c1 is None else f"{c1:.3f}",
                "corr_elections_prevote_rej": "" if c2 is None else f"{c2:.3f}",
                "corr_leader_changes_suspicions": "" if c3 is None else f"{c3:.3f}",
                "corr_elections_append_fail": "" if c4 is None else f"{c4:.3f}",
                "corr_elections_invoc_timeouts": "" if c5 is None else f"{c5:.3f}",
                "corr_status": overall,
            }
        )

    def sort_key(r: Dict[str, Any]) -> Tuple[int, int, int]:
        ok_rank = 0 if r["corr_status"] == "ok" else 1
        return (ok_rank, -int(r["sum_elections"]), -int(r["windows"]))

    out.sort(key=sort_key)
    return out


def top_nodes(node_rollups: List[Dict[str, str]], n: int = 20) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in node_rollups:
        rows.append(
            {
                "window_start": r.get("window_start", ""),
                "window_end": r.get("window_end", ""),
                "node_uuid": r.get("node_uuid", ""),
                "node_addr": r.get("node_addr", ""),
                "node_risk_score": to_int(r.get("node_risk_score", "0")),
                "was_suspected": to_int(r.get("was_suspected", "0")),
                "tcp_connect_timeouts": to_int(r.get("tcp_connect_timeouts", "0")),
                "tcp_disconnects": to_int(r.get("tcp_disconnects", "0")),
                "votes_rejected": to_int(r.get("votes_rejected", "0")),
                "pre_vote_rejections": to_int(r.get("pre_vote_rejections", "0")),
                "follower_behind_events": to_int(r.get("follower_behind_events", "0")),
                "invocation_timeouts": to_int(r.get("invocation_timeouts", "0")),
                "leadership_time_ms": to_int(r.get("leadership_time_ms", "0")),
            }
        )
    rows.sort(key=lambda x: x["node_risk_score"], reverse=True)
    return rows[:n]
