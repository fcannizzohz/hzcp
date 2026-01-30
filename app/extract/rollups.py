from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from ..model.events import Event
from .parse import parse_dt


def floor_window(ts: datetime, window_seconds: int) -> datetime:
    epoch = int(ts.timestamp())
    start_epoch = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(start_epoch)


def compute_rollups(events: List[Event], intervals: List[dict], window_seconds: int) -> Tuple[List[dict], List[dict]]:
    # group rollups keyed by (window_start, window_end, group_key)
    group_counts: Dict[Tuple[datetime, datetime, str], Dict[str, int]] = {}

    def gkey(ts: datetime, gk: str) -> Tuple[datetime, datetime, str]:
        ws = floor_window(ts, window_seconds)
        return ws, ws + timedelta(seconds=window_seconds), gk

    def inc(d: Dict[str, int], k: str, n: int = 1) -> None:
        d[k] = d.get(k, 0) + n

    for e in events:
        if not e.group_key:
            continue
        ts = parse_dt(e.ts)
        key = gkey(ts, e.group_key)
        group_counts.setdefault(key, {})

        et = e.event_type
        if et in ("leader_set",):
            inc(group_counts[key], "elections")
        if et in ("we_are_leader",):
            inc(group_counts[key], "we_are_leader")
        if et in ("vote_rejected",):
            inc(group_counts[key], "vote_rejections")
        if et in ("election_timeout",):
            inc(group_counts[key], "vote_timeouts")
        if et in ("append_rejected", "append_timeout"):
            inc(group_counts[key], "append_failures")
        if et in ("invocation_retry",):
            inc(group_counts[key], "invocation_retries")
        if et in ("invocation_timeout",):
            inc(group_counts[key], "invocation_timeouts")
        if et in ("members_container_replaced",):
            inc(group_counts[key], "membership_changes")
        if et in ("member_suspected_cluster",):
            inc(group_counts[key], "cluster_suspicions")
        if et in ("cp_member_missing_autoremove",):
            inc(group_counts[key], "cp_autoremove_scheduled")
            if e.extra_1.isdigit():
                group_counts[key]["cp_autoremove_seconds_sum"] = group_counts[key].get("cp_autoremove_seconds_sum", 0) + int(e.extra_1)
        if et in ("pre_vote_request",):
            inc(group_counts[key], "pre_vote_requests")
        if et in ("pre_vote_rejected",):
            inc(group_counts[key], "pre_vote_rejections")
        if et in ("pre_vote_ignored",):
            inc(group_counts[key], "pre_vote_ignored")
        if et in ("term_moved",):
            inc(group_counts[key], "term_moves")
        if et in ("snapshot_installing",):
            inc(group_counts[key], "snapshots_installed")
        if et in ("tcp_conn_closed",):
            inc(group_counts[key], "tcp_disconnects")
        if et in ("tcp_connecting",):
            inc(group_counts[key], "tcp_connect_attempts")
        if et in ("tcp_connect_timeout",):
            inc(group_counts[key], "tcp_connect_timeouts")

    # leader_changes + tenure stats from intervals (interval starts within window)
    tenure_by_wg: Dict[Tuple[datetime, datetime, str], List[int]] = {}
    for it in intervals:
        gk = it["group_key"]
        ts = parse_dt(it["start_ts"])
        ws = floor_window(ts, window_seconds)
        key = (ws, ws + timedelta(seconds=window_seconds), gk)
        group_counts.setdefault(key, {})
        tenure_by_wg.setdefault(key, [])
        inc(group_counts[key], "leader_intervals_started")
        tenure_by_wg[key].append(int(it["duration_ms"]))

    group_rows: List[dict] = []
    for (ws, we, gk), counts in sorted(group_counts.items(), key=lambda x: (x[0][0], x[0][2])):
        ten = tenure_by_wg.get((ws, we, gk), [])

        mean_ten = ""
        p95_ten = ""
        if ten:
            mean_ten = str(int(sum(ten) / len(ten)))
            ten_sorted = sorted(ten)
            idx = max(0, math.ceil(0.95 * len(ten_sorted)) - 1)
            p95_ten = str(ten_sorted[idx])

        network_instability_index = (
            counts.get("append_failures", 0)
            + counts.get("vote_timeouts", 0)
            + counts.get("invocation_retries", 0)
            + counts.get("cluster_suspicions", 0)
            + counts.get("tcp_disconnects", 0)
            + counts.get("tcp_connect_timeouts", 0)
            + counts.get("pre_vote_rejections", 0)
        )

        denom = 1 + counts.get("elections", 0) + counts.get("membership_changes", 0) + counts.get("vote_rejections", 0)
        mean_ten_ms = int(mean_ten) if mean_ten else 0
        cp_stability_index = (mean_ten_ms / 1000.0) / denom

        group_rows.append(
            {
                "window_start": ws.isoformat(sep=" "),
                "window_end": we.isoformat(sep=" "),
                "group_key": gk,
                "elections": str(counts.get("elections", 0)),
                "leader_intervals_started": str(counts.get("leader_intervals_started", 0)),
                "we_are_leader": str(counts.get("we_are_leader", 0)),
                "mean_leader_tenure_ms": mean_ten,
                "p95_leader_tenure_ms": p95_ten,
                "append_failures": str(counts.get("append_failures", 0)),
                "vote_rejections": str(counts.get("vote_rejections", 0)),
                "vote_timeouts": str(counts.get("vote_timeouts", 0)),
                "invocation_retries": str(counts.get("invocation_retries", 0)),
                "invocation_timeouts": str(counts.get("invocation_timeouts", 0)),
                "membership_changes": str(counts.get("membership_changes", 0)),
                "cluster_suspicions": str(counts.get("cluster_suspicions", 0)),
                "cp_autoremove_scheduled": str(counts.get("cp_autoremove_scheduled", 0)),
                "cp_autoremove_seconds_sum": str(counts.get("cp_autoremove_seconds_sum", 0)),
                "pre_vote_requests": str(counts.get("pre_vote_requests", 0)),
                "pre_vote_rejections": str(counts.get("pre_vote_rejections", 0)),
                "pre_vote_ignored": str(counts.get("pre_vote_ignored", 0)),
                "term_moves": str(counts.get("term_moves", 0)),
                "snapshots_installed": str(counts.get("snapshots_installed", 0)),
                "tcp_disconnects": str(counts.get("tcp_disconnects", 0)),
                "tcp_connect_attempts": str(counts.get("tcp_connect_attempts", 0)),
                "tcp_connect_timeouts": str(counts.get("tcp_connect_timeouts", 0)),
                "network_instability_index": str(network_instability_index),
                "cp_stability_index": f"{cp_stability_index:.6f}",
            }
        )

    # node rollups keyed by (window_start, window_end, node_uuid_or_addr)
    node_counts: Dict[Tuple[datetime, datetime, str, str], Dict[str, int]] = {}

    def nkey(ws: datetime, we: datetime, uuid: str, addr_: str) -> Tuple[datetime, datetime, str, str]:
        return ws, we, uuid, addr_

    # leadership_time_ms from interval overlaps
    for it in intervals:
        leader_uuid = it["leader_uuid"] or ""
        leader_addr = it["leader_addr"] or ""
        start = parse_dt(it["start_ts"])
        end = parse_dt(it["end_ts"])
        cur = start
        while cur < end:
            ws = floor_window(cur, window_seconds)
            we = ws + timedelta(seconds=window_seconds)
            seg_end = min(end, we)
            ms = int((seg_end - cur).total_seconds() * 1000)
            key = nkey(ws, we, leader_uuid, leader_addr)
            node_counts.setdefault(key, {})
            node_counts[key]["leadership_time_ms"] = node_counts[key].get("leadership_time_ms", 0) + ms
            cur = seg_end

    for e in events:
        ts = parse_dt(e.ts)
        ws = floor_window(ts, window_seconds)
        we = ws + timedelta(seconds=window_seconds)

        uuid = e.node_uuid or e.voter_uuid or ""
        addr_ = e.node_addr or e.voter_addr or ""
        if not uuid and not addr_:
            continue

        key = nkey(ws, we, uuid, addr_)
        node_counts.setdefault(key, {})

        def ninc(k: str, n: int = 1) -> None:
            node_counts[key][k] = node_counts[key].get(k, 0) + n

        if e.event_type == "vote_granted":
            ninc("votes_granted")
        if e.event_type == "vote_rejected":
            ninc("votes_rejected")
        if e.event_type == "pre_vote_rejected":
            ninc("pre_vote_rejections")
        if e.event_type == "invocation_retry":
            ninc("invocation_retries")
        if e.event_type == "invocation_timeout":
            ninc("invocation_timeouts")
        if e.event_type == "follower_behind":
            ninc("follower_behind_events")
        if e.event_type == "snapshot_installing":
            ninc("snapshots_installed")
        if e.event_type == "member_suspected_cluster":
            ninc("suspecting_others")
        if e.event_type == "tcp_conn_closed":
            ninc("tcp_disconnects")
        if e.event_type == "tcp_connect_timeout":
            ninc("tcp_connect_timeouts")

        if e.event_type == "member_suspected_cluster" and (e.peer_uuid or e.peer_addr):
            tkey = nkey(ws, we, e.peer_uuid, e.peer_addr)
            node_counts.setdefault(tkey, {})
            node_counts[tkey]["was_suspected"] = node_counts[tkey].get("was_suspected", 0) + 1

    node_rows: List[dict] = []
    for (ws, we, uuid, addr_), counts in sorted(node_counts.items(), key=lambda x: (x[0][0], x[0][2], x[0][3])):
        node_risk_score = (
            counts.get("votes_rejected", 0)
            + counts.get("pre_vote_rejections", 0)
            + counts.get("follower_behind_events", 0)
            + counts.get("invocation_timeouts", 0)
            + counts.get("tcp_connect_timeouts", 0)
            + counts.get("was_suspected", 0)
        )
        asymmetry_score = (
            (counts.get("follower_behind_events", 0) + counts.get("votes_rejected", 0) + counts.get("tcp_disconnects", 0) + counts.get("was_suspected", 0))
            - (counts.get("leadership_time_ms", 0) / 60000.0)
        )

        node_rows.append(
            {
                "window_start": ws.isoformat(sep=" "),
                "window_end": we.isoformat(sep=" "),
                "node_uuid": uuid,
                "node_addr": addr_,
                "leadership_time_ms": str(counts.get("leadership_time_ms", 0)),
                "votes_granted": str(counts.get("votes_granted", 0)),
                "votes_rejected": str(counts.get("votes_rejected", 0)),
                "pre_vote_rejections": str(counts.get("pre_vote_rejections", 0)),
                "follower_behind_events": str(counts.get("follower_behind_events", 0)),
                "snapshots_installed": str(counts.get("snapshots_installed", 0)),
                "invocation_retries": str(counts.get("invocation_retries", 0)),
                "invocation_timeouts": str(counts.get("invocation_timeouts", 0)),
                "suspecting_others": str(counts.get("suspecting_others", 0)),
                "was_suspected": str(counts.get("was_suspected", 0)),
                "tcp_disconnects": str(counts.get("tcp_disconnects", 0)),
                "tcp_connect_timeouts": str(counts.get("tcp_connect_timeouts", 0)),
                "node_risk_score": str(node_risk_score),
                "asymmetry_score": f"{asymmetry_score:.3f}",
            }
        )

    return group_rows, node_rows


