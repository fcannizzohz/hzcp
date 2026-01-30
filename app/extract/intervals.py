from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..model.events import Event
from .parse import parse_dt, make_event_id


def build_leader_timeline(events: List[Event]) -> Dict[str, List[Tuple[datetime, str, str]]]:
    """
    Build per-group_key timeline points (ts, leader_uuid, leader_addr) from:
      - cp_snapshot with explicit leader (peer_uuid present)
      - we_are_leader
      - leader_set (uuid only, addr may be filled later)
    """
    by_group: Dict[str, List[Tuple[datetime, str, str]]] = {}

    for e in events:
        if not e.group_key:
            continue

        ts = parse_dt(e.ts)

        if e.event_type == "cp_snapshot" and e.peer_uuid:
            by_group.setdefault(e.group_key, []).append((ts, e.peer_uuid, e.peer_addr))
        elif e.event_type == "we_are_leader":
            leader_uuid = e.peer_uuid or e.node_uuid
            leader_addr = e.peer_addr or e.node_addr
            if leader_addr or leader_uuid:
                by_group.setdefault(e.group_key, []).append((ts, leader_uuid, leader_addr))
        elif e.event_type == "leader_set":
            if e.peer_uuid:
                by_group.setdefault(e.group_key, []).append((ts, e.peer_uuid, ""))

    # sort & collapse consecutive duplicates (by uuid)
    for gk, pts in list(by_group.items()):
        pts_sorted = sorted(pts, key=lambda x: x[0])
        collapsed: List[Tuple[datetime, str, str]] = []
        last_uuid = None
        for t, u, a in pts_sorted:
            if u and u != last_uuid:
                collapsed.append((t, u, a))
                last_uuid = u
        by_group[gk] = collapsed

    return by_group


def compute_intervals(events: List[Event], end_ts: datetime) -> List[dict]:
    timeline = build_leader_timeline(events)
    intervals: List[dict] = []

    snaps_by_group: Dict[str, List[Event]] = {}
    for e in events:
        if e.event_type == "cp_snapshot" and e.group_key:
            snaps_by_group.setdefault(e.group_key, []).append(e)
    for gk in snaps_by_group:
        snaps_by_group[gk].sort(key=lambda e: parse_dt(e.ts))

    # build uuid->addr map from snapshots (bugfix improvement: fill leader_addr when missing)
    addr_by_uuid: Dict[str, str] = {}
    for e in events:
        if e.event_type == "role_observed" and e.node_uuid and e.node_addr:
            addr_by_uuid.setdefault(e.node_uuid, e.node_addr)

    def nearest_snapshot(gk: str, t: datetime) -> Optional[Event]:
        snaps = snaps_by_group.get(gk, [])
        if not snaps:
            return None
        best = None
        best_dt = None
        for s in snaps:
            sd = parse_dt(s.ts)
            d = abs((sd - t).total_seconds())
            if best is None or d < best_dt:
                best = s
                best_dt = d
        return best

    for gk, pts in timeline.items():
        if not pts:
            continue
        for i, (start_t, leader_uuid, leader_addr) in enumerate(pts):
            end_t = pts[i + 1][0] if i + 1 < len(pts) else end_ts
            if end_t < start_t:
                continue
            dur_ms = int((end_t - start_t).total_seconds() * 1000)

            snap = nearest_snapshot(gk, start_t)
            group_id = snap.group_id if snap else ""
            group_name = snap.group_name if snap else (gk if gk else "")
            term_start = snap.term if snap else ""
            log_start = snap.log_index if snap else ""

            if not leader_addr and leader_uuid:
                leader_addr = addr_by_uuid.get(leader_uuid, "")

            intervals.append(
                {
                    "interval_id": make_event_id([gk, leader_uuid, start_t.isoformat()]),
                    "group_key": gk,
                    "group_id": group_id,
                    "group_name": group_name,
                    "leader_uuid": leader_uuid,
                    "leader_addr": leader_addr,
                    "start_ts": start_t.isoformat(sep=" "),
                    "end_ts": end_t.isoformat(sep=" "),
                    "duration_ms": str(dur_ms),
                    "term_start": term_start,
                    "start_log_index": log_start,
                }
            )

    return intervals

