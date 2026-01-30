# hzcp/report/nodes.py

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Dict, List, Tuple

import pandas as pd


@dataclass(frozen=True)
class ObserverSeat:
    label: str
    private_ip: str
    public_ip: str
    cp_priority: str


def _split_seat(extra_1: str, extra_2: str) -> ObserverSeat:
    """
    extra_1: observer_label
    extra_2: "{private_ip}|{public_ip}|{priority}"
    """
    label = (extra_1 or "").strip()
    raw = (extra_2 or "").strip()
    parts = raw.split("|") if raw else []
    private_ip = parts[0].strip() if len(parts) > 0 else ""
    public_ip = parts[1].strip() if len(parts) > 1 else ""
    cp_priority = parts[2].strip() if len(parts) > 2 else ""
    return ObserverSeat(label=label, private_ip=private_ip, public_ip=public_ip, cp_priority=cp_priority)


def _seat_key(seat: ObserverSeat) -> str:
    # label is usually stable; fallback to IPs if label missing
    if seat.label:
        return seat.label
    return f"{seat.private_ip}|{seat.public_ip}"


def _as_iso(s: pd.Series) -> str:
    # ts column is already string; keep it, but handle empty
    if s.empty:
        return ""
    return str(s.iloc[0])


def build_observer_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      observer_label, observer_private_ip, observer_public_ip, observer_cp_priority
    from extra_1/extra_2.
    """
    out = df.copy()

    def _extract(row) -> Tuple[str, str, str, str]:
        seat = _split_seat(row.get("extra_1", ""), row.get("extra_2", ""))
        return seat.label, seat.private_ip, seat.public_ip, seat.cp_priority

    cols = out.apply(_extract, axis=1, result_type="expand")
    cols.columns = ["observer_label", "observer_private_ip", "observer_public_ip", "observer_cp_priority"]
    out = pd.concat([out, cols], axis=1)

    # Normalise group_key empties to "(none)" so pivots don’t drop them
    out["group_key"] = out["group_key"].fillna("").astype(str)
    out.loc[out["group_key"].str.strip() == "", "group_key"] = "(none)"
    out["event_type"] = out["event_type"].fillna("").astype(str)

    return out


def render_nodes_section(events: pd.DataFrame) -> str:
    """
    Returns an HTML section containing:
      1) Nodes inventory (one row per observer_label)
      2) From-my-seat pivots (observer_label x group_key x event_type)
      3) “Seat health” highlights (network/connectivity + CP elections) per seat
    """
    if events.empty:
        return "<h2>Nodes</h2><p>No events.</p>"

    df = build_observer_columns(events)

    # Inventory: unique observer_label rows, with first/last ts in that seat’s logs
    inv = (
        df.groupby("observer_label", dropna=False)
        .agg(
            observer_private_ip=("observer_private_ip", "first"),
            observer_public_ip=("observer_public_ip", "first"),
            observer_cp_priority=("observer_cp_priority", "first"),
            first_ts=("ts", "min"),
            last_ts=("ts", "max"),
            events=("event_id", "count"),
            files=("source_file", pd.Series.nunique),
        )
        .reset_index()
        .sort_values(["observer_label"])
    )

    # Pivot counts: observer_label, group_key, event_type
    pivot = (
        df.groupby(["observer_label", "group_key", "event_type"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["observer_label", "group_key", "count"], ascending=[True, True, False])
    )

    # “Seat health” quick signals
    # You can tune this list as you add event_types.
    interesting = {
        "member_suspected_cluster": "suspected peers",
        "cp_member_missing_autoremove": "auto-removes",
        "tcp_connect_timeout": "tcp connect timeouts",
        "tcp_conn_closed": "tcp conn closed",
        "tcp_connecting": "tcp connecting",
        "election_timeout": "election timeouts",
        "pre_vote_rejected": "pre-vote rejected",
        "vote_rejected": "vote rejected",
        "append_timeout": "append timeouts",
        "invocation_timeout": "invocation timeouts",
        "invocation_retry": "invocation retries",
    }

    health_rows: List[Tuple[str, str, int]] = []
    for et, label in interesting.items():
        sub = df[df["event_type"] == et]
        if sub.empty:
            continue
        counts = sub.groupby("observer_label").size()
        for obs, c in counts.items():
            health_rows.append((str(obs), label, int(c)))

    health = pd.DataFrame(health_rows, columns=["observer_label", "signal", "count"])
    if not health.empty:
        health = health.sort_values(["observer_label", "count"], ascending=[True, False])

    # ---- HTML render helpers (simple, no templates assumed) ----
    def table_html(frame: pd.DataFrame, caption: str) -> str:
        if frame.empty:
            return f"<h3>{escape(caption)}</h3><p>No data.</p>"
        cols = list(frame.columns)
        head = "".join(f"<th>{escape(str(c))}</th>" for c in cols)
        body_rows = []
        for _, r in frame.iterrows():
            tds = "".join(f"<td>{escape(str(r[c]))}</td>" for c in cols)
            body_rows.append(f"<tr>{tds}</tr>")
        body = "\n".join(body_rows)
        return f"""
<h3>{escape(caption)}</h3>
<table class="hz-table">
  <thead><tr>{head}</tr></thead>
  <tbody>
    {body}
  </tbody>
</table>
""".strip()

    # Also add a per-seat “from my seat” breakdown (top-N rows) to be readable
    per_seat_blocks: List[str] = []
    for seat, sub in pivot.groupby("observer_label", dropna=False):
        top = sub.head(50)  # cap
        per_seat_blocks.append(
            f"<details><summary><b>{escape(str(seat))}</b> – from my seat (top 50)</summary>"
            + table_html(top, "Counts by (group_key, event_type)")
            + "</details>"
        )

    html = [
        "<h2>Nodes</h2>",
        "<p>Node identity is extracted from each worker.log: observer_label (extra_1), and private/public/priority (extra_2).</p>",
        table_html(inv, "Nodes inventory"),
        table_html(health, "Seat health signals (counts)") if not health.empty else "<h3>Seat health signals</h3><p>No signals.</p>",
        "<h3>From my seat</h3>",
        "<p>Counts grouped by (observer_label, group_key, event_type).</p>",
        *per_seat_blocks,
    ]
    return "\n".join(html)