# hzcp/extract/parse.py

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..io.fs import iter_worker_logs
from ..model.events import Event
from . import regexes as rx

def make_event_id(parts: List[str]) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()


def canonical_group_key(gid: str) -> str:
    if not gid:
        return ""

    m = rx.CP_GROUP_ID_RE.match(gid)
    if m:
        return m.group("name")

    if "(" in gid and gid.endswith(")"):
        name = gid[: gid.rfind("(")]
        seed = gid[gid.rfind("(") + 1 : -1]
        if seed == name:
            return name
        return name

    return gid


def split_group_id(gid: str) -> Tuple[str, str]:
    if "(" in gid and gid.endswith(")"):
        name = gid[: gid.rfind("(")]
        seed = gid[gid.rfind("(") + 1 : -1]
        if seed == name:
            return name, ""
        return name, seed
    return gid, ""


def parse_dt(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str)


def parse_ts(line: str, base_date: Optional[str]) -> Tuple[Optional[datetime], str]:
    m = rx.TS_RE.match(line)
    if not m:
        return None, ""

    date_part = m.group("date")
    time_part = m.group("time")

    if date_part:
        ts_raw = m.group("ts")
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(ts_raw, fmt), "log_line"
            except ValueError:
                pass
        return None, ""

    if base_date:
        try:
            return datetime.strptime(f"{base_date} {time_part}", "%Y-%m-%d %H:%M:%S.%f"), "base_date"
        except ValueError:
            return None, ""

    t = datetime.strptime(time_part, "%H:%M:%S.%f")
    return datetime(1970, 1, 1, t.hour, t.minute, t.second, t.microsecond), "anchored_time_only"


def group_from_logger(logger: str) -> str:
    m = rx.LOGGER_GROUP_SUFFIX_RE.search(logger)
    return m.group("gname") if m else ""


def addr(ip: str, port: str) -> str:
    return f"{ip}:{port}"


def _derive_seat_from_worker_dir(worker_dir: str) -> Tuple[str, str]:
    """
    Best-effort fallback only.
    Expected examples:
      A1_W1-18.132.45.35-member -> ("A1_W1", "18.132.45.35")
    """
    if not worker_dir or not worker_dir.endswith("-member") or "-" not in worker_dir:
        return "", ""

    label, rest = worker_dir.split("-", 1)
    if not _looks_like_label(label):
        return "", ""

    pub = rest[: -len("-member")] if rest.endswith("-member") else ""
    if "-" in pub:
        pub = pub.split("-")[-1]

    if pub and not _looks_like_ipv4(pub):
        pub = ""

    return label, pub


def parse_all_events(
    root: Path, base_date: Optional[str], *, quiet: bool = False
) -> Tuple[List[Event], Dict[str, str], datetime]:
    events: List[Event] = []
    uuid_by_addr: Dict[str, str] = {}
    last_seen: Optional[datetime] = None

    for path in iter_worker_logs(root):
        worker_dir = path.parent.name

        # Only worker seats (prevents parsing non-seat logs as seats)
        if not worker_dir.endswith("-member"):
            continue

        if not quiet:
            print(f"processing: {path}")

        # ---- observer / "from my seat" identity (per worker.log) ----
        # IMPORTANT: label/public must come from banner lines in the file.
        observer_label = ""
        observer_private_addr = ""
        observer_public_addr = ""
        observer_cp_priority = ""

        # ---- per-file timestamp rollover handling (time-only logs) ----
        rollover_days = 0
        prev_raw: Optional[datetime] = None

        # Most recent parsed header fields
        last_ts: Optional[datetime] = None
        last_ts_source = ""
        last_thread = ""
        last_level = ""
        last_logger = ""
        last_actor_addr = ""
        last_group_name = ""

        # CP snapshot block state
        in_cp = False
        cp_lines: List[str] = []
        cp_meta: Optional[Tuple[str, int, int, int, datetime, str, int, str, str, str, str]] = None

        def update_last_ts(ts: datetime, ts_source: str) -> None:
            nonlocal last_ts, last_ts_source, last_seen, rollover_days, prev_raw
            if ts_source == "anchored_time_only":
                if prev_raw and ts < prev_raw:
                    rollover_days += 1
                prev_raw = ts
                ts = ts + timedelta(days=rollover_days)

            last_ts = ts
            last_ts_source = ts_source
            if last_seen is None or ts > last_seen:
                last_seen = ts

        def emit(
            event_type: str,
            msg: str,
            lineno: int,
            *,
            group_id: str = "",
            group_name: str = "",
            group_seed: str = "",
            term: str = "",
            log_index: str = "",
            cp_member_count: str = "",
            node_uuid: str = "",
            node_addr: str = "",
            peer_uuid: str = "",
            peer_addr: str = "",
            candidate_uuid: str = "",
            candidate_addr: str = "",
            voter_uuid: str = "",
            voter_addr: str = "",
            vote_granted: str = "",
            reason: str = "",
            timeout_ms: str = "",
            snapshot_bytes: str = "",
            extra_1: str = "",
            extra_2: str = "",
        ) -> None:
            if not last_ts:
                return

            effective_src = group_id or (last_group_name or "") or ""
            effective_group_key = canonical_group_key(effective_src)

            if group_id:
                derived_name, derived_seed = split_group_id(group_id)
            else:
                derived_name, derived_seed = effective_group_key, ""

            events.append(
                Event(
                    event_id=make_event_id([str(path), str(lineno), event_type, msg, last_ts.isoformat()]),
                    ts=last_ts.isoformat(sep=" "),
                    ts_source=last_ts_source,
                    event_type=event_type,
                    group_key=effective_group_key,
                    group_id=group_id,
                    group_name=(group_name or derived_name),
                    group_seed=(group_seed or derived_seed),
                    term=term,
                    log_index=log_index,
                    cp_member_count=cp_member_count,
                    observer_label=observer_label,
                    observer_private_addr=observer_private_addr,
                    observer_public_addr=observer_public_addr,
                    observer_cp_priority=observer_cp_priority,
                    node_uuid=node_uuid,
                    node_addr=node_addr,
                    peer_uuid=peer_uuid,
                    peer_addr=peer_addr,
                    candidate_uuid=candidate_uuid,
                    candidate_addr=candidate_addr,
                    voter_uuid=voter_uuid,
                    voter_addr=voter_addr,
                    vote_granted=vote_granted,
                    reason=reason,
                    timeout_ms=timeout_ms,
                    snapshot_bytes=snapshot_bytes,
                    extra_1=extra_1,
                    extra_2=extra_2,
                    source_file=str(path),
                    source_line=str(lineno),
                    thread=last_thread,
                    level=last_level,
                    logger=last_logger,
                    message=msg,
                )
            )

        def commit_cp_block() -> None:
            nonlocal in_cp, cp_lines, cp_meta
            if not in_cp or not cp_meta:
                in_cp = False
                cp_lines = []
                cp_meta = None
                return

            gid, size, term_i, log_index_i, block_ts, block_ts_source, src_line, thread, level, logger, actor_addr = cp_meta
            gname, gseed = split_group_id(gid)

            leader_uuid = ""
            leader_addr = ""

            for l in cp_lines:
                mm = rx.CP_MEMBER_RE.search(l.strip())
                if not mm:
                    continue

                u = mm.group("uuid")
                a = f"{mm.group('ip')}:{mm.group('port')}"
                role = mm.group("role") or ""

                uuid_by_addr.setdefault(a, u)

                events.append(
                    Event(
                        event_id=make_event_id([str(path), str(src_line), gid, u, role, block_ts.isoformat()]),
                        ts=block_ts.isoformat(sep=" "),
                        ts_source=block_ts_source,
                        event_type="role_observed",
                        group_key=canonical_group_key(gid),
                        group_id=gid,
                        group_name=gname,
                        group_seed=gseed,
                        term=str(term_i),
                        log_index=str(log_index_i),
                        cp_member_count=str(size),
                        observer_label=observer_label,
                        observer_private_addr=observer_private_addr,
                        observer_public_addr=observer_public_addr,
                        observer_cp_priority=observer_cp_priority,
                        node_uuid=u,
                        node_addr=a,
                        source_file=str(path),
                        source_line=str(src_line),
                        thread=thread,
                        level=level,
                        logger=logger,
                        message=role,
                    )
                )

                if role == "LEADER":
                    leader_uuid = u
                    leader_addr = a

            events.append(
                Event(
                    event_id=make_event_id([str(path), str(src_line), gid, "cp_snapshot", block_ts.isoformat()]),
                    ts=block_ts.isoformat(sep=" "),
                    ts_source=block_ts_source,
                    event_type="cp_snapshot",
                    group_key=canonical_group_key(gid),
                    group_id=gid,
                    group_name=gname,
                    group_seed=gseed,
                    term=str(term_i),
                    log_index=str(log_index_i),
                    cp_member_count=str(size),
                    observer_label=observer_label,
                    observer_private_addr=observer_private_addr,
                    observer_public_addr=observer_public_addr,
                    observer_cp_priority=observer_cp_priority,
                    node_uuid=uuid_by_addr.get(actor_addr, ""),
                    node_addr=actor_addr,
                    peer_uuid=leader_uuid,
                    peer_addr=leader_addr,
                    source_file=str(path),
                    source_line=str(src_line),
                    thread=thread,
                    level=level,
                    logger=logger,
                    message="CP Group Members snapshot",
                )
            )

            in_cp = False
            cp_lines = []
            cp_meta = None

        with path.open("r", errors="replace") as f:
            for lineno, line in enumerate(f, start=1):
                if in_cp and rx.TS_RE.match(line):
                    commit_cp_block()

                # ---- seat identity parsing (from FILE via regexes.py) ----
                # Match:
                #   Worker - Public address: 18.132.45.35
                m = rx.SIM_PUBLIC_ADDR_RE.search(line)
                if m:
                    observer_public_addr = m.group("public")

                # Match:
                #   Server - Successfully started server for A1_W1
                m = rx.SIM_LABEL_RE.search(line)
                if m:
                    observer_label = m.group("label")

                # Match:
                #   HazelcastUtils - Setting CP member priority to 100 for agent 172.31.88.126
                m = rx.SIM_CP_PRIORITY_RE.search(line)
                if m:
                    prio = m.group("priority")
                    priv = m.group("private")
                    observer_private_addr = priv
                    observer_cp_priority = prio

                # ---- standard header parsing ----
                hm = rx.HEADER_RE.match(line)
                if hm:
                    ts, ts_source = parse_ts(line, base_date)
                    if ts:
                        update_last_ts(ts, ts_source)

                    last_thread = hm.group("thread")
                    last_level = hm.group("level")
                    last_logger = hm.group("logger")
                    last_actor_addr = addr(hm.group("actor_ip"), hm.group("actor_port"))
                    last_group_name = group_from_logger(last_logger)
                else:
                    # fallback: timestamp only
                    ts, ts_source = parse_ts(line, base_date)
                    if ts:
                        update_last_ts(ts, ts_source)

                # ---- CP snapshot block handling ----
                if in_cp:
                    cp_lines.append(line)
                    if rx.END_BRACKET_RE.match(line):
                        commit_cp_block()
                    continue

                if not last_ts:
                    continue

                msg = line.rstrip("\n")

                # CP snapshot starts
                mg = rx.CP_GROUP_RE.search(line)
                if mg:
                    gid = mg.group("gid")
                    size = int(mg.group("size"))
                    term_i = int(mg.group("term"))
                    log_index_i = int(mg.group("logIndex"))

                    in_cp = True
                    cp_lines = []
                    cp_meta = (
                        gid,
                        size,
                        term_i,
                        log_index_i,
                        last_ts,
                        last_ts_source,
                        lineno,
                        last_thread,
                        last_level,
                        last_logger,
                        last_actor_addr,
                    )
                    continue

                # Learn uuid mapping from suspicion / autoremove if present
                ms = rx.CLUSTER_SUSPECT_RE.search(line)
                if ms:
                    target_addr = addr(ms.group("ip"), ms.group("port"))
                    target_uuid = ms.group("uuid")
                    uuid_by_addr.setdefault(target_addr, target_uuid)
                    emit(
                        "member_suspected_cluster",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        peer_uuid=target_uuid,
                        peer_addr=target_addr,
                        reason=ms.group("reason").strip(),
                    )
                    continue

                ma = rx.CP_AUTOREMOVE_RE.search(line)
                if ma:
                    target_addr = addr(ma.group("ip"), ma.group("port"))
                    target_uuid = ma.group("uuid")
                    uuid_by_addr.setdefault(target_addr, target_uuid)
                    emit(
                        "cp_member_missing_autoremove",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        peer_uuid=target_uuid,
                        peer_addr=target_addr,
                        extra_1=ma.group("sec"),
                    )
                    continue

                if rx.LEADERSHIP_REBALANCE_SKIPPED_RE.search(line):
                    emit(
                        "leadership_rebalance_skipped",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                    )
                    continue

                # TCP network events
                mc = rx.TCP_CONN_CLOSED_RE.search(line)
                if mc:
                    emit(
                        "tcp_conn_closed",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        peer_uuid=mc.group("ruuid"),
                        peer_addr=mc.group("remote"),
                        reason=mc.group("reason").strip(),
                        extra_1=mc.group("local"),
                    )
                    continue

                mconn = rx.TCP_CONNECTING_RE.search(line)
                if mconn:
                    emit(
                        "tcp_connecting",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        peer_addr=mconn.group("remote").strip(),
                        timeout_ms=mconn.group("timeout"),
                    )
                    continue

                if rx.TCP_CONNECT_TIMEOUT_RE.search(line):
                    emit(
                        "tcp_connect_timeout",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                    )
                    continue

                # Leader signals
                ml = rx.LEADER_SET_RE.search(line)
                if ml:
                    emit(
                        "leader_set",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        peer_uuid=ml.group("uuid"),
                    )
                    continue

                if rx.WE_ARE_LEADER_RE.search(line):
                    emit(
                        "we_are_leader",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        peer_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        peer_addr=last_actor_addr,
                    )
                    continue

                # Vote / PreVote signals
                mgv = rx.VOTE_GRANTED_RE.search(line)
                if mgv:
                    emit(
                        "vote_granted",
                        msg,
                        lineno,
                        term=mgv.group("term"),
                        candidate_uuid=mgv.group("uuid"),
                        voter_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        voter_addr=last_actor_addr,
                        vote_granted="true",
                    )
                    continue

                mrv = rx.VOTE_REJECTED_RE.search(line)
                if mrv:
                    emit(
                        "vote_rejected",
                        msg,
                        lineno,
                        term=mrv.group("term"),
                        candidate_uuid=mrv.group("uuid"),
                        voter_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        voter_addr=last_actor_addr,
                        vote_granted="false",
                    )
                    continue

                mpreq = rx.PRE_VOTE_REQ_RE.search(line)
                if mpreq:
                    emit(
                        "pre_vote_request",
                        msg,
                        lineno,
                        term=mpreq.group("term"),
                        candidate_uuid=mpreq.group("uuid"),
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                        extra_1=mpreq.group("lli"),
                    )
                    continue

                mprej = rx.PRE_VOTE_REJECT_RE.search(line)
                if mprej:
                    emit(
                        "pre_vote_rejected",
                        msg,
                        lineno,
                        term=mprej.group("term"),
                        candidate_uuid=mprej.group("uuid"),
                        voter_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        voter_addr=last_actor_addr,
                        vote_granted="false",
                        reason=mprej.group("reason").strip(),
                    )
                    continue

                if rx.PRE_VOTE_IGNORED_RE.search(line):
                    emit(
                        "pre_vote_ignored",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                    )
                    continue

                mtm = rx.TERM_MOVE_RE.search(line)
                if mtm:
                    emit(
                        "term_moved",
                        msg,
                        lineno,
                        candidate_uuid=mtm.group("cand"),
                        term=mtm.group("new"),
                        extra_1=f"old={mtm.group('old')}",
                        extra_2=f"lastLogIndex={mtm.group('lli')}",
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                    )
                    continue

                if rx.ELECTION_TIMEOUT_RE.search(line):
                    emit(
                        "election_timeout",
                        msg,
                        lineno,
                        node_uuid=uuid_by_addr.get(last_actor_addr, ""),
                        node_addr=last_actor_addr,
                    )
                    continue

                # Append / lag / snapshot / invocation
                if rx.APPEND_REJECT_RE.search(line):
                    emit("append_rejected", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.APPEND_TIMEOUT_RE.search(line):
                    emit("append_timeout", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.FOLLOWER_BEHIND_RE.search(line):
                    emit("follower_behind", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.SNAPSHOT_INSTALL_RE.search(line):
                    emit("snapshot_installing", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.SNAPSHOT_SEND_RE.search(line):
                    emit("snapshot_sending", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.INVOC_RETRY_RE.search(line):
                    emit("invocation_retry", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.INVOC_TIMEOUT_RE.search(line):
                    emit("invocation_timeout", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.INVOC_REPLACED_RE.search(line):
                    emit("invocation_replaced", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

                if rx.MEMBERS_REPLACED_RE.search(line):
                    emit("members_container_replaced", msg, lineno, node_uuid=uuid_by_addr.get(last_actor_addr, ""), node_addr=last_actor_addr)
                    continue

        # End-of-file: commit any open CP block
        if in_cp:
            commit_cp_block()

    if last_seen is None:
        last_seen = datetime(1970, 1, 1)

    return events, uuid_by_addr, last_seen
