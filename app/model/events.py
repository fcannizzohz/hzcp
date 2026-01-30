from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class Event:
    # cp_events.csv schema (canonical fact table)
    event_id: str
    ts: str
    ts_source: str
    event_type: str

    # grouping
    group_key: str = ""
    group_id: str = ""
    group_name: str = ""
    group_seed: str = ""

    # CP metadata
    term: str = ""
    log_index: str = ""
    cp_member_count: str = ""

    # observer / "from my seat" identity (the worker.log being parsed)
    observer_label: str = ""
    observer_private_addr: str = ""   # e.g. 172.31.88.126
    observer_public_addr: str = ""    # e.g. 18.132.45.35
    observer_cp_priority: str = ""    # e.g. 100

    # actor / peer roles
    node_uuid: str = ""
    node_addr: str = ""

    peer_uuid: str = ""
    peer_addr: str = ""

    candidate_uuid: str = ""
    candidate_addr: str = ""

    voter_uuid: str = ""
    voter_addr: str = ""

    vote_granted: str = ""
    reason: str = ""

    timeout_ms: str = ""
    snapshot_bytes: str = ""
    extra_1: str = ""
    extra_2: str = ""

    # provenance
    source_file: str = ""
    source_line: str = ""

    # raw log context
    thread: str = ""
    level: str = ""
    logger: str = ""
    message: str = ""

    @classmethod
    def csv_header(cls) -> List[str]:
        # Single authoritative CSV header for cp_events.csv
        return list(cls.__annotations__.keys())