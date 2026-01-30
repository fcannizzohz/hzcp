
from __future__ import annotations

import re

# timestamps
TS_RE = re.compile(r"^(?P<ts>(?:(?P<date>\d{4}-\d{2}-\d{2})[ T])?(?P<time>\d{2}:\d{2}:\d{2}\.\d{3}))")

# Parse the "standard" Hazelcast log header lines
HEADER_RE = re.compile(
    r"^(?P<ts>(?:(?P<date>\d{4}-\d{2}-\d{2})[ T])?(?P<time>\d{2}:\d{2}:\d{2}\.\d{3}))\s+"
    r"\[(?P<thread>[^\]]+)\]\s+"
    r"(?P<level>[A-Z]+)\s+"
    r"(?P<logger>\S+)\s+-\s+"
    r"\[(?P<actor_ip>\d+\.\d+\.\d+\.\d+)\]:(?P<actor_port>\d+)\s+"
    r"(?P<rest>.*)$"
)

LOGGER_GROUP_SUFFIX_RE = re.compile(r"\((?P<gname>METADATA|cpgroup-\d+)\)")
END_BRACKET_RE = re.compile(r"^\s*\]\s*$")

# CP snapshot header
CP_GROUP_RE = re.compile(
    r"CP Group Members\s*\{groupId:\s*(?P<gid>[A-Za-z0-9_.-]+\(\d+\))\s*,\s*size:(?P<size>\d+)\s*,\s*term:(?P<term>\d+)\s*,\s*logIndex:(?P<logIndex>\d+)\}"
)

CP_GROUP_ID_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9_.-]+)\((?P<seed>\d+)\)$"
)

# CPMember line (role optional)
CP_MEMBER_RE = re.compile(
    r"CPMember\{uuid=(?P<uuid>[0-9a-fA-F-]+),\s*address=\[(?P<ip>[^\]]+)\]:(?P<port>\d+)\}(?:\s*-\s*(?P<role>LEADER|FOLLOWER).*)?$"
)

# Leader signals
LEADER_SET_RE = re.compile(r"Setting leader:\s*RaftEndpoint\{uuid='(?P<uuid>[0-9a-fA-F-]+)'\}")
WE_ARE_LEADER_RE = re.compile(r"We are the LEADER!", re.IGNORECASE)

# Vote / PreVote
VOTE_GRANTED_RE = re.compile(
    r"Granted vote for VoteRequest\{candidate=RaftEndpoint\{uuid='(?P<uuid>[0-9a-fA-F-]+)'\}.*term=(?P<term>\d+)"
)
VOTE_REJECTED_RE = re.compile(
    r"Rejected vote for VoteRequest\{candidate=RaftEndpoint\{uuid='(?P<uuid>[0-9a-fA-F-]+)'\}.*term=(?P<term>\d+)"
)

PRE_VOTE_REQ_RE = re.compile(
    r"PreVoteRequest\{candidate=RaftEndpoint\{uuid='(?P<uuid>[0-9a-fA-F-]+)'\}.*term=(?P<term>\d+).*lastLogIndex=(?P<lli>\d+)"
)
PRE_VOTE_REJECT_RE = re.compile(
    r"Rejecting PreVoteResponse for PreVoteRequest\{candidate=RaftEndpoint\{uuid='(?P<uuid>[0-9a-fA-F-]+)'\}.*term=(?P<term>\d+).*?\}\s*since\s*(?P<reason>.*)$"
)
PRE_VOTE_IGNORED_RE = re.compile(r"Ignoring PreVoteResponse.*not follower anymore", re.IGNORECASE)

TERM_MOVE_RE = re.compile(
    r"Moving to new term:\s*(?P<new>\d+)\s*from current term:\s*(?P<old>\d+).*candidate=RaftEndpoint\{uuid='(?P<cand>[0-9a-fA-F-]+)'\}.*lastLogIndex=(?P<lli>\d+)"
)

ELECTION_TIMEOUT_RE = re.compile(r"(Election timed out|Not enough votes|Retrying election)", re.IGNORECASE)

# Append / lag / snapshot (generic)
APPEND_REJECT_RE = re.compile(r"Append.*rejected", re.IGNORECASE)
APPEND_TIMEOUT_RE = re.compile(r"Append.*timeout", re.IGNORECASE)
FOLLOWER_BEHIND_RE = re.compile(r"(Follower is behind|is behind)", re.IGNORECASE)
SNAPSHOT_INSTALL_RE = re.compile(r"Installing snapshot", re.IGNORECASE)
SNAPSHOT_SEND_RE = re.compile(r"Sending snapshot", re.IGNORECASE)

# Invocation manager
INVOC_RETRY_RE = re.compile(r"Retry(ing)? .*Raft invocation", re.IGNORECASE)
INVOC_TIMEOUT_RE = re.compile(r"(Raft invocation.*timed out|Invocation timed out)", re.IGNORECASE)
INVOC_REPLACED_RE = re.compile(r"Replaced .*RaftInvocation", re.IGNORECASE)

# Membership/CP members container
MEMBERS_REPLACED_RE = re.compile(r"Replaced\s+CPMembersContainer", re.IGNORECASE)

# Cluster suspicion
CLUSTER_SUSPECT_RE = re.compile(
    r"Member\s+\[(?P<ip>\d+\.\d+\.\d+\.\d+)\]:(?P<port>\d+)\s+-\s+(?P<uuid>[0-9a-fA-F-]+)\s+is suspected to be dead for reason:\s*(?P<reason>.*)$",
    re.IGNORECASE,
)

# CP auto-removal scheduling
CP_AUTOREMOVE_RE = re.compile(
    r"CPMember\{uuid=(?P<uuid>[0-9a-fA-F-]+),\s*address=\[(?P<ip>[^\]]+)\]:(?P<port>\d+)\}.*auto-removed.*after\s+(?P<sec>\d+)\s+seconds",
    re.IGNORECASE,
)

LEADERSHIP_REBALANCE_SKIPPED_RE = re.compile(r"leadership rebalancing.*skipped.*MemberLeftException", re.IGNORECASE)

# TCP transport lines
TCP_CONN_CLOSED_RE = re.compile(
    r"TcpServerConnection\{.*?localAddress=(?P<local>[^,}]+).*?remoteAddress=(?P<remote>[^,}]+).*?remoteUuid=(?P<ruuid>[0-9a-fA-F-]+).*?\} closed\. Reason:\s*(?P<reason>.*)$",
    re.IGNORECASE,
)
TCP_CONNECTING_RE = re.compile(
    r"Connecting to:\s*(?P<remote>[^,]+),\s*timeout\s*ms:\s*(?P<timeout>\d+)",
    re.IGNORECASE,
)
TCP_CONNECT_TIMEOUT_RE = re.compile(r"Connect timed out", re.IGNORECASE)

# hzcp/extract/regexes.py

# Matches: "Connecting to /172.31.88.35:5701, timeout: 10000, bind-any: true"
TCP_CONNECTING_RE = re.compile(
    r"Connecting to\s+(?P<remote>/\d+\.\d+\.\d+\.\d+:\d+),\s*timeout:\s*(?P<timeout>\d+)",
    re.IGNORECASE,
)

# Matches: "Could not connect to: /172.31.88.35:5701. Reason: IOException[Connection refused ...]"
TCP_CONNECT_FAILED_RE = re.compile(
    r"Could not connect to:\s*(?P<remote>/\d+\.\d+\.\d+\.\d+:\d+)\.\s*Reason:\s*(?P<reason>.*)$",
    re.IGNORECASE,
)

# Matches: "Removing connection to endpoint [172.31.88.35]:5701 ... Error-Count: 5"
TCP_REMOVE_CONN_RE = re.compile(
    r"Removing connection to endpoint\s+\[(?P<remote>\d+\.\d+\.\d+\.\d+)\]:(?P<port>\d+)\s+Cause\s*=>\s*(?P<cause>.*),\s*Error-Count:\s*(?P<count>\d+)",
    re.IGNORECASE,
)

# Worker identity / seat

SIM_CP_PRIORITY_RE = re.compile(
    r"Setting CP member priority to\s*(?P<priority>\d+)\s*for agent\s*(?P<private>\d{1,3}(?:\.\d{1,3}){3})\b"
)

SIM_PUBLIC_ADDR_RE = re.compile(
    r"Worker\s*-\s*Public address:\s*(?P<public>\d{1,3}(?:\.\d{1,3}){3})\b"
)

SIM_LABEL_RE = re.compile(
    r"Server\s*-\s*Successfully started server for\s*(?P<label>[A]\d+_W\d+)\b"
)

