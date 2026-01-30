from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, DefaultDict
from collections import defaultdict
from typing import Any, Dict, List
import hashlib

from .charts import svg_bar_labeled
from .html_assets import CSS, JS
from .insights import (
    Paths,
    esc,
    to_int,
    load_all,
    summarize_event_types,
    leader_stats,
    top_bad_windows,
    correlations_by_group,
    top_nodes,
)

def stable_id(title: str) -> str:
    return hashlib.sha1(title.encode("utf-8")).hexdigest()[:8]

def parse_observer_seat(r: Dict[str, str]) -> Dict[str, str]:

    label = (r.get("observer_label") or "").strip()
    private_ip = (r.get("observer_private_addr") or "").strip()
    public_ip = (r.get("observer_public_addr") or "").strip()
    priority = (r.get("observer_cp_priority") or "").strip()

    # label fallback if missing (keeps grouping stable-ish)
    if not label:
        label = f"{private_ip or '?'}->{public_ip or '?'}"

    return {
        "observer_label": label,
        "observer_private_ip": private_ip,
        "observer_public_ip": public_ip,
        "observer_cp_priority": priority,
    }


def build_nodes_inventory(events: List[Dict[str, str]]) -> List[List[Any]]:
    """
    One row per observer_label.
    Columns:
      observer_label, private_ip, public_ip, cp_priority, first_ts, last_ts, events, files
    """
    by_label: Dict[str, Dict[str, Any]] = {}

    for r in events:
        seat = parse_observer_seat(r)
        label = seat["observer_label"]

        ts = r.get("ts") or ""
        src = r.get("source_file") or ""

        acc = by_label.get(label)
        if not acc:
            acc = {
                "observer_label": label,
                "observer_private_ip": seat["observer_private_ip"],
                "observer_public_ip": seat["observer_public_ip"],
                "observer_cp_priority": seat["observer_cp_priority"],
                "first_ts": ts,
                "last_ts": ts,
                "events": 0,
                "files_set": set(),
            }
            by_label[label] = acc

        # prefer first non-empty identity fields
        if not acc["observer_private_ip"] and seat["observer_private_ip"]:
            acc["observer_private_ip"] = seat["observer_private_ip"]
        if not acc["observer_public_ip"] and seat["observer_public_ip"]:
            acc["observer_public_ip"] = seat["observer_public_ip"]
        if not acc["observer_cp_priority"] and seat["observer_cp_priority"]:
            acc["observer_cp_priority"] = seat["observer_cp_priority"]

        if ts:
            if not acc["first_ts"] or ts < acc["first_ts"]:
                acc["first_ts"] = ts
            if not acc["last_ts"] or ts > acc["last_ts"]:
                acc["last_ts"] = ts

        acc["events"] += 1
        if src:
            acc["files_set"].add(src)

    rows: List[List[Any]] = []
    for label, acc in sorted(by_label.items(), key=lambda kv: kv[0]):
        rows.append(
            [
                acc["observer_label"],
                acc["observer_private_ip"],
                acc["observer_public_ip"],
                acc["observer_cp_priority"],
                acc["first_ts"],
                acc["last_ts"],
                acc["events"],
                len(acc["files_set"]),
            ]
        )
    return rows

def build_from_my_seat_compact(
    events: List[Dict[str, str]], *, max_members: int | None = None
) -> List[Dict[str, Any]]:
    """
    Build a compact "from my seat" view.

    Output is a list of tables (one per observer_label), each table is:

      {
        "observer_label": "A1_W1",
        "headers": ["event_type", "<group1>", "<group2>", ..., "<groupN>", "TOTAL"],
        "rows": [
            ["tcp_connecting", 12, 0, 3, ..., 15],
            ["cp_snapshot",    2, 1, 0, ..., 3],
            ...
        ],
      }

    Where each row is an event_type, and each group column is the count of that
    event_type for that group_key. "(none)" is used for missing group_key.
    """
    # counts[label][event_type][group_key] = count
    counts: DefaultDict[str, DefaultDict[str, DefaultDict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )

    # also track which groups exist per label
    groups_by_label: DefaultDict[str, set[str]] = defaultdict(set)

    for r in events:
        seat = parse_observer_seat(r)
        label = (seat.get("observer_label") or "").strip() or "(unknown)"

        gk = (r.get("group_key") or "").strip() or "(none)"
        et = (r.get("event_type") or "").strip() or "(unknown-event-type)"

        counts[label][et][gk] += 1
        groups_by_label[label].add(gk)

    labels = sorted(counts.keys())
    if max_members is not None:
        labels = labels[:max_members]

    out: List[Dict[str, Any]] = []

    for label in labels:
        # stable column order: "(none)" first, then lexical for the rest
        groups = sorted(groups_by_label[label], key=lambda g: (g != "(none)", g))

        # compute per-event totals to sort rows by dominance
        event_totals: List[Tuple[str, int]] = []
        for et, by_group in counts[label].items():
            total = sum(by_group.values())
            event_totals.append((et, total))

        # rows: most common event types first
        event_totals.sort(key=lambda t: (-t[1], t[0]))

        headers = ["event_type", *groups, "TOTAL"]
        rows: List[List[Any]] = []

        for et, total in event_totals:
            by_group = counts[label][et]
            row = [et]
            for g in groups:
                row.append(by_group.get(g, 0))
            row.append(total)
            rows.append(row)

        out.append(
            {
                "observer_label": label,
                "headers": headers,
                "rows": rows,
            }
        )

    return out

def table_html(
    title: str,
    description: str,
    how_to_use: List[str],
    columns: List[str],
    rows: List[List[Any]],
) -> str:
    tid = "t_" + stable_id(title)

    ths = "".join(f'<th onclick="sortTable(\'{tid}\',{i})">{esc(c)}</th>' for i, c in enumerate(columns))

    trs = []
    for r in rows:
        tds = "".join(f"<td>{esc(v)}</td>" for v in r)
        trs.append(f"<tr>{tds}</tr>")

    how_html = "".join(f"<li>{esc(x)}</li>" for x in how_to_use)

    return f"""
    <section>
      <h2>{esc(title)}</h2>
      <div class="desc">{esc(description)}</div>
      <div class="use">
        <div class="use-title">How to use</div>
        <ul>{how_html}</ul>
      </div>
      <div class="note">Click a column header to sort. Copy rows into a spreadsheet for deeper analysis.</div>
      <div class="table-wrap">
        <table id="{tid}">
          <thead><tr>{ths}</tr></thead>
          <tbody>
            {''.join(trs)}
          </tbody>
        </table>
      </div>
    </section>
    """

def section_html_block(title, description, bullets, body_html):
    return f"""
    <section>
      <h2>{title}</h2>
      <p>{description}</p>
      <ul>
        {''.join(f"<li>{b}</li>" for b in bullets)}
      </ul>
      {body_html}
    </section>
    """

def build_html(in_dir_str: str, paths: Paths) -> str:
    events, intervals, rg, rn = load_all(paths)

    et = summarize_event_types(events)
    gstats, lstats = leader_stats(intervals)
    badw = top_bad_windows(rg)
    corr = correlations_by_group(rg)
    topn = top_nodes(rn)

    total_events = len(events)
    total_intervals = len(intervals)
    unique_groups = len({r.get("group_key") or r.get("group_id") for r in intervals if (r.get("group_key") or r.get("group_id"))})
    unique_nodes = len({(r.get("node_uuid"), r.get("node_addr")) for r in rn if r.get("node_uuid") or r.get("node_addr")})

    def sum_col(rows: List[Dict[str, str]], col: str) -> int:
        return sum(to_int(r.get(col, "0")) for r in rows)

    net_total = {
        "tcp_timeouts": sum_col(rg, "tcp_connect_timeouts"),
        "tcp_disconnects": sum_col(rg, "tcp_disconnects"),
        "pre_vote_rej": sum_col(rg, "pre_vote_rejections"),
        "suspicions": sum_col(rg, "cluster_suspicions"),
        "autoremove": sum_col(rg, "cp_autoremove_scheduled"),
        "elections": sum_col(rg, "elections"),
    }

    chart_event_types = svg_bar_labeled(
        [(k, float(v)) for k, v in et[:30]],
        width=1280,
        height=260,
        max_bars=24,
        show_values=True,
        show_x_labels=True,
        x_label_max=18,
    )

    chart_bad_groups = svg_bar_labeled(
        [(r["group_key"], float(r["leader_intervals_started"])) for r in gstats[:30]],
        width=1280,
        height=260,
        max_bars=12,
        show_values=True,
        show_x_labels=True,
        x_label_max=18,
    )

    et_rows_top = [[k, v] for k, v in et[:80]]
    g_rows_top = [[r["group_key"], r["leader_intervals_started"], r["distinct_leaders"], r["total_min"], r["mean_s"], r["p95_s"], r["churn_per_hr"]] for r in gstats[:80]]
    l_rows = [[r["leader_uuid"], r["total_min"], r["share"], r["groups"], r["intervals"]] for r in lstats[:80]]
    bw_rows = [[r["window_start"], r["window_end"], r["group_key"], r["network_instability_index"], r["tcp_connect_timeouts"], r["tcp_disconnects"], r["pre_vote_rejections"], r["cluster_suspicions"], r["elections"], r["leader_intervals_started"], r["cp_autoremove_scheduled"]] for r in badw]
    corr_rows = [[r["group_key"], r["windows"], r["sum_elections"], r["sum_tcp_timeouts"], r["sum_prevote_rej"], r["sum_suspicions"], r["corr_elections_tcp_timeouts"], r["corr_elections_prevote_rej"], r["corr_leader_changes_suspicions"], r["corr_elections_append_fail"], r["corr_elections_invoc_timeouts"], r["corr_status"]] for r in corr[:200]]
    node_rows = [[r["window_start"], r["window_end"], r["node_uuid"], r["node_addr"], r["node_risk_score"], r["was_suspected"], r["tcp_connect_timeouts"], r["tcp_disconnects"], r["votes_rejected"], r["pre_vote_rejections"], r["follower_behind_events"], r["invocation_timeouts"], round(r["leadership_time_ms"] / 60000.0, 2)] for r in topn]
    nodes_inv_rows = build_nodes_inventory(events)

    from_seat_tables = build_from_my_seat_compact(events)

    from_seat_tables_html = []
    for t in from_seat_tables:
        from_seat_tables_html.append(
            table_html(
                f"Seat: {t['observer_label']}",
                None,  # no per-table description
                [],
                t["headers"],
                t["rows"],
            )
        )

    from_seat_section_html = section_html_block(
        title="From my seat: event counts by member",
        description=(
            "Each table represents what a single member observed. "
            "Rows are event_type, columns are group_key, values are counts."
        ),
        bullets=[
            "Compare the same event_type across seats to spot asymmetric visibility.",
            "Look at TOTAL to see what dominated each member’s view.",
            "'(none)' group means the log line had no CP group attribution.",
        ],
        body_html="\n".join(from_seat_tables_html),
    )

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Hazelcast CP report</title>
<style>{CSS}</style>
<script>{JS}</script>
</head>
<body>
<header>
  <h1>Hazelcast CP subsystem report</h1>
  <small>Input CSVs: {esc(in_dir_str)}</small>
</header>
<main>

<section>
  <h2>Overview</h2>
  <div class="desc">
    High-level counts to sanity-check that parsing worked and to quickly spot runs with lots of instability.
    Treat these as “what happened” counters, then drill down with the tables below.
  </div>
  <div class="use">
    <div class="use-title">How to use</div>
    <ul>
      <li>If <b>tcp connect timeouts</b> / <b>pre-vote rejections</b> / <b>cluster suspicions</b> spike, focus on network and reachability first.</li>
      <li>If <b>elections</b> spike without network-ish signals, look for CPU/GC/thread starvation and disk pauses.</li>
      <li>If <b>CP auto-remove scheduled</b> increases, CP membership is drifting behind cluster membership.</li>
    </ul>
  </div>
  <div class="kpis">
    <div class="kpi"><div class="v">{total_events}</div><div class="l">raw events</div></div>
    <div class="kpi"><div class="v">{total_intervals}</div><div class="l">leader intervals</div></div>
    <div class="kpi"><div class="v">{unique_groups}</div><div class="l">groups observed</div></div>
    <div class="kpi"><div class="v">{unique_nodes}</div><div class="l">nodes observed</div></div>
    <div class="kpi"><div class="v">{net_total["elections"]}</div><div class="l">elections (rollups)</div></div>
    <div class="kpi"><div class="v">{net_total["tcp_timeouts"]}</div><div class="l">tcp connect timeouts</div></div>
    <div class="kpi"><div class="v">{net_total["tcp_disconnects"]}</div><div class="l">tcp disconnects</div></div>
    <div class="kpi"><div class="v">{net_total["pre_vote_rej"]}</div><div class="l">pre-vote rejections</div></div>
    <div class="kpi"><div class="v">{net_total["suspicions"]}</div><div class="l">cluster suspicions</div></div>
    <div class="kpi"><div class="v">{net_total["autoremove"]}</div><div class="l">CP auto-remove scheduled</div></div>
  </div>
</section>


<section>
  <details>
    <summary>
      <h2 style="display:inline">Event types: what they mean and why they matter</h2>
    </summary>

    <div class="desc">
      Quick glossary of extracted event types. Use this to interpret charts and “from my seat” tables.
    </div>

    <div class="use">
      <div class="use-title">How to use</div>
      <ul>
        <li>Start with <b>cp_snapshot</b> and <b>role_observed</b> to understand intended CP membership.</li>
        <li>Use <b>vote_*</b>, <b>pre_vote_*</b>, <b>term_moved</b>, and <b>election_timeout</b> to explain leader churn.</li>
        <li>Correlate <b>tcp_*</b> with timeouts and rejections to separate network issues from overload.</li>
      </ul>
    </div>

    <table class="tbl">
      <thead>
        <tr>
          <th>event_type</th>
          <th>Meaning</th>
          <th>Why it matters</th>
          <th>Common interpretation / next checks</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><code>cp_snapshot</code></td>
          <td>Observed CP group membership snapshot.</td>
          <td>Ground truth of who is in the group at that moment.</td>
          <td>Divergence across seats implies partial visibility or log gaps.</td>
        </tr>

        <tr>
          <td><code>role_observed</code></td>
          <td>Member role seen inside a snapshot (LEADER/FOLLOWER).</td>
          <td>Used to reconstruct leader history.</td>
          <td>Rapid role changes indicate churn.</td>
        </tr>

        <tr>
          <td><code>leader_set</code></td>
          <td>Leader UUID set or observed.</td>
          <td>Signals leadership transitions.</td>
          <td>High frequency = instability.</td>
        </tr>

        <tr>
          <td><code>we_are_leader</code></td>
          <td>Node claims leadership.</td>
          <td>Strong leader transition signal.</td>
          <td>Multiple seats claiming ≈ split or jitter.</td>
        </tr>

        <tr>
          <td><code>vote_granted</code></td>
          <td>Vote granted to candidate.</td>
          <td>Normal during elections, bad if constant.</td>
          <td>Correlate with network and lag signals.</td>
        </tr>

        <tr>
          <td><code>vote_rejected</code></td>
          <td>Vote explicitly rejected.</td>
          <td>Clear election failure evidence.</td>
          <td>Check rejection reasons and connectivity.</td>
        </tr>

        <tr>
          <td><code>pre_vote_rejected</code></td>
          <td>Pre-vote rejected with reason.</td>
          <td>Most actionable election diagnostic.</td>
          <td>Often points to lagging logs or unreachable majority.</td>
        </tr>

        <tr>
          <td><code>term_moved</code></td>
          <td>Raft term advanced.</td>
          <td>Tracks election churn.</td>
          <td>Frequent moves without progress = instability.</td>
        </tr>

        <tr>
          <td><code>election_timeout</code></td>
          <td>Election did not complete in time.</td>
          <td>Direct liveness failure signal.</td>
          <td>Check GC, CPU, and network.</td>
        </tr>

        <tr>
          <td><code>tcp_connect_timeout</code></td>
          <td>TCP connection attempt timed out.</td>
          <td>Strong network-path indicator.</td>
          <td>If paired with vote failures, blame network first.</td>
        </tr>

        <tr>
          <td><code>invocation_timeout</code></td>
          <td>Operation invocation timed out.</td>
          <td>System not making progress.</td>
          <td>Correlate with GC pauses and executor saturation.</td>
        </tr>
      </tbody>
    </table>
  </details>
</section>

<section>
  <h2>Event types (top)</h2>
  <div class="desc">
    A fingerprint of what dominated the run. The chart shows the top event types (counts printed on bars;
    hover bars for full label). The table below is sortable and shows more rows.
  </div>
  <div class="use">
    <div class="use-title">How to use</div>
    <ul>
      <li>If <b>tcp_connect_timeout</b> and <b>pre_vote_rejected</b> dominate, bias toward network jitter / partial connectivity.</li>
      <li>If <b>invocation_timeout</b> dominates, correlate with leader churn and follower lag windows.</li>
      <li>If a crucial type is missing entirely, it usually means log wording changed and the extractor needs a regex update.</li>
    </ul>
  </div>
  <div class="chart">{chart_event_types}</div>
</section>

{table_html(
  "Event types (table)",
  "Sortable view of event type counts. Same data as the chart, but not capped to top N.",
  [
    "Sort by count to identify the main failure mode (network-ish vs CP churn vs membership drift).",
    "Use this to compare runs: top 5 event types should be stable for the same issue class.",
    "If a type looks suspiciously low, check whether the extractor is missing that log wording in this run.",
  ],
  ["event_type", "count"],
  et_rows_top,
)}

<section>
  <h2>Most churny groups (top)</h2>
  <div class="desc">
    Groups with frequent leader turnover. Bar labels show leader_changes; hover bars for full group names.
    Use the table to sort by churn_per_hr (best single “instability” signal here).
  </div>
  <div class="use">
    <div class="use-title">How to use</div>
    <ul>
      <li>Start with the top 1–3 groups and drill into their “Worst windows”.</li>
      <li>If <b>METADATA</b> churns, expect broad impact; if only user cpgroup-* churns, impact can be localised.</li>
      <li>If churn is high but network signals are low, suspect slow node behaviour (CPU/GC/disk) rather than partitions.</li>
    </ul>
  </div>
  <div class="chart">{chart_bad_groups}</div>
</section>

{table_html(
  "Most churny groups (table)",
  "Sortable stability statistics per group_key derived from leader intervals.",
  [
    "Sort by churn_per_hr to find groups that can’t hold leadership.",
    "Compare distinct_leaders vs leader_changes to spot whether churn is wide (many nodes) or narrow (few nodes flipping).",
    "Cross-check group_key in “Per-group correlations” to see if churn lines up with tcp/pre-vote/suspicions.",
  ],
  ["group_key", "leader_intervals_started", "distinct_leaders", "total_min", "mean_interval_s", "p95_interval_s", "churn_per_hr"],
  g_rows_top,
)}

{table_html(
  "Leader share (all groups combined)",
  "Total leader time per leader_uuid summed across all CP groups. This is about skew and concentration, not wall-clock leadership.",
  [
    "If one UUID dominates, check for overload/hotspot or other nodes being unreliable.",
    "If leadership is evenly spread but churn is high, suspect group-level partitions rather than a single bad node.",
    "Use this to pick candidates for deeper per-node inspection (CPU/GC/network).",
  ],
  ["leader_uuid", "total_min", "share", "groups", "intervals"],
  l_rows,
)}

{table_html(
  "Worst windows by network_instability_index",
  "Top time windows with the highest composite network-instability score (timeouts, disconnects, pre-vote rejections, suspicions, etc.).",
  [
    "Start here when users report 'it was bad around X'. These windows are the shortest path to root cause.",
    "If net_idx is high and elections/leader_changes are also high, it’s usually network-driven CP churn.",
    "If net_idx is high but elections are low, you may have network issues hurting clients without forcing elections.",
  ],
  ["window_start","window_end","group_key","net_idx","tcp_timeouts","tcp_disconnects","pre_vote_rej","suspicions","elections","leader_intervals_started","cp_autoremove"],
  bw_rows,
)}

{table_html(
  "Per-group correlations (Pearson, windowed) — event totals",
  "Window totals per group. Use this to sanity-check volume before interpreting correlations.",
  [
    "If windows < 3, correlations will be blank/undefined.",
    "If totals are near-zero for everything, you’re correlating noise.",
  ],
  ["group_key","windows","sum_elections","sum_tcp_timeouts","sum_prevote_rej","sum_suspicions"],
  [[
      r[0],  # group_key
      r[1],  # windows
      r[2],  # sum_elections
      r[3],  # sum_tcp_timeouts
      r[4],  # sum_prevote_rej
      r[5],  # sum_suspicions
    ] for r in corr_rows],
)}

{table_html(
  "Per-group correlations (Pearson, windowed) — correlation coefficients",
  "Pearson correlation coefficients per group. Some rows may be blank: Pearson needs at least 3 windows and non-constant series.",
  [
    "Use corr(*) only when correlation_status is 'ok'.",
    "If 'insufficient windows (<3)', rerun with a smaller rollup window (or a longer run).",
    "If 'constant series', that metric didn’t vary in those windows, so Pearson is undefined.",
  ],
  ["group_key",
   "corr(elections,tcp_timeouts)","corr(elections,pre_vote_rej)","corr(leader_changes,suspicions)",
   "corr(elections,append_fail)","corr(elections,invoc_timeouts)","correlation_status"],
  [[
      r[0],   # group_key
      r[6],   # corr(elections,tcp_timeouts)
      r[7],   # corr(elections,pre_vote_rej)
      r[8],   # corr(leader_changes,suspicions)
      r[9],   # corr(elections,append_fail)
      r[10],  # corr(elections,invoc_timeouts)
      r[11],  # correlation_status
    ] for r in corr_rows],
)}

{table_html(
  "Top risky nodes (windowed)",
  "Nodes ranked by a simple additive risk score (rejections, behind events, invocation timeouts, tcp timeouts, being suspected). It’s a shortlist, not a verdict.",
  [
    "Pick the top nodes and see whether they appear across many windows (chronic) or just a spike (incident).",
    "High was_suspected + tcp_timeouts usually points to connectivity/AZ routing/host issues.",
    "High behind + invocation_timeouts with low tcp symptoms usually points to CPU/GC or disk IO (node is 'slow', not 'disconnected').",
  ],
  ["window_start","window_end","node_uuid","node_addr","risk","was_suspected","tcp_timeouts","tcp_disconnects","votes_rej","pre_vote_rej","behind","invoc_timeouts","leadership_min"],
  node_rows,
)}

{table_html(
  "Nodes inventory (observer seats)",
  "One row per worker/member log: label + private/public IP + CP priority + coverage window. "
  "This is the basis for any 'from my seat' analysis.",
  [
    "If a seat has blank private/public/priority, your extractor did not see the simulator banner lines (log format change or truncation).",
    "Compare first_ts/last_ts to ensure all seats cover the same run window (missing tail/head can skew counts).",
    "If cp_priority differs across seats, leadership skew may be expected rather than a fault.",
  ],
  ["observer_label","private_ip","public_ip","cp_priority","first_ts","last_ts","events","files"],
  nodes_inv_rows,
)}

{from_seat_section_html}

<section>
  <h2>Next improvements</h2>
  <div class="desc">
    A few upgrades that increase accuracy without turning this into a full observability platform.
  </div>
  <div class="use">
    <div class="use-title">How to use</div>
    <ul>
      <li><b>Vote RTT</b>: if logs contain paired request/response lines, compute p50/p95 latency per node pair.</li>
      <li><b>Peer attribution</b>: if Append/behind/timeout lines name the remote peer, parse it to build a node→node “bad link matrix”.</li>
      <li><b>Reason parsing</b>: vote rejection reasons are often the fastest way to prove network vs slow-node behaviour.</li>
    </ul>
  </div>
</section>

</main>
</body>
</html>
"""
