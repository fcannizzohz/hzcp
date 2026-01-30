# hzcp

Hazelcast CP log extractor and HTML report generator.

This tool parses Hazelcast Simulator `worker.log` files, extracts CP-related events, produces structured CSV fact tables, and renders a single self-contained HTML report for analysis.

The focus is **CP behaviour as observed by each node (“from my seat”)**, including elections, leadership changes, suspicions, timeouts, and correlations with network-style failures.

## What this tool is for

- Analysing Hazelcast CP behaviour from **real logs**, not inferred state
- Understanding **what each node actually observed**
- Correlating elections and leadership churn with network symptoms
- Producing a **portable HTML report** with no external dependencies

This is not a generic log parser. It is opinionated, CP-specific, and evidence-driven.

## Input assumptions

- Logs come from **Hazelcast Simulator workers**
- Files are named `worker.log`
- Logs live under directories ending in `-member`
- Worker identity is derived from:
  - Simulator startup lines in the log
  - Directory naming (fallback only)

If log formats change, regexes must be updated.

## Output

### CSVs

Written during extraction:

- `cp_events.csv`  
  Canonical fact table. One row per observed CP-related event.

- Additional derived CSVs (windowed rollups, correlations, summaries)

These CSVs are the **source of truth** for the report.

### HTML report

- Single HTML file
- Inline CSS
- Inline JS
- No external assets
- Designed for sharing and archiving

## Installation

Install it in other environments (pick one)

### From a local checkout (editable for dev):

```bash
pip install -e .
```

### From a git URL (good for servers/CI):

```bash
pip install "git+ssh://git@github.com/ORG/REPO.git@main"
```

(or git+https://...)

### Build a wheel and copy it anywhere (most portable offline):

```bash
python -m build
pip install dist/hzcp-0.1.0-py3-none-any.whl
```

## How to use

The entrypoint is `hzcp` with three subcommands:

* `extract`: parse logs and write CSVs
* `report`: generate HTML from CSVs
* `all`: run extract then report

### Basic example

```bash
hzcp all --in ./runs/your_test_name/your_run 
```

parses the files in that run directory and generates report in the same directory.

### Common flags

All subcommands share flags added by `_add_common_flags(...)`.
Refer to the source for the authoritative list.

### `hzcp extract`

Parse `worker.log` files and write `cp_*.csv`.

**Arguments**

* `--in <DIR>` **(required)**
  Root directory to scan recursively for `worker.log` files.

* `--out <DIR>` *(optional)*
  Directory to write CSVs. Defaults to `--in`.

* `--base-date YYYY-MM-DD` *(optional)*
  Anchor date for time-only timestamps.

* `--window-seconds <INT>` *(optional, default: 60)*
  Rollup window size used for aggregations.

**Example**

```bash
hzcp extract \
  --in ./runs/basic-large-cpmap-test/26-01-2026_15-27-49 \
  --out ./out \
  --base-date 2026-01-30 \
  --window-seconds 60
```

### `hzcp report`

Generate an HTML report from existing CSVs.

**Arguments**

* `--in <DIR>` **(required)**
  Directory containing `cp_*.csv` files.

* `--out <DIR>` *(optional)*
  Directory to write the report. Defaults to `--in`.

* `--name <FILE>` *(optional, default: `cp-report.html`)*
  Output file name.

**Example**

```bash
hzcp report \
  --in ./out \
  --out ./out \
  --name cp-report.html
```

### `hzcp all`

Run extraction and report generation in one step.

**Arguments**

* `--in <DIR>` **(required)**
  Root directory to scan recursively for `worker.log` files.

* `--out <DIR>` *(optional)*
  Directory to write CSVs and report. Defaults to `--in`.

* `--base-date YYYY-MM-DD` *(optional)*
  Anchor date for time-only logs.

* `--window-seconds <INT>` *(optional, default: 60)*
  Rollup window size.

* `--name <FILE>` *(optional, default: `cp-report.html`)*
  Output report name.

**Example**

```bash
hzcp all \
  --in ./runs/basic-large-cpmap-test/26-01-2026_15-27-49 \
  --out ./out \
  --base-date 2026-01-30 \
  --window-seconds 60 \
  --name cp-report.html
```

### `--version`

Print version and exit:

```bash
hzcp --version
```

## Report structure

The HTML report is organised into sections:

* **Overview KPIs**
  Total events, elections, leaders, suspects, timeouts

* **Event types (top)**
  What dominated the run

* **From my seat**
  Per-node tables showing:

  ```
  event_type → group_key : count
  ```

  This answers: *“what did node X actually see?”*

* **Per-group correlations**
  Pearson correlations between elections / leader changes and:

  * TCP timeouts
  * Pre-vote rejections
  * Suspicions
  * Append failures
  * Invocation timeouts

* **Event reference (collapsible)**
  Documentation of each event type and why it matters

## Interpretation guidance

* Events seen by **only one seat** usually indicate:

  * Partial connectivity
  * Local stalls
  * Log visibility gaps

* High correlation between elections and TCP timeouts usually means:

  * Network jitter
  * Packet loss
  * GC pauses masquerading as network failure

* Missing expected events usually means:

  * Log wording changed
  * Regex needs updating

## Design principles

* Logs are treated as **ground truth**
* No inferred causality without evidence
* Windowed analysis is explicit and visible
* “Unknown” and “(none)” are kept, never dropped

## Non-goals

* Real-time monitoring
* Generic log parsing
* CP state reconstruction
* Cluster-wide “truth”

This tool shows **what was observed**, not what *should* have happened.
