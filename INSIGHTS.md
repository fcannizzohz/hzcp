## 1. Asymmetric failure modes (not just “network bad”)

**Signal**

* Elections, suspicions, or append failures appear **only on a subset of seats**

**Insight**

* Failures are directional or scoped:

  * One-way packet loss
  * NIC / IRQ starvation on specific hosts
  * JVM pauses that only affect outbound traffic

**Why this matters**
Classic cluster views flatten this into “instability.” Your per-seat view lets you see *who couldn’t talk to whom*, not just that something broke.

## 2. CP sensitivity ranking by group

**Signal**

* Some CP groups churn leaders frequently; others stay stable in the same window

**Insight**

* Groups differ in:

  * Traffic intensity
  * Membership volatility
  * Placement on “bad” nodes

This often exposes:

* Hot partitions
* Skewed client access
* Bad affinity / placement decisions


## 3. Failure amplification vs failure origin

**Signal**

* TCP timeouts spike **before** elections on only 1–2 nodes
* Elections later appear cluster-wide

**Insight**

* Root cause is local; CP is just amplifying it.
* CP elections are *symptoms*, not causes.

This is especially useful when:

* Ops blames “CP instability”
* The real issue is GC, disk IO, or kernel networking on one box


## 4. False consensus illusions

**Signal**

* One node logs “leader elected”
* Another logs nothing for that window

**Insight**

* The system converged eventually, but **not simultaneously**.
* There was a period of split observation even if safety held.

This matters for:

* Latency-sensitive workloads
* Client-visible stalls
* Explaining “nothing crashed but everything slowed”


## 5. Log-visibility gaps (instrumentation debt)

**Signal**

* Expected event chains are broken:

  * Election without prior suspicion
  * Append failure without timeout
  * Pre-vote rejection with no follow-up

**Insight**

* Either:

  * Log wording changed
  * Events are happening below logging thresholds
  * Instrumentation is incomplete

This tells you **where your observability lies to you**.


## 6. Node trustworthiness scoring (implicit)

You can derive a *soft reliability score* per node.

**Heuristic**

* Nodes that:

  * Initiate many suspicions
  * Observe many timeouts
  * Rarely observe leadership stability

are usually:

* Under-provisioned
* CPU starved
* Co-located with noisy neighbors
* Bad JVM configs

You’re not claiming causality—just pattern consistency.


## 7. CP safety margin under stress

**Signal**

* Elections correlate with:

  * Append failures
  * Invocation timeouts
  * Pre-vote rejections

**Insight**

* How close the system was to violating liveness
* Whether failures were clean (fast elections) or messy (cascading retries)

This is critical for:

* Chaos experiments
* Load testing
* Capacity planning


## 8. Time-to-recovery distribution (not just averages)

**Signal**

* Windowed rollups show long tails in election or churn windows

**Insight**

* Mean recovery time lies.
* Tail behaviour tells you:

  * Whether the system stabilises quickly
  * Or oscillates under stress

This is more valuable than a single “recovered in X seconds” metric.


## 9. Regression detection across versions

Run the same workload across versions.

**Compare**

* Event rates
* Correlation strengths
* Per-seat asymmetry

**Insight**

* Behavioural regressions even when:

  * Tests pass
  * No failures are reported
  * Throughput looks fine

This is especially useful for CP changes that are “technically correct but operationally worse”.


## 10. When *not* to trust the conclusions

Your report also tells you when to stop.

Red flags:

* Massive `(none)` or `unknown` buckets
* Events collapse into one regex bucket
* Correlations disappear entirely

**Insight**

* The data is lying.
* Either logs changed or assumptions broke.

That’s a valid output.


## Meta insight (important)

This report is strongest when used to answer:

> “What did the system *experience*?”
> not
> “What *must* have happened?”

If someone tries to turn it into a global truth engine, they’re misusing it.

If you want, next we can:

* Define a **formal “allowed inference” checklist**
* Add **anti-pattern warnings** into the report itself
* Propose **derived metrics** that stay evidence-clean
