# Cube-maker dashboard decisions

The dashboard follows the operational questions in priority order: has processing
succeeded recently, what happened in the latest reported run, what work remains,
and where are failures or delays occurring? It has five named rows and no
dashboard tags. The first row contains only aggregate overview stats.

## Panels and concrete reasons

| Row | Panel | Decision and reason |
| --- | --- | --- |
| Overview | Last Successful Run Age | Keep first: repeated failed completions can look active while useful processing has stopped. This detects that condition. |
| Overview | Latest Run Result | Add: immediately distinguishes a recent failure from a successful completion. Uses the outcome of the latest pushed run timer. |
| Overview | Last Run Age | Keep: distinguishes missing completions or metrics pushes from repeated failed completions. |
| Overview | Remaining Directories (Last Run) | Rename Pending Directories: shows discovered work left unmarked after a reported run. A completed-run snapshot cannot establish the current HDFS backlog. |
| Overview | Last Run Duration | Add: immediately shows whether processing is consuming or exceeding the hourly schedule interval. |
| Overview | Failed Directories (Last Run) | Replace the selected-range failure total: identifies whether the latest run encountered a directory failure. Zero is valid because every run initializes this counter. |
| Run History | Run Result History | Add: reveals periods of repeated failure and recovery without interpreting fresh-process counters as cumulative run counts. |
| Run History | Run Duration History | Replace Run Duration percentiles: compares reported run durations over time. Each process records one run, so the timer sum directly represents its duration. |
| Directory Processing | Remaining Directories History | Add: shows whether successive reported runs leave more discovered work unfinished. |
| Directory Processing | Directory Outcomes (Last Run) | Revise Directory Outcomes: separates successful work, failures, and directories skipped without producing cubes. Raw completed-run counts avoid rates across replaced snapshots. |
| Directory Processing | Average Directory Duration (Last Run) | Replace Directory Duration percentiles: measures the complete directory attempt, including the tracking update. A current-run outcome-count guard hides retained timers when no matching attempts occurred. |
| Directory Processing | Directory Input Bytes (Last Run) | Replace Directory Input Throughput: shows workload size for comparison with duration, including wasted attempts. These are compressed file sizes counted per attempt, not measured Spark I/O. |
| Cube Processing | Cube Attempts by Outcome | Add: identifies the failing cube and uneven progress among the three producers while processing is active. |
| Cube Processing | Average Cube Duration | Replace Cube Duration by Cube percentiles: identifies the slow producer using the running average of completed attempts, including computation and database writes. |
| PostgreSQL Writes | Rows Upserted by Cube (Selected Range) | Keep and move out of the overview: compares committed output among cubes. Use live pod counters so the range estimate does not mix independent Pushgateway snapshots. |
| PostgreSQL Writes | Write Attempts by Phase and Outcome | Add: distinguishes JDBC staging failures from merge, cleanup, or commit failures. |
| PostgreSQL Writes | Average Staging Write Duration | Split PostgreSQL Write Duration: isolates Spark-to-PostgreSQL JDBC staging latency. |
| PostgreSQL Writes | Average Upsert Duration | Split PostgreSQL Write Duration: isolates PostgreSQL merge, staging cleanup, and commit latency. |

## Removed representations

| Original panel or query | Reason for removal |
| --- | --- |
| Failed Runs (Selected Range) | Each fresh process records one run. Consecutive failures can push the same count of `1`, so `increase()` on the pushed counter can report zero additional failures. Latest Run Result and Run Result History answer the reliability question without claiming an exact run count. |
| Failed Directories (Selected Range) | A final per-run count can increase, decrease, or remain unchanged when the next process pushes its snapshot. Its cross-run increase is not a trustworthy total. Show the latest reported attempt count instead. |
| Directory Outcomes rates | The snapshot counter belongs to one completed run; its rate across pushes does not represent processing speed. Replace with per-run outcome counts. |
| Directory Input Throughput rates | The metric records a directory's full known compressed size, including failed attempts and retries. A rate across final pushes is neither Spark read throughput nor useful bytes processed per second. |
| Run, directory, cube, and database p50/p95 queries | Short counter-rate windows contain little activity for an hourly batch. Histogram bounds are also not configured for long batch steps. Timer sums and counts support interpretable durations and averages without bucket-bound assumptions. |
| Combined PostgreSQL duration chart | Mixing staging and merge curves across cubes and outcomes makes the failing or slow phase harder to identify. Split latency by phase and put phase outcomes beside it. |
| Aggregate Rows Upserted in the first row | Output volume is not a reliable health test: an idle run can succeed with no new rows, and a failed run can commit some rows before failing. Keep the per-cube output comparison in the write row. |

## Metric sources and interpretation

`CubeMakerMetrics` creates a new registry for each process. Directory counters and
input-byte counters are initialized for all outcomes. Timers and row counters are
created only when their operations occur. `Main.ApplicationResources.close()`
pushes the registry with `pushAdd()` during cleanup. The monitoring configuration
also scrapes the running pod under `job="app-metrics"` every 15 seconds.

- **Overview, Run History, and Directory Processing:** select one Metrics Job
  and read Pushgateway snapshots. A newer snapshot replaces the metrics that it
  supplies; absent metric families can survive from earlier pushes. The successful
  timestamp deliberately survives failures. Directory duration queries require a
  positive current-run directory count for the matching outcome, which prevents
  an old duration from appearing on a run with no matching work.
- **Cube Processing and PostgreSQL Writes:** use pod scrapes, filtered by
  Namespace and Cube. This avoids presenting retained cube/write metrics as work
  from a newer run that never performed those operations. Curves show counts or
  running averages in the process active at each point in time. Idle periods
  produce gaps, and in-progress operations are not included in completed-attempt
  timers.
- **Rows Upserted:** uses `increase()` on each live series before aggregating by
  cube. This is a sampled estimate. Initial or final writes between scrapes can be
  missed, and short-lived runs may be absent entirely. Missing samples are not
  displayed as zero. Updates and retries count again; these are not unique rows.
- **Freshness:** applies to the latest reported completion or success, not to the
  age of the input data. Initialization failures and hard kills before a metrics
  push cannot update the latest result. The freshness panels expose a lack of new
  reports, but these metrics cannot explain executor termination by themselves.
- **Remaining work:** covers directories discovered by the reported run. Locked
  directories remain pending without a skipped-attempt increment. Newly arriving
  directories are unknown until another discovery. A successful run can leave
  locked work pending.
- **History:** Pushgateway exposes the latest report repeatedly between runs.
  Step lines represent that sampled state. Multiple reports between scrapes can
  be missed; none of these charts is an exact event log.
- **Filters:** Metrics Job affects the first three rows; Namespace and Cube affect
  the last two rows. Metrics Job is single-select to avoid combining one job's
  fresh completion with another job's stale state.
- **Thresholds:** the current chart runs hourly. Last Run Age warns at 90 minutes
  and becomes critical at 150 minutes; Last Successful Run Age warns at two hours
  and becomes critical at four hours. Duration warns at one hour and becomes
  critical at two hours. Adjust these if the schedule or expected processing time
  changes.

There is no current metric for exact completed-run counts across processes,
current processing state, oldest unprocessed input age, or cube-specific data
freshness. Those panels require additional instrumentation before they can be
displayed honestly.

References: [Prometheus batch-job instrumentation](https://prometheus.io/docs/practices/instrumentation/#batch-jobs),
[Pushgateway POST replacement semantics](https://github.com/prometheus/pushgateway#post-method),
and [Micrometer histogram configuration](https://docs.micrometer.io/micrometer/reference/concepts/histogram-quantiles.html).
