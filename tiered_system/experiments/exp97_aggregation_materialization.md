# Raw aggregation materialization and spill (exp97, retired)

The experiment did not justify changing the adopted materialized dictionary
SQL or its fixed 3 GB engine budget. Its native Go probe, test-only SQL rewrite
hooks, Python drivers and driver tests have been removed. This document and
retained artifacts are historical evidence, not a supported execution runbook.

## Local evidence

The local million-row Parquet experiment showed much lower buffers and no
spill with an inlined raw CTE, at the cost of two source scans instead of one.
The final repeat measured 0.327 s → 0.271 s at 128 MB, with about 146 MB peak
temp usage → zero; at 3 GB, 0.241 s → 0.242 s with neither form spilling.
Earlier timing differences were larger and did not consistently repeat.
Both forms passed 182 captured-oracle cold/split cases and 20 metadata/head-tail
cases. A packed per-tag list prototype failed with a native OOM and was rejected.

Retained evidence:

- [Initial local report](results/exp97-materialization-20261105/report.json)
- [Final local report](results/exp97-materialization-final-20261105/report.json)
- [Local validation summary](results/exp97-materialization-final-20261105/summary.json)
- [Rejected packed-list report](results/exp97-packed-20261105/report.json)

## Native Iceberg result

The host comparison used the adopted immutable warehouse and matched every
output to the retained exp96 historical Arrow response. Unprofiled native
medians, with one excluded warmup and three measurements per configuration:

| Case | Materialized 3 GB | Inlined 3 GB | Materialized 6 GB |
| --- | ---: | ---: | ---: |
| Daily mean | 2.755 s | 2.183 s | 2.785 s |
| Monthly mean | 69.740 s | 68.560 s | 63.037 s |

All configurations performed one full-row Iceberg scan, with internal CTE
rereads. The inlined 3 GB plan still reached the engine cap and roughly 6.5 GB
peak temporary-directory usage. The 6 GB control still spilled about 3.6 GB.
Daily timing differences changed sign in the separate diagnostic run. Native
timers excluded initialization/auth/hint setup and are not HTTP latencies.

The host probe needed to copy engine-setting strings before releasing the
Arrow reader, which owned their backing buffers. The failed first trial is
retained alongside the corrected runs. The entire probe is now removed.

Retained evidence:

- [Host control](results/exp97-host-1793905323/control/report.json)
- [Host diagnostics](results/exp97-host-1793905323/profile/report.json)
- [Independent review](results/exp97-host-1793905323/independent-review-20261105.json):
  36 bodies and 12 profile pairs verified, including hashes, metadata pinning,
  parity and recomputed medians.

The broader history and next proposed direction are in
[plan 13](../../plans/13_aggregation_queries.md). Nothing from this experiment
is an alternate production path or API configuration option. Historical SQL,
source snapshots, binaries and outputs under `results/` are preserved and are
not linked into the service.
