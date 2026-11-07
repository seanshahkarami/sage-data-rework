# Reduction before buffering (exp98, completed)

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

This experiment led to the integrated unrestricted aggregate path in
`beehive-data-api/internal/backend/cold/aggregate_reduce.go`. Head/tail retains
the live observation-selection path. Both share the numeric-expression builder;
there is no operator fallback or unrestricted legacy mode.

After the exp100 HTTP/client gates passed, the exp98 native probe, SQL-parsing
adapters, local SQL exporter, Python drivers and driver tests were removed.
Numeric/type/metadata regressions remain in `aggregate_reduce_test.go`, and the
full compiler oracle exercises production dispatch. This document and the
retained artifacts are historical evidence, not an execution runbook. Use
[exp100](exp100_aggregation_http.md) for maintained validation commands.

## Design evidence

The initial design combined numeric subtotals from separate raw tag aliases.
It was rejected: floating reassociation can turn a single sum of
`1e16, 1, -1e16` from 0 into 1. The
[counterexample](results/exp98-partial-20261105/float-reassociation-counterexample.json)
and prototype evidence remain preserved.

The successful design derives an equivalent canonical grouping tuple before
reduction: name, sorted non-promoted metadata entries, promoted effective
values and presence bits. Presence distinguishes a null value from an absent
key. Each public series/window uses one native numeric reduction. Public
metadata reconstruction happens after reduction, and no averages of averages
or numeric subtotal merging are introduced.

[Local evidence](results/exp98-partial-20261105/summary.json) established parity
and lower buffers/spill, with extra key-processing CPU. The native host run
also preserved parity, but its three-trial latency medians did not improve:

| Native workload | Dictionary | Reduction |
| --- | ---: | ---: |
| Daily mean | 2.186 s | 3.636 s |
| Monthly mean | 66.496 s | 67.192 s |
| Fleet minute count | 2.635 s | 2.711 s |

Monthly diagnostic engine peak fell from 3.00 GB to 1.22 GB and temporary usage
from 6.84 GB to zero. Those fresh-engine native results motivated the reused
engine/concurrency experiments rather than immediate rejection or promotion.
They are not HTTP latency measurements.

Retained host evidence:

- [Unprofiled control](results/exp98-host-1793912997/control/report.json)
- [Diagnostics](results/exp98-host-1793912997/profile/report.json)
- [Independent review](results/exp98-host-1793912997/independent-review-20261105.json):
  36 bodies and 12 profile pairs verified.

Exp99 then established resource and warm-follow-up benefits; exp100 validated
actual HTTP routing, both formats, split queries, limits and client precision.
All historical source snapshots, SQL, binaries and results remain under
`results/`; none is linked into the service.
