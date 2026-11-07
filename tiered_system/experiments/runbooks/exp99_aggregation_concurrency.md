# Bounded aggregation concurrency (exp99, completed)

Saved runs cited below as `results/...` are paths inside the split archive in the parent directory (`../results.z01`, `../results.z02`, `../results.z03`, and `../results.zip`). See [recorded results](../README.md#recorded-results). A command that creates a new run directory still writes local untracked output.

This completed experiment supported integrating pre-buffer reduction. Its
native concurrency probe, barrier/resource helpers, Python driver and driver
tests were removed after exp100 passed. Historical reports, profiles, samples,
SQL and binaries remain under `results/`. Use
[exp100](exp100_aggregation_http.md) for maintained HTTP validation.

## Scheduling and scope

The API holds its cold backend mutex through result closure, so concurrent
cold callers to one instance queue. The experiment compared that real shared
backend with independent native engines in one process, a replica/resource
proxy rather than concurrency enabled inside one deployed API.

Each active Arrow engine retained two CPU threads, eight async I/O threads,
read-ahead -1 and 3 GB. The host was Darwin arm64, 18 logical CPUs and 24 GiB
physical memory, without a process memory cap. Initialization/auth/hint setup
was excluded; native work, queueing and IPC output were timed. Fresh engines
were reused within each wave, with one excluded warmup and three measured
waves per mode/scope. These are not HTTP/SLO or maximum replica-density results.

## Results

Unprofiled monthly medians:

| Scope / callers | Mode | Wave | Queries/s | Sampled peak RSS | Allocated scratch |
| --- | --- | ---: | ---: | ---: | ---: |
| Shared / 1 | Dictionary | 70.96 s | 0.0141 | 2.74 GB | 6.54 GB |
| Shared / 1 | Reduction | 66.93 s | 0.0149 | 1.41 GB | ~0 |
| Shared / 2 | Dictionary | 131.43 s | 0.0152 | 3.36 GB | 6.60 GB |
| Shared / 2 | Reduction | 91.64 s | 0.0218 | 1.19 GB | ~0 |
| Independent / 2 | Dictionary | 75.86 s | 0.0264 | 4.23 GB | 12.41 GB |
| Independent / 2 | Reduction | 70.66 s | 0.0283 | 1.78 GB | ~0 |

Shared execution remained serialized: the reduction's follow-up query took
24–25 s, versus 62–65 s for the dictionary. It still scanned the input;
reduced read counters and blocked time were consistent with cached-input reuse,
not a cached response. Independent throughput improved about 7%, while CPU
per query increased about 16%. Shared two-caller throughput improved about
43% because of the faster warm follow-up.

Whole-process RSS and simultaneous aggregate scratch were sampled, not inferred
by summing per-query maxima. RSS excludes kernel page cache; sampled peaks may
miss transients. The descriptive p95 had too few requests to estimate service
tails. No node memory cap or multi-container density limit was measured.

Retained evidence:

- [Control](results/exp99-host-1793916413/control/report.json)
- [Diagnostics](results/exp99-host-1793916413/profile/report.json)
- [Independent review](results/exp99-host-1793916413/independent-review-20261105.json):
  60 responses, 20 profile pairs, raw resource metrics and medians verified.

The resource savings and warm benefit justified the subsequent exp100 HTTP
integration gate. Production scheduling and the 3 GB setting remain unchanged.
No old execution-mode option remains in the API or its active probes.
