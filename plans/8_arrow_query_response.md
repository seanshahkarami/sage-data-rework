# Arrow stream on the query response

**Resumed 2026-10-26:** Iceberg v3 / single stored VARIANT and the bounded
chronological plugin Parquet architecture are now adopted; see
[plan 10](10_iceberg_v3.md). Earlier pending-migration notes below are historical.
Whole-range execution is retained; windowing was rejected after exp80 and its
experimental code was removed. The measurements and decision are recorded at
the end of this document. Public Arrow/NDJSON schemas remain unchanged.

Decided 2026-10-20. The query API keeps newline JSON as the body it writes. A large extract can ask for an Arrow stream on the same `POST /query`. It is built. The local demo measurement is at the end of this note.

## What the endpoint returns today

`beehive-data-api` writes status 200 and then one JSON object per row. `writeRecord` in `service.go` is `json.NewEncoder(w).Encode`. The download name is `sage-download-<timestamp>.ndjson`. A record is `timestamp`, `name`, `value`, and `meta`. `value` is a float, an int, or a string. `meta` is an open map of strings (`types.go`).

`sage-data-client` sends `Accept-Encoding: gzip` and `load` calls `json.loads` on every line, parses `timestamp` as nanoseconds, and copies each meta entry into a `meta.*` column (`sage-data-client/src/sage_data_client/query.py`). The portal reads the body as text, splits on newlines, and `JSON.parse`s each line (`sage-gui/components/apis/beehive.ts`).

## What stays

Newline JSON stays the response when the request does not ask for anything else. The portal draws small graphs from that body. The request body stays the same JSON query. Pelican still has to serve the row groups the query touches. Arrow does not change the cold file layout in [7_parquet_file_struct_recommendation.md](7_parquet_file_struct_recommendation.md).

## Parquet as the body

A Parquet response is the wrong file for this endpoint. The footer is the last thing written, so the client cannot read a row until the query finishes. Both clients above would have to change, and a polymorphic `value` plus an open `meta` map do not fit one fixed Parquet schema without the same schema work an Arrow stream needs. Gzip on the JSON stream is already the size lever `sage-data-client` asks for.

## What the stream adds

Add it for extracts of millions of rows through `sage-data-client`. A minute of one sensor is about ten thousand rows, and repeating the JSON keys is cheap. A node-day is about ten million rows. `W0A0` on March 1 is 9.5 million of 140.6 million. At that size each line repeats `timestamp`, `name`, `value`, and the same meta tags, and Python parses every line. An Arrow stream sends those as columns. `value` is one string column holding the JSON scalar (`11.7`, `1024`, `"file.jpg"`, `true`), and `meta` is the JSON object for that row's tags. `pyarrow` reads the stream, the client `json.loads` each value and each meta object, and `query` still returns a Pandas dataframe.

The server-side half is what makes that download fit. The handler walks one Go record at a time and writes a JSON line. A full cold day of one node ran the 4 GB API container out of memory during that walk. The cold path should write DuckDB's batches straight into the response, so those rows never become Go structs.

The gain on the wire is client CPU and time-to-dataframe. Gzip already shrinks the repetitive JSON, so the download is not where the minutes are.

## Hot storage sends it too

The format is chosen in the HTTP handler, after a backend has produced rows. `service.go` calls `svc.backend.Query` and writes each `Record` from the `Results` stream. Influx, DuckDB, and the tiered backend all return that interface. A query that crosses the watermark is one response: `tieredResults` yields the cold rows, then the hot rows. An `Accept: application/vnd.apache.arrow.stream` branch in that loop batches whichever rows come back.

Writing DuckDB's batches straight to the socket stays on the cold path. Influx still builds a Go `Record` per Flux row in `influxResults.convertToAPIRecord`, and the handler batches those into Arrow. A large hot extract skips the JSON parse in the client. It still allocates one Go record per row on the server.

## The data client keeps returning a dataframe

The published `sage-data-client` keeps working with no change. `query` sends only `Accept-Encoding: gzip` and `load` parses newline JSON. The API writes that body unless the request asks for the Arrow stream. `load` stays the reader for a saved `.ndjson` file and for an old server.

Taking the Arrow path means a new release of the client. `query` sends `Accept: application/vnd.apache.arrow.stream`, reads that `Content-Type` with `pyarrow`, and returns the same dataframe: `timestamp`, `name`, `value`, and `meta.*`. Callers keep `df["meta.vsn"]` and `groupby`. `pyarrow` becomes a dependency next to `pandas` in `setup.cfg`. That drops the Python 3.6 floor already overtaken by `pandas>=2.0.0`.

Pandas stays the result. `pyarrow` is the decoder. The current Arrow path avoids parsing each outer NDJSON record, but still parses the JSON text in `value` and `meta` for every row. At month scale this dataframe reconstruction is the dominant client cost; the investigation below profiles it. `groupby` and column access then run on the dataframe, as they do today. `to_pandas()` should use Pandas 2 Arrow-backed string columns so each repeated `meta.*` value is not its own Python object. The server-side half does not depend on the client keeping Arrow after that conversion: the cold handler writes DuckDB's batches to the HTTP body either way.

## How the request selects it

The client sends `Accept: application/vnd.apache.arrow.stream`. The response `Content-Type` is that same type. The body is the streaming format: a schema message, then record batches, then an end-of-stream mark. The handler can write a batch as rows arrive. `application/vnd.apache.arrow.file` is the random-access file with a footer, and it has the same problem as a Parquet response.

The schema has to hold a float, an int, or a string in `value`, and an open set of string tags in `meta`. Both are JSON text in the Arrow body. The portal sends no Arrow `Accept` and keeps newline JSON.

## The format and the transport

Arrow on this endpoint is the bytes in the HTTP body. HTTP is what carries them. Seeing Arrow move across `POST /query` does not mean Arrow is the network protocol.

The payload is the Arrow IPC stream. A batch carries one array per column. The current schema uses ordinary string arrays for `name`, `value`, and `meta`; it does not dictionary-encode repeated metadata. The cold handler can write a batch that DuckDB already produced into the response, instead of allocating a Go struct and a JSON line per row. `pyarrow` reads those chunks into a dataframe. That is Arrow as the in-memory and on-the-wire layout.

The transport stays HTTP. Go's `net/http` writes the body and `sage-data-client` reads it with `urllib`, the same `POST /query` both clients use today. HTTP copies bytes from the server's write buffer to the client's read buffer and does not interpret them. Newline JSON and an Arrow stream are two payloads on that one route.

The portal does not take the Arrow payload. `sage-gui` is a browser page built with Parcel. Node is the build and test tool. `getData` in `components/apis/beehive.ts` runs in the page: `fetch`, `res.text()`, `JSON.parse`, then an array of objects for Chart.js. The table stops at 100,000 rows, and most other calls use `tail: 1`. Decoding Arrow in the page would still end at that array of objects. The page keeps newline JSON on the same HTTP route.

## Arrow Flight

Flight is a different transport for the same kind of payload. A Flight server speaks gRPC. The client calls `DoGet` with a ticket and receives record batches. It does not serve `POST /query`. Adopting it means a second server and port beside `beehive-data-api`, a Flight client in `sage-data-client` (`pyarrow.flight` instead of `urllib`), and a proxy in front of the portal, because the page cannot call gRPC.

| | this design | Flight |
|---|---|---|
| payload | Arrow IPC stream, `application/vnd.apache.arrow.stream` | Arrow record batches |
| transport | HTTP `POST /query` | gRPC `DoGet` |
| large extract | `urllib` plus `pyarrow` | a Flight client |
| portal | newline JSON on the same route | a proxy, then the page still wants objects |

Flight pays off when one service ships large columnar batches to another and the RPC layer is the thing being built. Here the batches are the part worth having, and the HTTP route already exists. `arrow-go` v18.5.1 is a direct require in `beehive-data-api/go.mod`. The handler builds the record batches. On the cold path those batches are DuckDB's. On the hot path they are built from the Go records Influx already produced. On a one-minute cold query the cost is still reading the day file from Pelican.

## Ndjson vs Arrow

2026-10-21, against `demo-influxdb` and `demo-data-api` on Rancher Desktop. The API image was rebuilt with `-tags=duckdb_arrow` after Arrow began reusing one native DuckDB per cold backend. The watermark was `2025-05-01T00:00:00Z`. One name is `wxt.env.temp` on `vsn=W0A0`. All names is every measurement on `W0A0`. Hot is Influx. Cold is the live Iceberg table. Each suite started from a fresh API process. The API cap is 14 GB and the Rancher VM is 16 GB.

Speedup is newline-JSON latency divided by Arrow latency, so a number above 1 means Arrow was faster. Memory is the peak API-container sample during the call. The server does not gzip. Short queries are the median of 2 runs after one warmup. Each month is one run. Body sizes are the raw response.

Curl latency is the download plus a DuckDB 1.5.6 `count` of `value` and `meta`. Script: `tiered_system/experiments/exp52_arrow_vs_ndjson_rerun.py`. Results: `tiered_system/experiments/results/exp55_curl_reused_engine.json`.

| Query | Rows | NDJSON | Arrow | Speedup | NDJSON memory | Arrow memory | NDJSON body | Arrow body |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Hot, 1 min, one name | 686 | 0.032 s | 0.016 s | 2.05× | 39 MB | 39 MB | 0.24 MB | 0.20 MB |
| Hot, 1 hour, one name | 43,525 | 0.344 s | 0.238 s | 1.45× | 40 MB | 88 MB | 15.05 MB | 12.70 MB |
| Hot, 6 hours, one name | 261,034 | 1.505 s | 1.014 s | 1.48× | 49 MB | 85 MB | 90.24 MB | 76.17 MB |
| Hot, 1 min, all names | 6,243 | 0.136 s | 0.083 s | 1.63× | 46 MB | 49 MB | 2.22 MB | 1.88 MB |
| Hot, 1 hour, all names | 396,225 | 2.373 s | 1.579 s | 1.50× | 48 MB | 47 MB | 140.87 MB | 119.52 MB |
| Cold, 1 min, one name | 698 | 0.401 s | 0.386 s | 1.04× | 109 MB | 191 MB | 0.24 MB | 0.20 MB |
| Cold, 1 hour, one name | 43,509 | 0.941 s | 0.596 s | 1.58× | 284 MB | 321 MB | 14.89 MB | 12.70 MB |
| Cold, 1 min, all names | 6,352 | 0.597 s | 0.514 s | 1.16× | 531 MB | 761 MB | 2.24 MB | 1.93 MB |
| Cold, 1 hour, all names | 395,989 | 4.173 s | 1.982 s | 2.11× | 1.28 GB | 1.39 GB | 139.64 MB | 119.79 MB |
| Hot May, one name | 30,388,936 | 172.9 s | 115.5 s | 1.50× | 1.37 GB | 1.43 GB | 10.50 GB | 8.86 GB |
| Cold March, one name | 25,940,207 | 280.6 s | 122.5 s | 2.29× | 11.1 GB | 6.72 GB | 8.87 GB | 7.56 GB |

Row counts matched on every curl query. DuckDB read the May bodies in 1.5 s (JSON) and 2.7 s (Arrow), and the March bodies in 1.5 s and 1.7 s. The API log for May was 171.4 s JSON and 112.8 s Arrow. For March it was 279.1 s JSON and 120.8 s Arrow. Gzip of those bodies, level 6, not sent by the server: May is 251 MB JSON and 297 MB Arrow. March is 168 MB JSON and 312 MB Arrow.

sage-data-client latency is the time until the pandas dataframe exists. Arrow uses the batched `_load_arrow`. Newline JSON uses `load`, which this decoder change does not touch, so the JSON column remains the earlier pass in `results/exp55_sage_data_client_reused_engine.json`. The Arrow column is the live rerun in `results/exp58_arrow_decoder_live.json`. `query()` itself asks for Arrow first.

| Query | Rows | NDJSON | Arrow | Speedup | NDJSON memory | Arrow memory | NDJSON body | Arrow body |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Hot, 1 min, one name | 686 | 0.028 s | 0.021 s | 1.38× | 41 MB | 40 MB | 0.24 MB | 0.20 MB |
| Hot, 1 hour, one name | 43,525 | 0.889 s | 0.254 s | 3.51× | 43 MB | 83 MB | 15.05 MB | 12.70 MB |
| Hot, 6 hours, one name | 261,034 | 5.699 s | 0.976 s | 5.84× | 51 MB | 79 MB | 90.24 MB | 76.17 MB |
| Hot, 1 min, all names | 6,243 | 0.179 s | 0.092 s | 1.94× | 47 MB | 46 MB | 2.22 MB | 1.88 MB |
| Hot, 1 hour, all names | 396,225 | 9.337 s | 1.694 s | 5.51× | 52 MB | 47 MB | 140.87 MB | 119.52 MB |
| Cold, 1 min, one name | 698 | 0.419 s | 0.391 s | 1.07× | 129 MB | 113 MB | 0.24 MB | 0.20 MB |
| Cold, 1 hour, one name | 43,509 | 1.457 s | 0.661 s | 2.21× | 307 MB | 184 MB | 14.89 MB | 12.70 MB |
| Cold, 1 min, all names | 6,352 | 0.631 s | 0.520 s | 1.21× | 517 MB | 368 MB | 2.24 MB | 1.93 MB |
| Cold, 1 hour, all names | 395,989 | 10.992 s | 1.996 s | 5.51× | 1.18 GB | 713 MB | 139.64 MB | 119.79 MB |
| Hot May, one name | 19,263,320 JSON; 30,388,936 Arrow | 1592.7 s | 104.5 s |  | 1.10 GB | 0.78 GB | 6.65 GB | 8.86 GB |
| Cold March, one name | 25,940,207 | 3526.4 s | 137.1 s | 25.7× | 11.7 GB | 3.62 GB | 8.87 GB | 7.56 GB |

The May newline-JSON client call is still the short result from the earlier pass: 19,263,320 rows after the 15-minute Influx timeout, so that row has no speedup. The new Arrow decoder returned the full May and March months. May Arrow's server time was 103.6 s and the dataframe existed at 104.5 s, with client RSS peaking at 654 MB and no major page faults. March Arrow's server time was 136.4 s and the dataframe existed at 137.1 s. Client RSS peaked at 2.01 GB, with 123 major faults. The API peaked at 3.62 GB on that call.

The March newline-JSON sort is what uses most of the 14 GB cap: 11.1 GB over curl and 11.7 GB over the earlier client pass.

### Verification

Tests pass with and without `-tags=duckdb_arrow`. Targeted race-detector tests
also pass. New coverage checks exact identities and timestamp order for 200,003
rows, native streaming result mode, empty and nonempty IPC decoding, cancellation
between batches and during execution, setup/prepare errors, spill-directory
cleanup, and HTTP abort after a batch has already been emitted. An opt-in local
integration check also passed through `backend.queryArrow` with cached HTTP/Iceberg
extensions and scoped-secret setup, followed by a second query after cleanup.

### Live Iceberg query

2026-10-21. The demo API was rebuilt from this streaming reader and the same March query was run against the published Iceberg table, one pass, container cap still 12 GB. Curl plus DuckDB, the exp49 reader. Result file: `tiered_system/experiments/results/exp49_cold_month_arrow_streaming.json`.

Arrow returned the full series. 25,940,207 rows, 7.56 GB on the wire, first byte at 102.6 s, download 123.0 s, DuckDB read 1.7 s, API log 122.9 s. Gzip of that body, not sent by the server, is 312 MB. Sampled container memory peaked near 3.6 GB and was 184 MB when the body finished. The previous Arrow attempt on this query sent nothing and sat near 10.4 GB. The newline-JSON pass of the same month took 461.9 s to download 8.87 GB and peaked near 10.5 GB.

A one-minute cold query on the new binary returned 698 rows, with `meta` still a JSON string.

## Bugs & Fixes

### Reusing the Arrow database, 2026-10-21

The backend now keeps one native Arrow database, initialized lazily by the first
Arrow query. `INSTALL` and `LOAD` for `httpfs` and `iceberg`, thread/memory
settings, and creation of its spill directory run once per successful database
initialization. Later requests use a fresh connection to that same native
database. The Arrow database remains separate from the newline-JSON database
because the Go driver does not expose its native handle for streaming execution.

Each request still sets its connection timezone to UTC, installs a freshly
minted scoped Pelican token, and reads the current version hint. It owns and
releases its result buffers, connection, and cancellation watcher. Reusing the
database does not pin readers to an old snapshot or reuse an expired token.
The existing backend mutex serializes cold queries. `ColdBackend.Close()` waits
for active results, closes both engines, and removes the Arrow spill directory.
An initialization failure cleans up the failed instance so a later request can
retry. A canceled or failed query leaves the shared database available.

An isolated local benchmark compares creating a database and loading cached
extensions for each request with connecting to the reused, initialized database.
Both execute a 698-row synthetic query and write the same Arrow IPC schema to
a discard sink. It includes per-request secret setup and UTC configuration,
but excludes Pelican, HTTP, and client decoding. Twenty iterations per case,
DuckDB 1.5.6 on macOS arm64:

| Lifecycle | Mean time per iteration |
|---|---:|
| New database and cached extension setup per request | 20.15 ms |
| Reused initialized database, fresh connection per request | 0.79 ms |

This measures removal of repeated initialization, not the total speedup of the
cold minute over Pelican. Artifact:
`tiered_system/experiments/results/exp53_arrow_engine_reuse.json`.

The curl and sage-data-client tables above are the container rerun of this
lifecycle. On the per-request database, those four cold Arrow windows took
1.942 s, 1.913 s, 4.352 s, and 6.363 s. The first Arrow request of a process
still installs the extensions. Later requests use the shared database. The
local 19 ms figure remains the cached-extension portion only. The container
gap that closed also includes a fresh native database, a new Pelican secret,
and the first HTTP metadata read that each old Arrow request repeated.

Tests cover reuse across sequential requests, unfinished-result cleanup,
cancellation and query-error recovery, successful initialization only once,
cleanup after failed initialization, and backend shutdown. Both build
configurations and targeted race tests pass. The cached-extension integration
check confirms that HTTP/Iceberg remain usable after closing the initialization
connection and that the next request uses the same native database.

Reproduce the isolated benchmark from `beehive-data-api` with extensions already
cached:

```sh
BEEHIVE_ARROW_CACHED_EXTENSIONS=1 go test -tags duckdb_arrow \
  -run '^TestColdArrowBackendCachedExtensions$' \
  -bench '^BenchmarkColdArrowStartup$' -benchtime=20x -count=1 .
```

### Why cold-month Arrow stalls while newline JSON finishes

#### Correction and fix, 2026-10-21

The original diagnosis attributed the stall to a second, complete Arrow copy
on top of the sort. Source inspection and a controlled reproduction do not
support that explanation. The resource pressure is real, but the important
execution choice is **materializing the complete query result before returning
its reader**.

In the pinned `duckdb-go/v2 v2.10506.0`, `Arrow.QueryContext` calls the same
`Conn.QueryContext` as the row path. That goes through `Stmt.executeBound`,
`PendingPrepared`, and `ExecutePending`. `PendingPrepared` requests a
non-streaming result. Only after execution returns does the driver construct
an Arrow reader. It converts one fetched chunk at a time, releasing the
previous chunk; it does not preconvert every result row to Arrow.

Consequently, neither observing the handler release one batch at a time nor
using `database/sql.Rows` establishes bounded query memory. The full native
result can already be resident. That result allocation can exceed DuckDB's
`memory_limit`; the setting is not a process RSS or cgroup cap. Changing map
metadata to JSON removed one expensive representation, but did not change
this execution mode. The two SQL projections also differ in value serialization
and timestamp conversion, so matching `to_json(meta)` alone does not make their
allocation behavior identical.

The previous claims that the sort itself cannot spill and that a temporary
directory could not help were too broad. A blocking sort and a retained final
result are different allocations. The fix still sorts to preserve response
ordering; it changes how the final result is consumed.

#### Implemented change

`beehive-data-api/cold_arrow.go` now uses `newStreamingArrowReader` in
`cold_arrow_stream.go`. It requests `PendingPreparedStreaming`, then converts
and writes one fetched chunk at a time through the existing IPC writer.
The SQL projection, response schema, and ordering are preserved.

The Go driver does not expose its connection handle or a streaming query
method. The initial streaming implementation owned a query-scoped native DuckDB instance
using the bindings already pinned in the module. The reuse change above moves
that instance to the backend lifetime while keeping each connection query-scoped. This avoids a driver fork,
private-field access, or modification of the driver's global mapping functions.
It applies the same UTC, two-thread, 3 GB settings and HTTP/Iceberg extension
setup as the row backend, and installs the query's scoped Pelican secret.

The reader owns its result, Arrow buffers, connection, and cancellation watcher.
Release frees those query resources. The backend owns the native instance and
its unique local spill directory, which are removed by `ColdBackend.Close()`.
The backend keeps its existing one-query lock. A cancellation watcher interrupts
native execution and remains active during fetching; cleanup joins that watcher
before disconnecting. A late query or IPC write error aborts HTTP framing using
`http.ErrAbortHandler`, so clients cannot accept a partial extract as complete. This also makes cancellation during a blocking sort
observable rather than waiting for a batch to arrive.

The streaming C API is deprecated upstream but remains available in the pinned
DuckDB 1.5.6 bindings. This is a targeted compatibility implementation; recheck
it when upgrading DuckDB. The current Go Arrow API does not expose a replacement
streaming method. No dependency version or container memory limit was increased.
The newline-JSON path still uses the materializing driver and retains its own
large-result resource problem.

#### Controlled measurement

`experiments/exp51_cold_arrow_materialization.py` drives the opt-in
`TestColdArrowMemoryProbe` in separate processes and samples native RSS with
psutil. The query generates unique wide metadata, applies the current Arrow
projection and `ORDER BY timestamp`, encodes IPC, and discards the body. It uses
the same DuckDB 1.5.6 library, two threads, and identical SQL in both cases.
This isolates result retention from Pelican, HTTP, curl, Pandas, and container
configuration. Values below are individual runs, not medians.

| Rows | DuckDB limit | Existing Go Arrow API peak RSS | Native streaming peak RSS | Existing / streaming total |
|---|---|---|---|---|
| 1,000,000 | 256 MB | 644.4 MB | 323.1 MB | 0.75 / 0.77 s |
| 4,000,000 | 256 MB | 1,672.1 MB | 369.7 MB | 3.37 / 3.40 s |

All rows were consumed through the same IPC writer. The existing API's RSS grows
with output size despite the 256 MB setting. Native streaming removes most of
that growth. Results: `experiments/results/exp51_cold_arrow_materialization.json`.

A month-sized synthetic case returned **25,940,207 rows in 25.49 s**, at
**1,101.8 MB peak RSS**, with `memory_limit='1GB'`. QueryContext-equivalent setup
returned after 18.12 s; ordering still requires work before output can start.
Results: `experiments/results/exp51_cold_arrow_month_stream_1gb.json`.

The same month-sized synthetic case at 256 MB failed during fetching with an
explicit DuckDB out-of-memory error at about 367 MB RSS. This is recorded in
`experiments/results/exp51_cold_arrow_month_stream.json`. Streaming removes
unbounded output retention; it does not make every sort executable under an
arbitrarily small budget. The production setting remains 3 GB.

#### Real archive query, local files

The same March 2025 query was then run against the original local archive:
`date=2025-03-*/*.parquet`, `name=wxt.env.temp`, `vsn=W0A0`, the API's
`buildColdArrowSQL` projection and ordering, with only `iceberg_scan` replaced
by `read_parquet`. The native reader wrote the current IPC schema to a discard
sink. **25,940,207 rows matched**, the same count recorded for the live Iceberg
query. It took **79.41 s** (74.54 s before the reader was ready), with
**1,324.9 MB peak RSS** and `memory_limit='1GB'`.

This is the actual archive and filter, rather than a synthetic cardinality
match. The input layout differs from the reorganized Pelican Iceberg table,
and it excludes HTTP download/client decoding, so these are not replacement
numbers for exp49. Results: `experiments/results/exp51_cold_arrow_real_march.json`.

Reproduce from the workspace root after compiling the API test binary:

```sh
python -I sage-data-rework/tiered_system/experiments/exp51_cold_arrow_materialization.py \
  /tmp/beehive-arrow-probe.test --rows 25940207 --modes stream --memory 1GB \
  --source "$PWD/sage-data-rework/data/archive/date=2025-03-*/*.parquet" \
  --output sage-data-rework/tiered_system/experiments/results/exp51_cold_arrow_real_march.json
```

### Why the latest speedups are limited — investigation, 2026-10-21

The measurements show two different limits: the curl path is mostly query
execution and response production; the Python path adds a large row-by-row
reconstruction cost. These should not be treated as one Arrow decoding number.

#### The current response keeps JSON inside Arrow

`beehive-data-api/arrow_response.go:apiSchema` is timestamp plus three ordinary
string columns: `name`, `value`, `meta`. On the cold path,
`cold_sql.go:coldArrowProjection` calls `to_json` for values and metadata. On the
hot path, `appendValue` and `appendMeta` still call `json.Marshal` per row after
Influx's Flux client constructs a Go record. This skips the outer NDJSON record
and textual timestamp, but preserves much of the JSON generation/parsing work.
It does not provide typed numeric arrays end to end.

The earlier statement that metadata was dictionary-encoded was incorrect. Neither
the schema nor the default IPC writer selects dictionary encoding or compression.
Those are optional Arrow features, not automatic consequences of choosing IPC.
See the [Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).

A saved real 698-row cold response has **181,480 bytes of metadata out of
203,816 column-buffer bytes (89%)**, despite having just **one distinct metadata
object**. Its reserialized plain IPC stream is 204,448 bytes; encoding `name`
and `meta` as dictionaries makes it 18,072 bytes, and Zstd IPC with the original
plain-string schema makes it 9,056 bytes. These are size demonstrations on the
actual minute sample, not predicted month sizes or end-to-end latency gains.
Dictionary types require compatible schema/reader handling. Compression also
has CPU costs that need measurement.

This explains the modest raw-body reduction: March is 8.87 GB as NDJSON and
7.56 GB as the current Arrow stream, about 15% smaller. Most repeated tag text
is still being sent. The recorded gzip sizes are computed after download; the
server sends neither format compressed. Binary IPC by itself does not imply
an order-of-magnitude wire-size reduction.

#### Most curl time is outside the decoder

For March Arrow in `exp55_curl_reused_engine.json`:

- first byte: **101.36 s**;
- complete download: **120.77 s**;
- DuckDB count after download: **1.74 s**.

About **84%** of the download time elapses before its first byte, consistent
with the blocking `ORDER BY timestamp` and scan/planning work preceding output.
This is a timing boundary, not a CPU profile of the sort alone. Eliminating
all remaining output time while leaving that 101-second stage unchanged would
only improve Arrow's own download by about 19%. The cold minute is even more
concentrated before its first byte (about 0.383 out of 0.384 seconds).

The count benchmark uses DuckDB's optimized JSON reader. On March that reader
already processes the saved NDJSON in 1.49 seconds; there is little total latency
to remove from that step. `count(value), count(meta)` also does not expand tags
into pandas columns or parse the Arrow JSON-scalar strings into application
values. It does not measure the workload of `_load_arrow`.

For hot data, both formats still pass through the Flux row iterator and Go
record construction. Arrow accelerates response batching, but does not convert
that upstream path into a columnar scan. Its roughly 1.5x curl improvement is
therefore compatible with the implementation.

#### The Python loader rebuilds rows and temporarily holds many copies

`sage-data-client/query.py:_load_arrow` does all of the following:

1. `read_all()` retains the complete Arrow response.
2. `_arrow_values` calls `to_pylist()` and `json.loads` for each scalar.
3. `_meta_columns` calls `to_pylist()`, parses each metadata object, copies its
   entries into another Python dictionary, and retains one dictionary per row.
4. It walks that entire dictionary list once per metadata key to build Python
   column lists, which pandas then converts again.

Only timestamp and name explicitly preserve their Arrow columns through this
path. Even when pandas ultimately uses Arrow-backed strings for metadata,
the intermediate Python dictionaries, strings, and lists have already been
allocated. The claimed small dataframe-copy step is not what the month results
measure.

The recorded March Arrow server time is **133.13 s**, while dataframe completion
is **1,442.34 s**: roughly **1,309 seconds elapse after the server finishes**.
The comparable May figures are 104.37 and 1,559.11 seconds. These are elapsed-time
observations, not an exact CPU-time decomposition, but clearly locate the main
remaining delay on the client side. The benchmark's memory samples cover the
API container, not the Python process, so they cannot rule out client paging.

#### Independent local profile and prototype

`exp56_arrow_client_costs.py` repeats the saved 698-row response into controlled
fixtures, then runs each decoder in a fresh process and samples its RSS. The
fixture deliberately repeats metadata; it is not a recreation of the month's
metadata cardinality. The current decoder is the actual `_load_arrow` in this
workspace. Versions: pandas 3.0.6, PyArrow 25.0.1.

On 200,000 rows, cProfile observes **400,000 `json.loads` calls**.
`_meta_columns` takes 0.641 seconds of the profiled loader's 1.018 seconds;
`_arrow_values` takes another 0.243 seconds. Profiling overhead is excluded
from the separate elapsed measurements below.

An experiment-only prototype dictionary-encodes metadata on the client,
parses each distinct JSON object once, and expands tag columns with Arrow
operations. It deliberately keeps the existing per-row value decoder to isolate
metadata work. Sample dataframe values match the current loader; metadata dtype
representations differ. Production client and server code were not changed.

| Fixture rows | Current loader | Metadata prototype | Current peak client RSS | Prototype peak client RSS |
|---|---:|---:|---:|---:|
| 200,000 | 0.585 s | 0.093 s | 536 MB | 227 MB |
| 1,000,000 | 2.866 s | 0.419 s | 2,171 MB | 714 MB |

The current one-million-row final dataframe is only **262 MB**, while its peak
RSS exceeds 2 GB. At month scale, object allocation/GC and memory pressure are
credible contributors to the much larger slowdown. **Paging was not measured
in exp55**, so attributing an exact portion of the 24-minute client time to swap
would be speculation. Measure client RSS, page faults, and swap as well as API
memory on the next large run. Do not extrapolate the prototype's 6.8x fixture
speedup to a month with different metadata cardinality.

Script: `tiered_system/experiments/exp56_arrow_client_costs.py`.
Artifact: `tiered_system/experiments/results/exp56_arrow_client_costs.json`.
Reproduce using a small saved response from the current API schema:

```sh
python -I sage-data-rework/tiered_system/experiments/exp56_arrow_client_costs.py \
  /path/to/cold-minute.arrows --rows 200000 1000000
```

#### What to prioritize

The next optimization should be the Python decoder: bound intermediate work
by batches, avoid a metadata dictionary for every row, decode repeated metadata
once where repetition exists, and keep resulting columns in Arrow-compatible
arrays. The experimental dictionary approach needs mixed/high-cardinality,
null/missing-tag, and type compatibility tests before adoption. Returning a
complete dataframe still requires memory for that complete dataframe.

A further wire-schema revision can carry typed values and structured metadata
instead of JSON text, so both sides stop undoing the benefits of columnar data.
Use dictionary encoding and/or IPC compression only after measuring their cost
with representative data and consumers. Faster cold query planning/layout or
sorting is a separate optimization from response encoding.

Finally, the May Python-client times cannot establish a relative format speedup:
NDJSON returned **19,263,320** rows and Arrow **30,388,936**. The NDJSON timeout
and incomplete result are already recorded. Server times from the Python-client
runs also include socket backpressure: March NDJSON takes 1,765 seconds there
versus 279 seconds with curl. That is not an isolated measurement of server CPU.

#### Python decoder optimization (exp57)

Implemented in `sage-data-client/src/sage_data_client/query.py`. This supersedes
exp56's recommendation to optimize the client. The exp56 figures above describe
the decoder before this change; exp56 imports the workspace loader, so use
exp57's frozen legacy implementation for reproducible before/after comparisons.

The loader now consumes IPC record batches instead of `read_all()`. Python
conversion is limited to at most 65,536 rows at a time, including subdivision of
oversized incoming batches. Within each slice, Arrow dictionary encoding lets
it parse each distinct JSON value and metadata object once. Metadata expansion
uses Arrow `take` and retains native string arrays rather than a Python tag
list per column over the complete response. There is no growing global cache
of metadata objects. Retained timestamps and names are copied out of IPC
message buffers so they cannot accidentally retain the raw JSON payload.

Value inference happens once after all batches arrive, preserving mixed
numeric/string/bool/null values and large integers. UTC timestamps, nanosecond
precision, column order, and missing/late tags are covered by offline tests.
Metadata columns now explicitly use nullable `string[pyarrow]`, so code that
inspects exact dtypes or missing-value sentinels may observe a change; use
`isna()` for missing values. The earlier Arrow MAP metadata schema also works.
This remains an API returning a complete dataframe: the final result and the
accumulated value column still need memory proportional to the row count.
An individual incoming IPC message must also fit in memory.

##### Controlled decoder measurements

`exp57_arrow_decoder_optimized.py` freezes the original decoder and compares it
with the workspace implementation in fresh processes. Timing includes decoding
and dataframe construction, excluding imports and fixture generation. Parent
sampling measures client peak RSS every 5 ms. pandas 3.0.6, PyArrow 25.0.1.

Fixtures repeat the same saved 698-row API sample as exp56. The **unique** case
adds a unique string tag to every row, forcing every metadata JSON object to
be parsed; the measurement values and remaining tags still repeat. This is
neither a full-month replay nor a network/server benchmark. Fixture values
match the legacy decoder on the saved sample and a 10,003-row slice of each
fixture, allowing the intentional metadata dtype difference.

At the cold server's **2,048-row IPC batch size**, with **1,000,000 rows**:

| Metadata | Legacy decoder | Optimized decoder | Legacy peak RSS | Optimized peak RSS |
|---|---:|---:|---:|---:|
| Repeated | 2.898 s | 0.118 s | 2,146 MB | 424 MB |
| Every object unique | 3.415 s | 1.517 s | 2,009 MB | 464 MB |

That is approximately **24.6x** and **2.25x** faster respectively, with peak RSS
reduced by approximately **80%** and **77%**. These are single controlled runs;
RSS includes the interpreter and loaded libraries. Separate 65,536-row batch
measurements are recorded in the default exp57 artifact. Performance depends
on cardinality and incoming batch size.

##### Live rerun

`exp58_arrow_decoder_live.py` ran the same windows through the workspace
`_load_arrow` against the demo API, one warmup and the median of two measured
runs for the short queries, one run for each month. Newline JSON was left on
the earlier client pass. Arrow row counts matched that pass on every short
window and matched the full month counts: 30,388,936 in May and 25,940,207
in March.

| Query | Previous Arrow | This Arrow | Server | Client RSS | Major faults |
|---|---:|---:|---:|---:|---:|
| Hot May, one name | 1559.1 s | 104.5 s | 103.6 s | 654 MB | 0 |
| Cold March, one name | 1442.3 s | 137.1 s | 136.4 s | 2.01 GB | 123 |

On both months the dataframe exists within about a second of the server
finishing. The earlier gap, roughly 1,450 s in May and 1,310 s in March, was
the old loader. March's 123 major faults are the only ones recorded; the
client was not swapping through the month. The API peaked at 3.62 GB during
the March call. Result:
`tiered_system/experiments/results/exp58_arrow_decoder_live.json`.

Reproduce locally using a saved response:

```sh
python -I sage-data-rework/tiered_system/experiments/exp57_arrow_decoder_optimized.py \
  /path/to/cold-minute.arrows --rows 1000000 --batch-size 2048 \
  --output sage-data-rework/tiered_system/experiments/results/exp57_arrow_decoder_optimized_2048.json
```

Artifacts:
- `tiered_system/experiments/results/exp57_arrow_decoder_optimized.json`
- `tiered_system/experiments/results/exp57_arrow_decoder_optimized_2048.json`

Validation: **18 offline tests pass** (12 decoder/transport tests plus six
existing NDJSON/query regression tests). These include many distinct tags,
missing/null tags, reordered dictionary entries, cross-batch scalar inference,
invalid/truncated responses, and mocked Arrow/NDJSON transport negotiation.
Live HTTP tests could not run in the local sandbox because socket binding is
blocked. No container changes, Pelican/NRDStor writes, git commits, or pushes
were needed for this optimization.

### Arrow IPC compression: implementation and local comparison (exp59/exp60)

Implemented configurable **none / Zstd / LZ4** compression in the API's shared
Arrow writer. Both record-built hot responses and directly forwarded cold
batches use it, without changing the schema. Compression operates on each IPC
batch's buffers, not the entire HTTP response; PyArrow decodes it automatically.
The Python client needs no production changes for compression.

Configuration:

- Operator setting: `ARROW_COMPRESSION=none|zstd|lz4`, or the
  `-arrow.compression=none|zstd|lz4` flag. The default is **zstd**. The demo
  compose file forwards this variable. A request cannot select a different codec.
- Each Arrow response echoes the selected mode in `X-Arrow-Compression`.
- Invalid startup configuration fails startup. NDJSON ignores this setting.
- This is IPC buffer compression, so the server does **not** advertise it as
  HTTP `Content-Encoding`. The media type remains the Arrow stream type.
- Compression uses one worker per request. Large queries stay streaming, but
  codecs need per-batch workspace and CPU; concurrent requests multiply costs.

#### Local experiment

`exp59_arrow_ipc_compression.py` drives the **production Go writer** through
`TestArrowCompressionProbe` in a fresh process, then decodes each output with
the optimized workspace Python client in another fresh process. It measures
encoding elapsed/CPU time, completion of the first serialized IPC batch,
decoding elapsed/CPU time, bytes, and sampled process RSS. Full dataframe hashes,
columns, and dtypes match the uncompressed result across all modes and runs.

The experiment uses 200,000 and 1,000,000 rows, 2,048 and 65,536 row IPC batches,
and two metadata fixtures. The repeated fixture repeats the saved 698-row cold
response; the unique fixture adds a deterministic SHA-256 string tag per row.
The latter forces distinct metadata with a relatively incompressible tag value;
other tags and measurement values still repeat. Each configuration has three
fresh-process runs; the table reports medians. Codec order rotates each repeat.
These are controlled fixtures, **not a live month replay**. Encoding reads an
existing IPC fixture and writes a temporary file: it excludes DuckDB scanning,
sorting, hot-record JSON construction, and network transfer. The first-batch
metric here is serialization completion, not network time to first byte.

**Dependency limitation:** the sandbox cannot download pinned Arrow Go v18.5.1.
The isolated writer and service tests used cached **Arrow Go v18.8.0**, with
cached Zstd (`klauspost/compress` v1.19.2) and LZ4 (v4.1.29), through a temporary
modfile. Go 1.26.5, macOS arm64; pandas 3.0.6 and PyArrow 25.0.1. Workspace
`go.mod`/`go.sum` were not upgraded. The artifact records the binary's complete
build information. Validate and measure the pinned dependencies on the VM;
codec versions can change costs and sizes.

For **1,000,000 rows in 2,048-row batches** (cold batch size):

| Metadata | Codec | IPC size | Encode elapsed | Encode CPU | Python decode | First batch complete |
|---|---|---:|---:|---:|---:|---:|
| Repeated | None | 292.18 MB | 0.061 s | 0.082 s | 0.123 s | 0.20 ms |
| Repeated | Zstd | 13.91 MB | 0.102 s | 0.127 s | 0.147 s | 1.02 ms |
| Repeated | LZ4 | 17.14 MB | 0.094 s | 0.150 s | 0.136 s | 0.28 ms |
| Every object unique | None | 379.18 MB | 0.070 s | 0.104 s | 1.587 s | 0.21 ms |
| Every object unique | Zstd | 50.03 MB | 0.539 s | 0.667 s | 1.603 s | 1.91 ms |
| Every object unique | LZ4 | 85.84 MB | 0.207 s | 0.274 s | 1.549 s | 0.53 ms |

Zstd saves approximately **95% / 87%** of transferred bytes in these two
fixtures. LZ4 saves **94% / 77%**. LZ4 has substantially less encoding CPU cost
on the unique-metadata fixture, while Zstd makes the smaller output. Small
elapsed differences, including apparently faster decoding under compression,
should not be overinterpreted. The measured batch-size effect is also material:
with 65,536-row unique-metadata batches Zstd produces 48.58 MB, but the first
batch completes at 19.77 ms rather than 1.91 ms; peak encoder RSS rises from
about 37 MB to 203 MB. Keep cold batch sizes unchanged for this first comparison.

Compression does not shrink the final dataframe. Process RSS in the artifact
includes interpreter/runtime startup and dataframe checksum validation; it is
not solely transient codec memory. The sum of separately measured encoding and
decoding times is a local serial cost, not predicted end-to-end elapsed time.

**Conclusion:** both codecs warrant live measurement. Zstd is a strong candidate
when transfer size matters; LZ4 is a useful alternative when API CPU or batch
latency matters. These results do not justify an unconditional default switch.
Compression will not remove the existing cold scan/sort wait before batches
become available. Typed values and structured metadata remain separate future
schema experiments.

Artifact: `tiered_system/experiments/results/exp59_arrow_ipc_compression.json`.
Reproduce with the workspace's pinned Go dependencies available:

```sh
(cd beehive-data-api && go test -c -o /tmp/sage-arrow-compression.test)
python -I sage-data-rework/tiered_system/experiments/exp59_arrow_ipc_compression.py \
  /path/to/cold-minute.arrows --probe-binary /tmp/sage-arrow-compression.test
```

Validation: **19 selected Go tests pass**, plus compression round-trip subtests
for both records and direct batches (using the cached dependency caveat above).
The environment-only encoding probe is skipped in ordinary tests and ran 72
times in exp59. **19 offline Python tests pass**, including a new mixed-value,
large-integer, missing-tag test across Zstd/LZ4 IPC. The live harness was checked
with mocked responses for all codecs, first-batch timing, and rejection of an
old server that does not confirm the requested codec. The full pinned Go suite
and native DuckDB integration tests have not run in this sandbox.

#### Live VM comparison

`exp60_arrow_compression_live.py` uses the same hot/cold windows as exp58. The
demo API was rebuilt with the compression writer. The server default stayed
`none`; each request set `X-Arrow-Compression`. Short queries are the median of
3 runs after one warmup. Each month is one run. Row checksums, column order,
and dtypes matched across the three codecs on every window. Result:
`tiered_system/experiments/results/exp60_arrow_compression_live.json`.

| Query | None | Zstd | LZ4 | None body | Zstd body | LZ4 body |
|---|---:|---:|---:|---:|---:|---:|
| Hot May, one name | 113.7 s | 103.5 s | 104.5 s | 8.86 GB | 535 MB | 671 MB |
| Cold March, one name | 122.8 s | 112.3 s | 120.6 s | 7.56 GB | 431 MB | 580 MB |

Zstd cuts those bodies by about 94%, LZ4 by about 92%. March still waits about
99–104 s for the first batch, so compression does not remove the scan and sort.
After that wait, the smaller body finishes sooner: Zstd's March total is 10.6 s
under the uncompressed run, and May's is 10.2 s under. Client peak RSS stayed
near 4.8–5.0 GB for May and 3.6–4.1 GB for March. Major faults were 0 on May,
25 for March Zstd, and 210 for March LZ4. API samples every 15 seconds peaked
at 4.96 GB during March, with CPU touching the container's 2 CPU cap while a
compressed batch was encoded. The 14 GB cap was not approached.

Short windows show the same size cut with little change in time. The cold hour
of all names is 1.789 s and 120 MB uncompressed, 1.637 s and 5.37 MB with Zstd,
and 1.671 s and 6.73 MB with LZ4.

The server default is now `zstd`. The request override used for this comparison
has been removed. `ARROW_COMPRESSION=none|zstd|lz4` remains the operator
setting, and Arrow responses still echo the selected codec.

`exp60_arrow_compression_live.py` uses the same hot/cold windows as exp58.
The comparison above requested each codec. The script now records the codec
the server selected. It runs each request in a fresh Python process, warms
short queries once, and records bytes, first body byte,
first decoded IPC batch, dataframe completion, client CPU, major faults, and
peak client RSS. It checks all reconstructed rows using order-independent
sum/XOR row checksums, plus row count, column order, and dtypes; tie ordering
within equal timestamps is not guaranteed by the query. This is a consistency
check, not a cryptographic proof of row multiset equality. API CPU/memory need
to be collected externally on the VM. Month runs are opt-in and single runs
by default; short runs use three repetitions. Completed runs are saved as they
finish. The harness refuses a server that silently ignores the compression
header, avoiding false comparisons against an old image.

The table above is that run, with `--include-months`, after rebuilding only
the API and leaving `ARROW_COMPRESSION=none`. Reproduce it with the demo's
cold environment configured:

```sh
(cd sage-data-rework/tiered_system/demo && \
  docker compose --profile cold up -d --build --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp60_arrow_compression_live.py
# Add --include-months to include the full March/May windows.
```

For a quick smoke measurement, add `--match '1 min, one name' --reps 1`. The
script records the codec named by the response header. Set
`ARROW_COMPRESSION=none` or `lz4` and recreate the API to measure another
codec. Existing client queries receive Zstd IPC without asking for it.

### Typed Arrow values, schema v2 (exp61/exp62)

**Status, 2026-10-22: experimental only; not served by the public API.**
The following implementation notes and measurements describe the prototype.
Its multiple value fields do not meet the requirement to preserve the public
wire schema for direct HTTP consumers. The HTTP handler now always serves
`timestamp`, `name`, `value`, `meta` with the existing Arrow types; the client
no longer advertises `sage-schema=2`. Existing prototype parameters cannot
select the experimental schema. Zstd and the batched decoder remain enabled.
The exp61 fixtures are retained for research; exp62 is a historical prototype
comparison and will reject the current server's unchanged public schema.


The next improvement is implemented in both `beehive-data-api` and
`sage-data-client`: native scalar value columns replace JSON value text for
normal observations. **Metadata remains JSON and Zstd remains the server
compression default.** Neither Iceberg storage layout nor the returned pandas
column names change.

#### Wire format and compatibility

An Arrow stream's schema is fixed, while Sage queries may contain several
measurement types or cross hot/cold tiers. Schema v2 therefore uses nullable
parallel scalar columns, with at most one non-null value field per row:

| Wire field | Arrow type | Meaning |
|---|---|---|
| `timestamp` | timestamp[ns] | UTC instants, as before |
| `name` | string | Measurement name |
| `value_float` | float64 | Floating measurement |
| `value_int` | int64 | Signed integer |
| `value_uint` | uint64 | Unsigned hot measurement |
| `value_bool` | bool | Boolean hot measurement |
| `value_str` | string | String measurement, without JSON quoting |
| `value_json` | string | Compatibility fallback for unusual hot values |
| `meta` | string | Existing JSON tag object |

All scalar fields null means a null measurement. Cold storage currently has
float/int/string columns; uint/bool/JSON slots are typed nulls on that path.
The cold SQL retains float-before-int-before-string precedence if more than one
storage field is populated, matching v1. It removes `to_json` for values while
preserving `to_json(meta)`, timestamp precision, filters, and ordering. The hot
builder appends native scalars. Tier-spanning results share the same negotiated
schema for the entire stream.

The updated Python client automatically advertises:

```text
Accept: application/vnd.apache.arrow.stream; sage-schema=2,
        application/vnd.apache.arrow.stream;q=0.9,
        application/x-ndjson;q=0.1
```

The server replies with `Content-Type: application/vnd.apache.arrow.stream;
sage-schema=2` when v2 was selected. Unversioned Arrow stays v1 for existing
clients. The new client also reads v1 from an older server. This is automatic
schema negotiation in the SDK; callers keep using `query()` without an extra
option. Compression remains an operator setting, independent of this revision.

The Python decoder reconstructs the existing single `value` column. Native
numeric queries (including mixtures of signed integers and floats) fill NumPy
arrays directly. Mixed strings/bools/large unsigned integers are inferred only
after the whole response, preserving the existing pandas dtype and avoiding
premature integer rounding. It retains copies of only active scalar buffers,
so original IPC message buffers containing JSON metadata can be released.
Overlapping value fields and invalid typed field schemas are rejected.

Compatibility details tested against actual Go v1/v2 outputs include int64
limits, uint64 max, nulls, booleans, Unicode, late tags, large mixed integers,
JSON containers, and Go's float formatting. In v1, an integral hot float below
1e21 can be JSON-encoded without a decimal point and read as a Python integer.
V2 preserves that behavior for safe native conversions. Integral hot floats
above 2^53 retain JSON fallback because Go's shortest decimal spelling can
differ from the exact integer represented in binary. Float32, `json.Number`,
custom JSON values, and invalid UTF-8 strings also retain JSON fallback rather
than silently changing legacy semantics. Ordinary native floats/ints/strings
need no JSON value conversion.

#### Local comparison

`exp61_arrow_typed_values.py` exports the **actual production cold SQL
projections** and mixed-value fixtures from a Go test probe. In fresh processes,
it executes each projection with local DuckDB, writes IPC batches, passes the
batches through the **production Go Zstd writer**, and decodes with the workspace
Python client. Each configuration has three runs with alternating schema order.
Full dataframe hashes, dtypes, and columns match for both versions. Direct
Go-generated mixed hot fixtures also match with `assert_frame_equal`.

This experiment uses synthetic local one-million-row fixtures, two DuckDB
threads, a 512 MB DuckDB limit, five repeated metadata tags, and 2,048-row
batches. Fixtures are repeated numeric values, distinct numeric values, and
mixed float/int/string values including integers above 2^53. It does **not**
scan Iceberg or exercise network transfer. SQL timing includes Python DuckDB
execution and uncompressed IPC writing; it does not prove the native Go C API's
first-batch behavior. A native tagged regression test covers that path when run
on the VM.

Medians for **1,000,000 rows**, with **Zstd in both versions**:

| Fixture | Schema | Compressed IPC | SQL + IPC write | Go IPC encode | Python decode |
|---|---|---:|---:|---:|---:|
| Repeated | v1 JSON values | 14.24 MB | 0.283 s | 0.078 s | 0.097 s |
| Repeated | v2 typed values | 10.47 MB | 0.286 s | 0.093 s | 0.096 s |
| Unique | v1 JSON values | 14.75 MB | 0.289 s | 0.095 s | 0.495 s |
| Unique | v2 typed values | 11.54 MB | 0.287 s | 0.108 s | 0.096 s |
| Mixed | v1 JSON values | 15.34 MB | 0.284 s | 0.097 s | 0.456 s |
| Mixed | v2 typed values | 12.01 MB | 0.300 s | 0.126 s | 0.128 s |

Distinct numeric values decode approximately **5.2x faster**; mixed values
approximately **3.6x faster**. The repeated-value decoder is essentially tied,
because v1's existing dictionary optimization already avoids repeated parsing.
Compressed IPC is approximately **22–26% smaller** in these fixtures, beyond
Zstd alone. More wire columns slightly increase the Go encoding cost and can
increase cold projection width. SQL time is similar locally; that is not proof
that the full-month sort has the same resource cost.

The final dataframe sizes match exactly: this is not a smaller final-result
representation. Whole-process peak RSS in the artifact includes imports and
full-frame hashing, and does not demonstrate a material memory reduction. The
main measured gain is value decoding CPU, especially with distinct numbers.
The earlier live client already finished close to the server, so do not
extrapolate a 5x decoder improvement into a 5x end-to-end month speedup. The
cold scan and sort remain independent work.

Versions: DuckDB 1.5.6, pandas 3.0.6, PyArrow 25.0.1. As with exp59, the local
selected Go code compiled against cached **Arrow Go v18.8.0** through a temporary
modfile; the pinned v18.5.1 and DuckDB Go bindings are unavailable offline.
Workspace Go dependencies were not changed. The artifact records build info.

Artifacts and reproduction:

```sh
(cd beehive-data-api && go test -c -o /tmp/sage-arrow-typed.test)
python -I sage-data-rework/tiered_system/experiments/exp61_arrow_typed_values.py \
  --probe-binary /tmp/sage-arrow-typed.test
```

Result: `tiered_system/experiments/results/exp61_arrow_typed_values.json`.
Validation: **25 offline Python tests pass**, including new typed/legacy
comparisons across batch boundaries, nulls, negative zero, numeric limits,
uint inference, mixed values, malformed/overlapping fields, copied buffers,
Zstd, new-client/old-server fallback, and native numeric decoding without JSON.
**23 selected Go tests pass** using the cached dependency caveat above;
additional environment-only probes generate actual compressed v1/v2 hot
responses and exercise production batch encoding during the experiment.
The full pinned suite and the new `TestStreamingColdTypedArrow` native DuckDB
tagged regression test still need to run in the VM-capable environment.

#### VM rerun

`exp62_arrow_typed_live.py` compares v1 and v2 against the same API, with
compression held fixed. It reuses exp60's query windows/timers and runs each
request in a fresh process. Short queries use a warmup for each schema and three
measured repetitions with alternating schema order. Month queries are opt-in
and single runs by default. Row count, order-independent row checksums, column
order, dtypes, and codec must match. It records first body byte, first decoded
batch, dataframe completion, bytes, client CPU/faults, and sampled client RSS;
API resource usage should be sampled externally as in exp60. The harness
rejects an old server that returns v1 after v2 was requested. Its schema checks,
compressed reads, and first-batch timing were validated with mocked responses.

Rebuild the API and use the updated workspace client:

```sh
(cd sage-data-rework/tiered_system/demo && \
  docker compose --profile cold up -d --build --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp62_arrow_typed_live.py \
  --match '1 min, one name' --reps 1
# Drop --match/--reps for all short windows; add --include-months for March/May.
```

The API at `127.0.0.1:10000` was not reachable from this session, so no live
result is claimed and no VM containers were changed. No git commits/pushes or
Pelican/NRDStor writes were made. Structured metadata remains the next separate
experiment; this change deliberately retains its existing representation.

### Public schema contract restored (2026-10-22)

Preserving the dataframe after SDK reconstruction is insufficient: direct HTTP
consumers must receive the same single `value` field, with the existing type
and meaning, from hot, cold, and combined queries. Ordinary unversioned HTTP
responses had remained unchanged, but the opt-in multi-column prototype still
introduced a second public contract. It is no longer selectable via HTTP.

- Arrow keeps exactly `timestamp`, `name`, `value`, and `meta`. `value` remains
  a JSON-scalar string and `meta` remains the existing JSON-object string.
- NDJSON retains the same four fields and its existing native JSON values.
- The Sage client requests the ordinary Arrow media type. No client-specific
  value reconstruction is necessary to understand the public HTTP schema.
- Zstd remains the server default. Batched decoding and metadata/value JSON
  deduplication remain in place. Typed-column code and exp61 results remain
  experimental helpers, not a served response format.

Regression coverage now exercises the HTTP handler directly for hot records,
native cold batches, and a tier-spanning response, using NDJSON, ordinary
Arrow, and the obsolete prototype parameter. Every Arrow case must have the
exact original field types; every JSON row must have the same four fields.
The prototype request cannot expose `value_float`, `value_int`, or other
storage fields. Python tests also assert that `query()` does not advertise
the experimental schema.

A single Arrow union/struct `value` field would preserve its name, but would
still change its type and reader requirements. It therefore is not a
transparent replacement for the existing string field. Keeping the exact
wire schema means the measured typed-column decoder speedups cannot be
claimed for the public endpoint. Further transparent improvements should
target query execution, scan/sort work, internal conversion costs, or
compression rather than exposing storage representations to callers.

Validation after this correction: **24 selected Go tests pass**, including all
nine direct HTTP tier/format combinations, and **25 offline Python tests pass**.
The Go run used the same temporary cached Arrow v18.8.0 dependency setup noted
above; the pinned/native DuckDB integration suite remains pending. This change
is in the workspace only: no running VM was rebuilt, and no git commits,
pushes, or Pelican/NRDStor writes were made.
>NOTE: I might completely revert the code and remove all of the type arrow code soon. Still thinking about it.

### Cold execution: metadata assembly dominates the local profile (exp63/exp64)

Implemented a server-only SQL optimization for ordinary cold extracts. The
public schema stays `timestamp`, `name`, `value`, `meta`; values and metadata
keep the existing JSON text representation in Arrow, and NDJSON keeps its
existing JSON fields. Hot storage, Zstd, and the Python decoder are unchanged.

#### What the profile found

The wait before the first batch is not a measurement of sort CPU alone.
`exp63_cold_execution.py` executes the production-generated SQL over the real,
read-only March 1 archive: 15 local Parquet files, filtered to `W0A0` and
`wxt.env.temp`, returning **1,048,491 rows**. This is archive data, not the
published Iceberg day file; its count must not be substituted for live Iceberg
counts. The local profile has no Pelican/network/catalog latency.

The baseline constructs a promoted-tag MAP with
`map_from_entries(list_filter(list_zip(...)))`, merges it into each row's raw
MAP, then serializes the merged MAP. This repeated work is much more expensive
than the sort in this local case. Median operator times from two fresh-process
runs (DuckDB operator timings are CPU/work measurements, not additive elapsed
latency):

| Operator group | Baseline | Optimized |
|---|---:|---:|
| Parquet scan | 0.394 s | 0.316 s |
| Projections, primarily metadata reconstruction | 5.113 s | 0.876 s |
| Sort | 0.067 s | 0.072 s |

Both plans keep the same filters, columns needed from storage, timestamp sort,
and matching result rows. This change does not claim new file/row-group pruning.
The scan-time difference is small relative to the projection improvement and
can include cache/measurement variation.

#### Implementation and contract preservation

For ordinary extracts, `coldResponseSource` reads raw metadata plus promoted
columns. `coldMetaJSON` combines them using DuckDB JSON functions instead of
constructing an intermediate MAP for every row:

1. Serialize the raw MAP, treating a null MAP as empty.
2. Build a JSON object for the promoted tag columns.
3. Merge that object into `{}` first, removing null promoted entries.
4. Merge the remaining entries into the raw metadata JSON.

The separate null-removal step matters: an absent promoted value must not
delete a raw metadata entry. Rows whose raw metadata already contains any
promoted key use the **original MAP construction** instead, preserving override
semantics and even JSON key order. The output is still VARCHAR JSON text.
Both ordinary Arrow and NDJSON cold projections use this helper.

Per-series `head`/`tail` queries retain the original merged-MAP path, including
its partition keys, so this optimization cannot change series grouping. The
experimental typed-value projection also remains separate and is not served.
`ORDER BY timestamp` and its existing tie-order guarantees are unchanged.
No storage files, partitions, or ingestion settings were rewritten.

#### Local execution and memory results

The experiment exports the **actual Go baseline/current SQL**, then runs each
in a fresh Python/DuckDB process with two threads and `memory_limit='512MB'`.
The baseline uses the retained original metadata assembly, rather than a
hand-written approximation. SQL execution and uncompressed IPC writing are
timed; full-row checksum validation is outside that timer. Mode order rotates
between repetitions. Results below are medians of **two runs**; process RSS is
the maximum sampled across both, including imports and subsequent validation.

| Query | Rows | Baseline total | Optimized total | Baseline first batch | Optimized first batch | Baseline / optimized RSS |
|---|---:|---:|---:|---:|---:|---:|
| Real March 1, temp + W0A0 | 1,048,491 | 5.694 s | **1.382 s** | 5.558 s | **1.258 s** | 714 / 715 MB |
| Real first minute, same filters | 698 | 0.234 s | 0.233 s | 0.234 s | 0.233 s | 169 / 174 MB |
| Synthetic wide unique metadata, spill stress | 2,000,000 | 5.495 s | **2.346 s** | 5.289 s | **2.171 s** | 776 / 765 MB |

The day extract is approximately **4.1x faster locally**, and the stress case
approximately **2.3x faster**. The minute case is scan/setup dominated and is
essentially unchanged. The stress case still spills: about 118 MB baseline
versus 128 MB optimized. This is a CPU improvement, not a promise that every
memory metric or spill volume decreases. DuckDB's memory setting is not an
RSS ceiling. A full-month live rerun remains necessary, including API CPU,
RSS, spill/disk usage, and first-batch latency under the existing container cap.

All variants match row counts, Arrow schema/types, full-row sum/XOR checksums,
and **exact metadata JSON strings**, while keeping timestamps nondecreasing.
A separate 240-row semantic fixture checks exact Arrow table equality for
null/empty metadata, null and empty promoted values, raw/promoted collisions,
case-sensitive keys, Unicode/escaping, int64 values above 2^53, and mixed
measurements. `head=1` and `tail=1` each reduce the fixture to 120 rows and match
the baseline for both Arrow and the NDJSON SQL projection.

A delayed-JSON candidate was also measured and rejected: it keeps the MAP
reconstruction work, makes the sort carry nested MAP data, and does not
materially improve total time. On the real day it takes 5.631 s with 741 MB
peak RSS and 17 MB spill, versus the baseline's 5.694 s, 714 MB and no spill.
The chosen change keeps the existing string-payload sort.

Versions: DuckDB 1.5.6, PyArrow 25.0.1, pandas 3.0.6. The selected Go SQL/test
probe uses the cached Arrow Go v18.8.0 temporary modfile setup described above;
pinned workspace dependencies were not changed. The native Go/DuckDB tagged
integration test still needs VM-capable validation. Its new
`TestStreamingColdMetadataOptimization` compares baseline and optimized native
streams, including fallback rows, and requires matching original API schemas,
exact record batches, and streaming execution.

Artifact: `tiered_system/experiments/results/exp63_cold_execution.json`, with
plans, operator timings, scan/filter details, spill/RSS, checksums, and build
information. All generated SQL, IPC, and spill files use isolated local temp
folders; original archive files are read-only. Reproduce:

```sh
(cd beehive-data-api && go test -c -o /tmp/sage-cold-execution.test)
python -I sage-data-rework/tiered_system/experiments/exp63_cold_execution.py \
  --probe-binary /tmp/sage-cold-execution.test --include-delayed --reps 2
```

Validation: **24 selected Go tests and 25 offline Python tests pass**, plus
the actual DuckDB semantic/profiling checks described above. The public HTTP
schema regression tests continue to pass. The live comparison is in the next
section.

#### VM rerun with the public schema

Both passes used `exp64_cold_execution_live.py` against the demo API (14 GB
cap, 2 CPUs, Zstd, public single-value schema). The baseline pass ran on the
image already serving, before the rebuild. The optimized image was rebuilt
after renaming the SQL helper to `coldMetaJSONExpr`: `coldMetaJSON` is already
the JSON parser in `cold_backend.go`, and the image build failed on that
redeclaration. Short queries are medians of three runs after one warmup.
March is one run. Every query matched the baseline on row count, columns,
dtypes, and full-row sum/XOR checksums. The March body is the same
430,985,448 bytes.

| Query | Rows | Baseline total | Optimized total | Baseline first batch | Optimized first batch |
|---|---:|---:|---:|---:|---:|
| Cold, 1 min, one name | 698 | 0.437 s | 0.388 s | 0.435 s | 0.383 s |
| Cold, 1 hour, one name | 43,509 | 0.691 s | 0.450 s | 0.637 s | 0.433 s |
| Cold, 1 min, all names | 6,352 | 0.508 s | 0.490 s | 0.501 s | 0.480 s |
| Cold, 1 hour, all names | 395,989 | 1.805 s | 0.900 s | 1.674 s | 0.777 s |
| Cold March, one name | 25,940,207 | 130.960 s | **58.194 s** | 114.502 s | **46.903 s** |

March is about **2.25×** faster end to end, and the first decoded batch arrives
about **2.4×** sooner (114.5 s to 46.9 s). The one-hour, all-names extract is
about **2.0×** faster. The one-minute windows stay near the baseline. The
one-hour, one-name optimized runs were 0.427 s, 0.450 s, and 0.917 s, so that
median includes one slow repetition.

`docker stats` every 15 seconds during March peaked at 3.24 GiB and 199% CPU
on the baseline image, and 3.39 GiB and 183% CPU on the optimized image.
Client RSS during the March checksum was 4.22 GB baseline and 4.34 GB
optimized (587 and 561 major faults). Client CPU was about 8 s on both.
Sampled API memory stayed near 3.3 GiB.

Artifacts:
`tiered_system/experiments/results/exp64_cold_execution_live_baseline.json`
and `exp64_cold_execution_live.json`.

```sh
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --include-months --label "baseline metadata projection" \
  --output sage-data-rework/tiered_system/experiments/results/exp64_cold_execution_live_baseline.json
(cd sage-data-rework/tiered_system/demo && \
  docker compose --profile cold up -d --build --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --include-months --label "optimized metadata projection" \
  --baseline sage-data-rework/tiered_system/experiments/results/exp64_cold_execution_live_baseline.json
```

No git commits or pushes, and no Pelican/NRDStor writes. The demo API image
was rebuilt for the second pass.

#### Next diagnostic: where March waits before its first batch (exp65)

The optimized VM run spends 46.903 of 58.194 seconds before the first decoded
batch. That observation alone does not identify sorting, Iceberg planning or
Pelican I/O as the cause. Added opt-in server diagnostics to measure the native
streaming path, plus `exp65_cold_query_profile.py` to summarize copied files.
No further query execution optimization is claimed yet.

Enable with `COLD_PROFILE_DIR` or `-cold.profile-dir`. Default is disabled;
the demo compose file forwards the environment variable. Each startup creates
an isolated `session-*` directory under that local parent, with unique
`query-*.duckdb.json`/`.timing.json` pairs per cold Arrow query. Directories are
0700 and files 0600. There are no changes to requests, public schema, codecs,
NDJSON, or Pelican storage. Setup/credentials run before enabling profiling;
profiles are connection-local. Raw DuckDB profiles contain query SQL, URLs and
filters; the report omits SQL and operator expressions. No HTTP header logging
is enabled. Copy diagnostics out before recreating the API container.

The timing sidecar records:

- Authentication, version-hint HTTP request, SQL construction and cold-engine
  queue wait separately. Engine initialization is recorded separately too;
  extension install/load belongs only to the first Arrow use of the process.
- Connection/secret setup, prepare, pending-result creation and execution,
  Arrow schema conversion, cumulative chunk fetching and native Arrow batch
  conversion. `prepare_seconds` includes binding/planning and can include
  Iceberg metadata HTTP reads. Execution/fetch can do scan and sort work.
- First native batch, native EOF and total backend lifetime as elapsed markers
  from entry into the cold backend. These markers overlap the individual
  phases. The client's decoded first batch also includes IPC/compression,
  socket and client work, so the two first-batch measurements need not match.
- Rows, batches and complete/failed/cancelled/incomplete status. The summary
  checks that a nonempty native profile was actually written after native
  result destruction/disconnect. Partial queries are excluded from bottleneck
  analysis; they can still have a DuckDB profile.

DuckDB's detailed JSON adds planner/optimizer metrics, scan/projection/sort
operator timings and cardinalities, reported bytes, peak buffer memory and
spill space. Operator seconds describe work across threads and can include
I/O waits: they are **not pure CPU**, not additive wall time, and cannot be
added to backend phases. Detailed planner/optimizer totals also overlap their
submetrics. Reported byte counts are not a complete HTTP traffic counter;
zero does not prove no Pelican reads. This diagnostic can identify where the
wait occurs and which operators do work; it cannot precisely divide scan time
between network, decompression and CPU. Backend lifetime additionally includes
consumer pacing and response encoding/backpressure. For tier-spanning queries
it can include time before the cold result is consumed.

Run on the VM from the workspace root. These commands only rebuild the API,
read the existing March data, and write diagnostics in isolated local folders:

```sh
# Control: current optimized image, profiling disabled.
(cd sage-data-rework/tiered_system/demo && \
  COLD_PROFILE_DIR= docker compose --profile cold up -d --build --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --include-months --match 'cold March' --month-reps 3 \
  --label 'optimized control, profiling disabled' \
  --output sage-data-rework/tiered_system/experiments/results/exp65_control_live.json

# Enable diagnostics and repeat identical queries/schema/Zstd.
(cd sage-data-rework/tiered_system/demo && \
  COLD_PROFILE_DIR=/tmp/sage-cold-profiles \
  docker compose --profile cold up -d --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --include-months --match 'cold March' --month-reps 3 \
  --label 'optimized, detailed server profiling' \
  --baseline sage-data-rework/tiered_system/experiments/results/exp65_control_live.json \
  --output sage-data-rework/tiered_system/experiments/results/exp65_profiled_live.json

# Unique local collection folder. Copy BEFORE disabling/recreating the container.
profile_copy=$(mktemp -d /tmp/sage-exp65-copy.XXXXXX)
docker cp demo-data-api:/tmp/sage-cold-profiles "$profile_copy/"
python -I sage-data-rework/tiered_system/experiments/exp65_cold_query_profile.py \
  "$profile_copy" \
  --output sage-data-rework/tiered_system/experiments/results/exp65_server_profiles.json

(cd sage-data-rework/tiered_system/demo && \
  COLD_PROFILE_DIR= docker compose --profile cold up -d --no-deps data-api)
```

These are sequential runs. Match files by SQL/window in the raw profile if
other users queried the server; the report's source paths retain pairing.
Compare first requests separately because they initialize the native engine;
use later repetitions for reused-engine behavior. Control vs profiled timings
also reveal instrumentation overhead and cache/run variation. If they differ
materially, alternate control/profiled passes before drawing conclusions.

Local validation uses DuckDB 1.5.6 with the actual optimized SQL exported by the
Go probe and the read-only March 1 archive, draining Arrow batches. It checks
that detailed JSON is emitted and setup SQL is absent, then runs the report
script with `--raw`. That mode explicitly marks results as DuckDB-only and
ineligible for native backend phase analysis. Artifact:
`tiered_system/experiments/results/exp65_local_profile_validation.json`.
It measures no Iceberg/Pelican access and is not an explanation for the VM's
46.9 seconds.

This validation drained **1,048,491 rows** in 2.189 s, first batch at 2.019 s.
The profile reported projection work 1.147 s, Parquet scan work 0.471 s and
sort work 0.101 s. These are one local validation run, not a performance
comparison or a prediction for March on the VM. Selected-file Go tests,
including public-schema/service regressions, pass with the race detector;
both report-script unit tests pass.

Go tests cover disabled profiling, isolated private paths, SQL quoting and
profiler setup order, status/EOF handling and idempotent report finalization.
Python tests cover operator work vs wall time, missing/partial profiles and
safe file pairing. A native tagged regression
`TestStreamingColdQueryProfile` drains the actual C streaming reader and checks
profile output without capturing setup SQL. Run with the pinned modules:

```sh
(cd beehive-data-api && go test -tags=duckdb_arrow \
  -run '^TestStreamingColdQueryProfile$' -count=1 ./...)
```

`TestStreamingColdQueryProfile` passed with the pinned modules
(`ok influxdb-data-api 0.649s`, Arrow Go 18.5.1).

#### VM collection

Profiling stayed off for the control and on for the second pass. Both used
the rebuilt API, Zstd, and the public schema, three March repetitions each.
Checksums, columns, and dtypes match between the passes. Medians:

| Pass | Total | First decoded batch |
|---|---:|---:|
| Profiling disabled | 57.659 s | 45.876 s |
| Detailed profiling | 55.460 s | 43.758 s |

Individual totals were 68.209 / 57.659 / 55.246 s with profiling off, and
55.460 / 56.232 / 53.399 s with profiling on. The gap sits inside that spread,
so the passes were not alternated. Engine initialization on the first profiled
query was 0.094 s. The demo container was returned to `COLD_PROFILE_DIR` empty
after the profiles were copied. API samples every 15 s peaked at 4.92 GiB
during the control and 4.82 GiB during the profiled pass.

The client's first batch lines up with the server marker
`first_batch_since_backend_start` (median 43.738 s). Median phases on the
three complete, eligible profiles:

| Phase | Median |
|---|---:|
| Authentication + version hint + SQL build | 0.047 s |
| Prepare, including planning | 0.502 s |
| Execute until the pending result can stream | 42.957 s |
| Fetch of the remaining chunks | 5.908 s |
| Native Arrow conversion | 0.716 s |
| Backend lifetime | 54.687 s |

DuckDB's own wall latency for those queries is 52.511 / 54.456 / 55.448 s,
with CPU time 85.7 / 89.8 / 92.3 s and blocked-thread time under 5 ms.
Operator work, which overlaps across the two threads and can include I/O
inside the scan:

| Operator | Work, three runs |
|---|---|
| Iceberg scan, 31 files | 47.8 / 49.3 / 51.6 s |
| Projection to `timestamp`, `name`, `value`, `meta` | 20.6 / 20.7 / 21.3 s |
| `ORDER BY timestamp` | 14.1 / 18.4 / 18.8 s |

The scan's estimated cardinality is 1,691,437,390 rows. It returns the same
25,940,207 rows as the response. Reported bytes read are 390 / 393 / 402 MB.
Peak buffer memory is 5.6–5.8 GB and the sort's temp directory peaks at
2.76–2.93 GB. `cumulative_rows_scanned` is 16,914,373,908, about ten times
the estimate; that counter is not a count of bytes fetched.

The time before the first batch is the ordered scan producing its first
chunk. Prepare and engine startup are a fraction of a second. After that
chunk, fetching and converting the rest of the result is about 6.6 s of
backend time. The scan, the metadata/value projection, and the spilling sort
all still do substantial work, and this profile does not split scan time
into Pelican waits, decompression, and CPU.

Artifacts: `results/exp65_control_live.json`,
`exp65_profiled_live.json`, and `exp65_server_profiles.json`.
No commits, pushes, remote storage writes, or `/node-data` access.
>NOTE: I might completely remove the profiling code. Still thinking about it.

#### Review of exp65 and next execution experiment

The profiles support targeting the ordered scan, rather than engine startup,
planning or native Arrow conversion. All three native first-batch markers are
within 0.8 s of `execute_pending_seconds`; native engine initialization is
0.094 s on first use and negligible thereafter. Preparation is about 0.5 s.
The complete profiles show no query failure or evidence of an allocation
failure. They do show resource pressure: the sort spills roughly 2.8–2.9 GB.
Neither the scan cardinality estimate nor the inflated rows-scanned counter
establishes how many physical rows were decoded. Likewise, the low
blocked-thread counter does not isolate HTTP wait time inside the scan.

The post-first-batch interval is **10.45–10.95 s**, not just the 6.10–6.64 s
spent in fetch plus native conversion. Subtracting those timers leaves
4.19–4.35 s outside them. That residual can include IPC/Zstd writing, consumer
pacing and socket backpressure; the current timers do not divide it further.
Native conversion alone is about 0.7 s, so eliminating it cannot explain a
large improvement in this workload. The operator work numbers overlap and
must not be interpreted as seconds that a change would directly remove from
wall latency.

The next candidate to benchmark is **bounded chronological execution** for
ordinary extracts: compare the current whole-month query against sequential
1-day, 3-day and 7-day windows. Each window retains its timestamp sort and the
same public four-column Arrow schema; concatenate windows within one Arrow
stream. The hypothesis is earlier first data and a smaller sorting working
set/spill volume, not reduced bytes or guaranteed faster total execution.
Extra planning/HTTP calls and less parallel work can offset the gains.

For a valid experiment, resolve and pin the same immutable Iceberg metadata
URL for all windows; keep authentication/engine reuse and compression fixed.
Use adjacent half-open intervals so boundary timestamps appear exactly once,
and retain the existing single-query path for head/tail/aggregations until
their whole-query semantics are handled separately. A file being time-sorted
by ingestion is insufficient reason to remove the query's explicit sort.
Compare row/schema/checksum equality, nondecreasing timestamps, first-batch
and total time, spill and process memory before adopting this execution mode.
This is a proposed experiment; exp65 has not measured it and no runtime
execution behavior was changed in this review.

#### DuckDB 2.0 assessment before windowed execution (2026-10-22)

Recommendation: benchmark DuckDB **2.0.0** against **1.5.6** before changing
the execution strategy. This is a release/code compatibility review, not a
2.0 performance result. Dependencies, runtime settings and storage remain
unchanged in this review.

The strongest candidate is asynchronous Parquet I/O: reads can overlap query
work rather than occupy the same worker. That targets exp65's largest operator,
the Iceberg scan (47.8–51.6 s of operator work). The inference is that our HTTPS
Parquet reads may benefit; the actual Iceberg/httpfs/Pelican combination still
needs measurement. This does not imply eliminating the blocking timestamp sort.
[Release announcement](https://duckdb.org/2026/10/21/announcing-duckdb-200),
[async I/O design](https://duckdb.org/2026/07/31/asynchronous-io).

The I/O docs specify separate `threads` and `async_threads` pools and automatic
read-ahead. With default `read_ahead_depth=-1`, memory management can shrink
read-ahead to one job under pressure. A positive fixed depth has no memory
budget. Since exp65 already spills, more I/O concurrency is not an unconditional
win. Keep two CPU workers and the same memory/container limits for the first
comparison; record effective async/pool settings rather than assuming defaults
from the host or CPU quota. Only then test modest async pool sizes such as
4/8/16. Avoid copying the documentation's large-machine tuning example.
[I/O settings](https://duckdb.org/docs/2.0/data/io).

Other changes have a weaker connection to the current workload. Better
Parquet pruning and expression execution merit measurement, but release-note
improvements do not establish that our exact filters benefit. `VARIANT` does
not automatically convert our existing scalar columns and MAP metadata or
remove the required JSON string response. Quack is an optional architecture
change; asynchronous reads do not require turning the API into a DuckDB server.
DuckDB's native storage-format change does not require rewriting the existing
Iceberg/Parquet warehouse: this API opens in-memory query engines.
[Release changes](https://github.com/duckdb/duckdb/releases/tag/v2.0.0),
[2.0 highlights](https://duckdb.org/2026/08/17/duckdb-20-highlights),
[migration guide](https://duckdb.org/docs/2.0/operations_manual/migration).

Concrete migration points found in this workspace:

- `cold_sql.go:promotedTagMap` and the Python reference `cold_backend.py` use
  `x -> x[2] IS NOT NULL`. Change to `lambda x : x[2] IS NOT NULL` before the
  engine upgrade: single-arrow lambdas are disabled by default in 2.0. A local
  1.5.6 check verified exact JSON equality across four null/empty/populated tag
  fixtures with the replacement; this is not a test on 2.0.
  [Lambda transition](https://duckdb.org/docs/2.0/sql/functions/lambda).
- Current Go pins are `duckdb-go/v2 v2.10506.0` and
  `duckdb-go-bindings v0.10506.0`. Upstream documents the 2.0 mapping as
  `v2.20000.0` / `v0.20000.0`; native library modules now come from
  `go.duckdb.org`. Update driver, bindings and resolved native modules together
  in an isolated candidate. Installing a macOS CLI does not upgrade the
  statically embedded API engine. The fetched install pages still displayed
  older versions, so verify exact tagged artifacts and `SELECT version()`;
  do not equate an unpinned install with a verified 2.0 build.
  [Go upgrade notes](https://github.com/duckdb/duckdb-go#upgrading-to-duckdb-v2),
  [bindings release](https://github.com/duckdb/duckdb-go-bindings/releases/tag/v0.20000.0).
- Our custom native path calls `PendingPreparedStreaming`, `ExecutePending`,
  fetch/chunk and Arrow conversion APIs. The 2.0 C reference still lists
  `duckdb_pending_prepared_streaming`, marked deprecated. There is no evidence
  here that upgrading requires abandoning streaming, but exact Go wrapper,
  native linking, Arrow ownership, cancellation and bounded-memory behavior
  remain build/test requirements. Do not replace it with a materializing API
  just to compile. [C API](https://duckdb.org/docs/2.0/clients/c/api#duckdb_pending_prepared_streaming).
- `profiling_mode='detailed'` is deprecated/no-op in 2.0; detailed information
  is always collected. Check profiler configuration and exp65 JSON metrics/tree
  compatibility against real output. Engine and `httpfs`/`iceberg` extension
  versions must be recorded per run; validate bearer-secret HTTPS access and
  direct metadata-URL reads. Direct `iceberg_scan` remains documented in 2.0.
  [Profiling migration](https://duckdb.org/docs/2.0/operations_manual/migration#removed-and-deprecated-settings),
  [Iceberg reads](https://duckdb.org/docs/2.0/core_extensions/iceberg/overview).

Validation sequence: build/test the candidate with `duckdb_arrow`, including
native streaming, cancellation, schema/metadata/head-tail semantics and spill
regressions. First compare local archive execution to separate engine/SQL
changes from remote I/O. Then alternate 1.5.6/2.0 API runs on the same pinned
March snapshot, with identical public schema, Zstd, CPU/memory and query.
Measure first batch, total time, process RSS, spill, operator work, row count,
timestamps, exact metadata fixtures, columns/dtypes and full-row checksums.
Test short cold and tier-spanning queries too. Keep the known-good image for
comparison. No storage layout change or writes to `/node-data` are needed.

Release docs recommend testing a shadow deployment and note continued 1.5
support through March 2027. That supports evaluating 2.0 now without treating
an immediate deployment as mandatory. No 2.0 binary was built or benchmarked
in this sandbox, and no dependency files were changed, committed or pushed.

##### DuckDB 2.0 migration candidate and evaluation harness (exp66)

Implemented the migration candidate after the assessment above. The native
build, local comparison, and live March measurement are in the results
section below.

The candidate pins `duckdb-go/v2 v2.20000.0`, bindings `v0.20000.0`, and the
new `go.duckdb.org/libduckdb/* v0.20000.0` native modules. The live GitHub API
confirmed tag `v2.20000.0` at `bb8380a152d28fb07f6771a762d1a35f9308fd4e`;
PR 217 is merged despite the stale fetched web page displaying Draft. The
bindings/native checksums were copied from the tagged driver's `go.sum`.
The VM subsequently resolved the driver package's own checksum with
`go mod tidy`; it is now present in `go.sum`. Do not treat selected Go tests
using a cached temporary module as verification of these dependencies; the
full native VM test result is recorded below.
[Tagged driver](https://github.com/duckdb/duckdb-go/tree/v2.20000.0),
[bindings migration](https://github.com/duckdb/duckdb-go-bindings/pull/116).

Implementation changes:

- Shared cold-engine settings retain `threads=2` and `memory_limit='3GB'`, add
  `async_threads=8`, and use memory-budgeted `read_ahead_depth=-1`. Eight is an
  initial candidate, not a measured optimum. Operator-only overrides are
  `COLD_ASYNC_THREADS` / `-cold.async-threads` (1..256), and
  `COLD_READ_AHEAD_DEPTH` / `-cold.read-ahead-depth` (-1 or 0). Positive fixed
  depths are not exposed because they bypass read-ahead memory budgeting.
  Both cold Arrow and NDJSON engines use the configuration; the demo forwards
  it. Existing container limits stay fixed. Zero disables lookahead, not async
  I/O. [Configuration](https://duckdb.org/docs/2.0/configuration/overview).
- Metadata SQL in Go and the Python reference uses `lambda x : ...`. Explicit
  `arrow_output_version='1.0'` retains the existing Arrow layout. No public
  field/type changes, typed-value wire format, storage rewrite, windowed
  execution, or Sage client changes were introduced.
- Startup checks/logs actual DuckDB 2.0.x. Native profile sidecars record the
  linked version independently using `LibraryVersion`. Profiling drops the
  deprecated detailed-mode setting. exp65 normalizes old flat and new grouped
  JSON, preserves numeric 2.0 metric groups and omits query SQL. Cumulative
  intermediate rows are not mislabeled as returned rows.
  [Profiler metric layout](https://duckdb.org/docs/2.0/dev/metrics).
- The tagged native bindings still export `PendingPreparedStreaming` and the
  Arrow conversion functions used by our code. Streaming stays in place.
  Added `TestStreamingDuckDB20Settings` to verify engine identity and settings
  across fresh connections; existing streaming/cancellation/schema tests must
  pass against the new native libraries.

Checks completed here: selected-file Go settings/profile/service/schema tests
pass with the race detector using cached Arrow 18.8.0 in a temporary modfile;
five exp65/exp66 Python tests pass; 25 offline data-client tests pass. Actual
Go-exported migrated SQL passes six exact Arrow-table comparisons on DuckDB
**1.5.6** (ordinary/head/tail, Arrow and NDJSON projections, null/empty metadata,
raw/promoted collisions, escaping, large integers and mixed values). These
check backward compatibility of the SQL change, not 2.0 execution. `diff --check`
passes. Validation status: `results/exp66_validation_status.json`.

Before changing dependencies, the current 1.5.6 Go sources, dependency files
and Dockerfile were copied to an isolated local folder. A portable copy is
`data/duckdb20-evaluation/duckdb156-source.tar.gz` (ignored local data; copy it
to the VM explicitly). It includes the same new native evaluation probe. No
stash was popped or modified, and neither repository was committed or pushed.

Build/resolve on the VM from the workspace root:

```sh
# Preserve an executable baseline without switching or resetting either repository.
eval_root=$(mktemp -d /tmp/sage-duckdb20-eval.XXXXXX)
tar -xzf sage-data-rework/data/duckdb20-evaluation/duckdb156-source.tar.gz -C "$eval_root"
(cd "$eval_root/duckdb156" && \
  go mod download && go test -tags=duckdb_arrow -c -o "$eval_root/duckdb156.test")

# Resolve and verify the actual 2.0 module graph and run the native regressions.
(cd beehive-data-api && \
  go mod tidy && go mod verify && \
  go test -tags=duckdb_arrow -count=1 ./... && \
  go test -tags=duckdb_arrow -c -o "$eval_root/duckdb200.test")

python -I sage-data-rework/tiered_system/experiments/exp66_duckdb20_evaluation.py \
  --binary "1.5.6=$eval_root/duckdb156.test" \
  --binary "2.0.0=$eval_root/duckdb200.test" \
  --reps 3 --profiles \
  --output sage-data-rework/tiered_system/experiments/results/exp66_duckdb20_evaluation.json
```

exp66 rotates version order for minute/day/spill-stress queries, drains the
actual Go native reader through the production Zstd IPC writer, and checks the
engine reported by each binary instead of trusting labels. Full-row checksums,
public Arrow schema and nondecreasing timestamps must match before timings
are compared. Unprofiled repetitions determine the medians; `--profiles` adds
separate diagnostic runs. Native database initialization and checksum validation
are outside the query timer. RSS is sampled over the native subprocess; profiles
provide buffer/spill metrics. Inputs are read-only local Parquet, so this phase
separates native engine/SQL differences from Pelican behavior. All generated
SQL, IPC and spill files use isolated local temporary folders. The default
512 MB engine limit intentionally stresses spilling; also rerun with
`--memory 3GB` for the deployed setting. Python still needs PyArrow, pandas,
NumPy, psutil and DuckDB for the shared experiment utilities; it does not run
or stand in for the native Go engine being timed.

After the native tests succeed, rebuild and measure the live candidate with
profiling disabled first:

```sh
(cd sage-data-rework/tiered_system/demo && \
  COLD_PROFILE_DIR= COLD_ASYNC_THREADS=8 COLD_READ_AHEAD_DEPTH=-1 \
  docker compose --profile cold up -d --build --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --label 'DuckDB 2.0 async=8 automatic read-ahead' --include-months --month-reps 3 \
  --baseline sage-data-rework/tiered_system/experiments/results/exp64_cold_execution_live.json \
  --output sage-data-rework/tiered_system/experiments/results/exp66_duckdb20_live.json
```

The exp64 baseline has both short windows and March; exp65's baseline has March
only. Inspect the cold-backend startup log for DuckDB 2.0.x and effective I/O
configuration. Historical timing comparisons are provisional because cache and
load can differ. For fresh alternating passes, build the preserved source into
`sage-data-api:duckdb156` and the candidate into `sage-data-api:duckdb200`.
A local compose override containing only
`services: {data-api: {image: "${DUCKDB_EVAL_IMAGE:?}"}}` lets the same demo
configuration use either image with `up --no-build --no-deps data-api`.
Alternate 1.5.6/2.0, then reverse order, saving separate exp64 outputs and
checking full-row signatures against the first pass. Do not change the
watermark, schema, codec, warehouse snapshot or resource limits between runs.

Collect a separate profiled pass using the exp65 procedure and updated reader;
copy profiles before recreating the container. Check `engine_version` is
`v2.0.0`, compare first-batch/total latency, scan/projection/sort work,
row-group metrics, peak RSS, buffer memory, spill and streaming-buffer metrics.
Sweep async=4/8/16 only after the fixed-setting version comparison, and return
profiling to disabled afterwards. Writes remain local diagnostics; no Pelican
or NRDStor writes and no `/node-data` access are required.

##### Measured

`go mod tidy` resolved the tagged 2.0 module graph. `go test -tags=duckdb_arrow ./...`
passes. Two cancellation tests now also watch the first fetch: DuckDB 2.0
returns the streaming result before a blocking aggregate runs, and the
interrupt still stops that fetch within the existing 50 ms deadline.

DuckDB 2.0 reads the archive timestamps as nanoseconds. A direct cast to
`TIMESTAMPTZ` rounds, so the cold SQL truncates with
`make_timestamp(epoch_ns(time)//1000)` and compares `epoch_us` bounds. That
keeps the microsecond instants 1.5.6 already returns. Local Arrow tables and
the live full-row checksums match across engines.

Local native runs use the preserved 1.5.6 test binary and the candidate
binary, the same SQL, read-only March 1 Parquet, and Zstd. Medians of three
rotated repetitions:

| Memory | Case | Rows | 1.5.6 total | 2.0 total | 1.5.6 first | 2.0 first |
|---|---|---:|---:|---:|---:|---:|
| 512 MB | Minute | 698 | 0.257 s | 0.226 s | 0.255 s | 0.225 s |
| 512 MB | March 1 | 1,048,491 | 1.463 s | 1.361 s | 1.283 s | 1.194 s |
| 512 MB | Spill stress | 2,000,000 | 3.042 s | 2.875 s | 2.183 s | 2.030 s |
| 3 GB | Minute | 698 | 0.251 s | 0.222 s | 0.250 s | 0.220 s |
| 3 GB | March 1 | 1,048,491 | 1.432 s | 1.352 s | 1.236 s | 1.178 s |
| 3 GB | Spill stress | 2,000,000 | 2.916 s | 2.740 s | 2.081 s | 1.939 s |

On local files the engine change is about 5–12%. The 2.0 process reported
`v2.0.0`, `async_threads=8`, and `read_ahead_depth=-1`.

The live API was rebuilt with profiling off, `async_threads=8`, and automatic
read-ahead. Startup logged `cold DuckDB v2.0.0 threads=2 memory_limit=3GB async_threads=8 read_ahead_depth=-1`.
Short queries are medians of three. March is the median of three. Every query
matched the optimized 1.5.6 exp64 checksums, columns, and dtypes.

| Query | Rows | 1.5.6 total | 2.0 total | 1.5.6 first batch | 2.0 first batch |
|---|---:|---:|---:|---:|---:|
| Cold, 1 min, one name | 698 | 0.388 s | 0.345 s | 0.383 s | 0.338 s |
| Cold, 1 hour, one name | 43,509 | 0.450 s | 0.755 s | 0.433 s | 0.739 s |
| Cold, 1 min, all names | 6,352 | 0.490 s | 0.490 s | 0.480 s | 0.481 s |
| Cold, 1 hour, all names | 395,989 | 0.900 s | 0.863 s | 0.777 s | 0.736 s |
| Cold March, one name | 25,940,207 | 58.194 s | **30.403 s** | 46.903 s | **22.012 s** |

The March runs were 43.848 / 30.286 / 30.403 s, with first batches 33.710 /
22.012 / 21.019 s. The one-hour, one-name runs were 0.755, 0.781, and 0.342 s.
`docker stats` during March peaked at 4.18 GiB and 200% CPU.

A profiled pass of the same March query stayed at a 29.648 s median and
recorded `engine_version=v2.0.0`. `execute_pending` is about 0.1 ms; the first
batch marker is 20.3 / 21.7 / 25.1 s. Operator work:

| Operator | Three profiled runs |
|---|---|
| Iceberg scan | 4.70 / 4.70 / 5.17 s |
| Projection to the public columns | 18.9 / 19.7 / 20.2 s |
| Sort by timestamp | 7.55 / 10.7 / 12.6 s |

DuckDB scanned 66 of 8,487 row groups and 25,940,207 rows. Reported bytes read
are 3.09–3.28 GB. Blocked-thread time is 4.5–6.0 s. Peak buffer memory is
3.02 GB, the temp directory peaks at 2.64–2.83 GB, and the streaming buffer
peaks at 11.6 MB. Query wall time is 26.6–33.0 s with 32.7–39.1 s of CPU time.
The scan's operator time is far below the 1.5.6 profile; the projection is
still about 20 s of operator work.

Warm March totals at other async pools, two runs each, were 34.300 / 28.424 s
at 4 threads and 35.186 / 33.083 s at 16. Checksums matched. The API was
returned to async 8 and profiling disabled.

Artifacts: `results/exp66_duckdb20_evaluation.json`,
`exp66_duckdb20_evaluation_3gb.json`, `exp66_duckdb20_live.json`,
`exp66_duckdb20_profiled_live.json`, `exp66_duckdb20_profiles.json`,
`exp66_duckdb20_async4_live.json`, and `exp66_duckdb20_async16_live.json`.
No commits, pushes, remote storage writes, or `/node-data` access.

##### Review of the measured 2.0 results

Recommendation: retain DuckDB 2.0, async=8 and memory-budgeted read-ahead.
The observed March total drops **47.8%** (58.194 to 30.403 s, **1.91x**),
and first decoded batch drops **53.1%** (46.903 to 22.012 s, **2.13x**).
All five live cases have matching row/checksum/column/dtype signatures against
exp64; March also has exactly the same 430,985,448-byte body. Both local
comparison artifacts contain matching signatures across both actual engines
for minute/day/stress. All three profiled queries are complete, eligible and
identify `v2.0.0`. The native test pass is reported by the VM run; this review
checks its recorded artifacts and does not claim to rerun native tests here.
The validation status JSON and API README now reflect those completed VM runs.
Two 3 GB first-batch table cells above were corrected to match the artifact.

The major observed profile change is the scan: its operator work falls from
roughly 48–52 s to 4.7–5.2 s. That is consistent with the upgrade improving the
remote scan path, but does not isolate asynchronous I/O from engine/extension,
SQL compatibility and cache differences. The 1.5.6 live baseline was one
historical March run; the three 2.0 runs are 43.848/30.286/30.403 s, so this is
an observed comparison, not a guaranteed speedup. Local lower-latency results
in rotated runs provide separate support for keeping the upgrade.

Execution phase boundaries changed: 2.0 returns a native streaming result
before the blocking work completes. Its tiny `execute_pending_seconds` is
not evidence that sorting disappeared. That work now occurs in the first
`FetchChunk`; cumulative fetch time includes this initial wait. In the three
profiles, first batch arrives at 20.34/21.71/25.14 s despite native-ready
markers of only 0.33/0.34/0.49 s. For cross-version comparisons, use elapsed
first-batch and EOF markers rather than comparing only `execute_pending` or
assuming `fetch_chunks_seconds` means the remaining chunks after first data.

Projection is now the largest measured operator-work category (about
19–20 s), followed by the sort (7.6–12.6 s). Native Arrow conversion remains
about 0.7 s. The sort still spills 2.64–2.83 GB, so the upgrade has not removed
all memory pressure or the ordering barrier. The 2.0 reported read/write byte
counters include storage work and are not a direct Pelican traffic measurement;
the apparent increase from the old read counter must not be interpreted as
8x more network transfer. Similarly, blocked-thread time is not an HTTP-only
timer. The 11.6 MB streaming-buffer peak is distinct from process RSS and
buffer-manager memory.

Keep the one-hour single-name regression visible: median 0.450 to 0.755 s,
with new repetitions 0.755/0.781/0.342 s versus old 0.450/0.917/0.427 s.
The overlap does not establish a deterministic slowdown, but two of three new
runs are slower. A fresh alternating comparison and a short-window profile
should precede a diagnosis. In particular, check time-filter/row-group pushdown
with the new timestamp-truncation expression; a function around the storage
column is worth inspecting, but these results do not prove it lost pushdown.
The other short cases are improved or approximately unchanged.

The 4- and 16-thread trials do not justify changing the default: two runs each
are noisy, and neither setting clearly beats 8. Next optimization work should
measure metadata JSON and scalar-value projection costs separately while
retaining the public schema. Bounded chronological execution remains a candidate
for first-batch latency/spill, rather than an assumed next step for total time.
No runtime code, storage data, commits or pushes were changed by this review.

The microsecond truncation that kept DuckDB 2.0 output identical to 1.5.6 is
removed. The API is 2.0 only. Filters widen `time` and the bound to
`TIMESTAMPTZ_NS`, and the response projects that instant as UTC
`TIMESTAMP_NS`. That expression accepts both the published Iceberg
microsecond column and nanosecond Parquet, and it keeps a nanosecond
remainder instead of rounding or truncating it. A live one-minute
`wxt.env.temp` / `W0A0` query on the rebuilt demo API returned 698 rows
with the same full-frame checksum as `exp66_duckdb20_live.json`
(`6494751885119804359` / `12248171161112829197`). Every timestamp in that
response still ends in three zeros, which is the stored Iceberg precision.
Startup remains `cold DuckDB v2.0.0 threads=2 memory_limit=3GB
async_threads=8 read_ahead_depth=-1`.

#### Metadata projection optimization on DuckDB 2.0 (exp67)

Implemented checked fast paths for promoted-tag JSON. Ordinary Arrow and
NDJSON extracts already merged raw and promoted JSON, but also parsed the
promoted object into `json_merge_patch('{}', ...)` just to remove null values
on every row. The new `coldPromotedJSONExpr` checks the promoted fields and
constructs an object containing only the known-present keys for these cases:

- All promoted tags except optional `job` are non-null; include `job` if set.
- Only `plugin` and `vsn` are non-null.

Other presence patterns keep the existing general null-stripping expression.
Raw/promoted key collisions keep the exact MAP-based path. Empty strings are
present values; JSON encoding, key order, null raw tags and precedence remain
unchanged. The checks run on actual rows, not assumed warehouse properties.
Head/tail still use their existing MAP grouping and serialization. No schema,
codec, client, timestamp filtering, ordering, storage, CPU/memory limit or I/O
setting was changed. This is a JSON-construction optimization, not a cache.

A usable precompiled Go test binary linking DuckDB **2.0.0** was available at
`/tmp/sage-duckdb20-eval.Its1t3/duckdb200.test`. The current Go source separately
exports the pre-change and optimized SQL via `TestColdProjectionProbe`; the
native binary accepts that SQL as input and drains it through the real streaming
reader and Zstd writer. Thus these are actual DuckDB 2.0 executions of the new
SQL, not Python DuckDB 1.5 measurements. The existing binary does not compile or
verify newly added Go test code. A full fresh tagged build remains a VM check;
this sandbox cannot download the pinned Go modules, access Docker or reach the
VM API. The pre-change SQL reference exists only in test code and uses the
same current 2.0 timestamp and filter behavior.

The filtered March 1 extract has **1 distinct metadata combination across
1,048,491 rows** (including raw metadata and all promoted fields). This is
measured for that extract, not assumed for other queries. Separate profiled
ablations fully consume the result. Their payloads deliberately differ, so
elapsed-time subtraction would also include changed scans, sorts and IPC.
The relevant day projection operator work is:

| Diagnostic | Projection work |
|---|---:|
| Before optimization | 0.835 s |
| Optimized | 0.622 s |
| Metadata replaced with `{}` | 0.052 s |
| Value replaced with JSON `null`, optimized metadata retained | 0.614 s |

This supports targeting metadata construction rather than value serialization.
The selected change reduces day projection work about **26%** in this diagnostic;
operator work is not additive wall time or a direct prediction of March latency.

Unprofiled medians of three rotated baseline/candidate repetitions, two CPU
workers, async=8, automatic read-ahead, 3 GB engine setting, Zstd to isolated local
IPC files. Native initialization and checksum validation are outside the timer.
All cases match full-row sum/XOR signatures, schema and timestamp ordering:

| Query | Rows | Before total | Optimized total | Before first batch | Optimized first batch |
|---|---:|---:|---:|---:|---:|
| Minute, one name | 698 | 0.194 s | 0.214 s | 0.193 s | 0.212 s |
| Hour, one name | 43,509 | 0.236 s | 0.225 s | 0.229 s | 0.219 s |
| Hour, all names, W0A0 | 395,989 | 1.630 s | 1.597 s | 1.576 s | 1.538 s |
| March 1, one name | 1,048,491 | **1.268 s** | **1.038 s** | **1.140 s** | **0.903 s** |
| Unique metadata workload | 2,000,000 | 2.430 s | 2.439 s | 1.691 s | 1.708 s |

The day is about **18% lower latency**; the unique-metadata workload is
approximately unchanged. Day RSS peaks at 556/547 MB before/after; unique
metadata at 651/664 MB. RSS is sampled for the whole native subprocess.

The first minute comparison is noisy. A separate, sequential nine-repetition
minute rerun gives **0.197/0.202 s** before/after; a separate 512 MB pass gives
0.225/0.203 s. This does not establish a consistent large minute regression,
but there is no claim of a minute speedup. The historical live one-hour
regression still needs a fresh VM run after the user's timestamp cleanup.
The local archive has 15 files with one row group each, spanning the day;
it cannot stand in for the time-sorted published Iceberg layout when assessing
short-window pruning.

A separate 512 MB run uses **4,000,000 rows with unique wide metadata** to force
spilling. Median total is 5.330/5.240 s, first batch 3.733/3.650 s. Sampled RSS
is 616/618 MB. One diagnostic reports 467/490 MB peak spill, with approximately
512 MB buffer usage in both. This does not promise reduced spill or memory;
it checks that the optimization does not depend on a tiny metadata dictionary.
The 512 MB day median is 1.289/1.084 s. These are local runs without HTTP/Iceberg.

Two reuse/serialization candidates were also executed and rejected:

- Directly concatenating serialized object bodies preserves the day output and
  has a good day timing, but the 3 GB unique-metadata diagnostic takes
  3.255 s versus 2.502 s baseline. Extra string handling makes it worse there.
- A correlated scalar subquery encourages metadata deduplication, but adds
  delimiter/hash-join and materialization work. Its unique-metadata diagnostic
  takes 2.913 s; a low-cardinality day benefit does not justify that tradeoff.

Neither approach is used by the API. Their diagnostics are retained with
`--include-rejected`; they are exploratory single profiled runs, not median
production comparisons.

Validation: native DuckDB 2.0 exact Arrow-table equality on **1,024 rows** covering
all **128 promoted-tag presence masks** crossed with eight raw metadata shapes,
including null/empty values, collisions, case sensitivity, braces, Unicode,
quotes/backslashes/newlines, mixed values, integers above 2^53 and nanosecond
instants. Head and tail each return 919 rows and match exactly. All measured
queries also match public schema/full-row signatures and ordering. Selected
Go profile/settings/service/public-schema tests pass with the race detector
using the cached temporary Arrow 18.8.0 modfile; six experiment unit tests pass.
New permanent native regression `TestStreamingColdPromotedJSONShapes` covers
the tag patterns without requiring archive files or extension downloads; its
fresh compilation/execution remains pending on the VM.

Artifacts:
`results/exp67_metadata_projection.json`,
`exp67_metadata_projection_spill.json`, and
`exp67_metadata_projection_short.json`.

Reproduce with a fresh native Go 2.0 test binary (it can serve as both probes):

```sh
(cd beehive-data-api && \
  go test -tags=duckdb_arrow -count=1 ./... && \
  go test -tags=duckdb_arrow -c -o /tmp/sage-projection.test)
python -I sage-data-rework/tiered_system/experiments/exp67_metadata_projection.py \
  --native-probe /tmp/sage-projection.test --sql-probe /tmp/sage-projection.test \
  --reps 3 --include-rejected
python -I sage-data-rework/tiered_system/experiments/exp67_metadata_projection.py \
  --native-probe /tmp/sage-projection.test --sql-probe /tmp/sage-projection.test \
  --memory 512MB --stress-rows 4000000 --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp67_metadata_projection_spill.json
```

The VM first collects a fresh current-2.0 control before rebuilding, so the
one-hour and March comparisons include the timestamp cleanup on both sides:

```sh
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --include-months --month-reps 3 --label 'DuckDB 2.0 before promoted JSON fast paths' \
  --output sage-data-rework/tiered_system/experiments/results/exp67_before_live.json
(cd sage-data-rework/tiered_system/demo && \
  COLD_PROFILE_DIR= COLD_ASYNC_THREADS=8 COLD_READ_AHEAD_DEPTH=-1 \
  docker compose --profile cold up -d --build --no-deps data-api)
python -I sage-data-rework/tiered_system/experiments/exp64_cold_execution_live.py \
  --include-months --month-reps 3 --label 'DuckDB 2.0 promoted JSON fast paths' \
  --baseline sage-data-rework/tiered_system/experiments/results/exp67_before_live.json \
  --output sage-data-rework/tiered_system/experiments/results/exp67_after_live.json
```

#### Measured on the demo API

Both sides are DuckDB 2.0.0 with the nanosecond timestamp projection, two CPU
workers, async=8, automatic read-ahead, and profiling off. The control is the
image from before this JSON change. The candidate is the rebuilt API. Full-row
checksums, columns, dtypes, and Zstd match on every case. Medians of three:

| Query | Rows | Before total | After total | Before first batch | After first batch |
|---|---:|---:|---:|---:|---:|
| Minute, one name | 698 | 0.369 s | 0.315 s | 0.363 s | 0.309 s |
| Hour, one name | 43,509 | 0.627 s | 0.312 s | 0.611 s | 0.299 s |
| Minute, all names | 6,352 | 0.646 s | 0.449 s | 0.640 s | 0.441 s |
| Hour, all names | 395,989 | 0.780 s | 0.720 s | 0.669 s | 0.608 s |
| March, one name | 25,940,207 | 29.597 s | 29.084 s | 22.300 s | 18.798 s |

March individuals are 42.917/29.597/26.982 s before and 29.882/29.084/23.185 s
after. The total-time median moves by about half a second, inside the spread
of those runs. The first-batch median moves from 22.300 s to 18.798 s
(individuals 33.348/22.300/20.320 s before, 18.798/20.411/16.395 s after).
The one-hour single-name before runs are 1.201/0.336/0.627 s and the after
runs are 0.375/0.312/0.311 s, so the median drop includes a slow first control
run. Short windows stay noisy.

The profiled March pass matches the unprofiled candidate checksums. Client
medians are 26.746 s total and 19.055 s to the first batch. Prepare stays
under 0.16 s. The large projection operator does 14.332/13.899/13.684 s of
work, down from about 20 s on the exp66 profile of the previous metadata SQL.
A second projection is about 1.0 s. Scan is 4.064/4.854/3.668 s and the sort
is 11.000/10.386/6.917 s. Each profile scans 66 of 8,487 row groups and
25,940,207 rows, with peak buffer memory about 3.02 GB and a temp directory
of 2.70–2.79 GB. Operator seconds are not additive wall time. Profiling is
disabled again. Artifacts: `results/exp67_before_live.json`,
`exp67_after_live.json`, `exp67_profiled_live.json`, and
`exp67_march_profiles.json`. No storage writes, commits, or pushes.

#### Review of the VM artifacts

Keep the promoted-JSON fast paths. Independently checking all three repetitions
per case confirms matching row counts, full-row hash sums/XORs, decoded columns
and dtypes, schema version and compression mode. March remains 430,985,448
response bytes. All-name response byte counts vary slightly even within a
single image; matching decoded signatures do not imply identical IPC bytes or
ordering between equal timestamps.

The observed March first-batch reduction is 3.502 s (15.7%). Total time falls
only 0.513 s (1.7%), which is insufficient to establish a reliable throughput
gain with this spread and three repetitions collected before/after rebuilding.
The shorter-query medians all improve, but the hour comparison includes a slow
control run; it does not establish a repeatable 2x improvement.

There is also no demonstrated improvement in the time after the first batch.
Subtracting first-batch time from total **within each March run** gives medians
of 7.297 s before and 8.673 s after. Client CPU medians rise from 7.110 s to
8.297 s. These are observations from overlapping fetch/encoding/transfer/client
work, not evidence that this SQL change makes decoding slower. In the separate
profiled pass, native EOF follows the backend's first batch by 5.742–7.029 s,
and native Arrow conversion itself takes only 0.563–0.614 s.

The next experiment should isolate the blocking sort: compare the unchanged
whole-range query with bounded chronological execution (1-, 3- and 7-day
windows), retaining the explicit timestamp sort within each window and one
public Arrow stream. Resolve relative bounds once and pin one Iceberg snapshot
for the entire request; use nonoverlapping half-open bounds with nanosecond
precision. Start with ordinary cold extracts; head/tail and tier-spanning
queries need their existing whole-query semantics preserved. Measure both
first batch and total time, spill, scan work and output equality, because
repeated planning/scanning can offset the smaller sort working set. Alternate
control/candidate runs on the same image and snapshot before selecting a
default. This is a proposed experiment, not an implemented execution change.


#### VARIANT assessment before changing execution (2026-10-22)

Keep exp67 unchanged. VARIANT is a candidate for a future **storage** schema,
especially a single mixed-type `value` column; it is not an established
performance improvement for this workload. DuckDB 2.0 supports typed variants,
Parquet reads/writes and shredding (common fields stored in typed physical
columns). A single logical column therefore need not mean a single physical
payload. References: [DuckDB VARIANT](https://duckdb.org/docs/2.0/sql/data_types/variant),
[Parquet encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).

The current value columns are already typed DOUBLE/BIGINT/VARCHAR; metadata is
MAP(VARCHAR,VARCHAR) with promoted VARCHAR tags, not JSON text in storage.
Converting these to VARIANT inside each SELECT adds conversion work and cannot
retroactively shred existing Parquet files. JSON serialization remains required
at the public response boundary. There is no evidence yet that this removes
our projection cost or reduces the timestamp sort's spill.

Executed a small local semantic probe with the existing native Go test binary
linked to v2.0.0, using `TestColdDuckDBEvaluationProbe` and isolated temporary
files, with no extensions/network/storage writes. The response retained the
four public Arrow fields and Zstd. Verified `to_json(v)::VARCHAR` retains
9007199254740993 exactly, distinguishes string "123" from number 123, and
serializes a boolean as `true`. NULL::VARIANT becomes SQL NULL in this path;
explicit JSON null and SQL NULL need separate fixtures before adoption.

A metadata trap was reproduced:

```sql
-- This serializes as an array of {key,value} entries:
to_json(MAP {'sensor':'x','units':'C','plugin':'p','vsn':'v'}::VARIANT)
-- This remains an object, but changes key order:
to_json(to_json(MAP {'sensor':'x','units':'C','plugin':'p','vsn':'v'})::VARIANT)
```

The original object key order is sensor,units,plugin,vsn; the second expression
returns plugin,sensor,units,vsn. Do not replace the exp67 merge/serializer with a
MAP cast. Raw/promoted collision precedence, null/empty strings and exact
metadata string compatibility still need to be preserved. JSON object order
is semantically immaterial, but changing it would fail the current exact-string
fixtures/signatures.

A durable VARIANT column in Iceberg requires format v3. DuckDB's 2.0 Iceberg
docs advertise v3 VARIANT reads/writes, but that does not establish compatibility
with our publishing stack. The demo pins PyIceberg 0.12.0; the locally installed
0.12.0 has no VariantType and its TableMetadataV3 JSON writer explicitly raises
NotImplementedError. The current seed schema uses three typed value columns and
a string-valued MAP. References: [Iceberg semi-structured types](https://iceberg.apache.org/spec/#semi-structured-types),
[DuckDB Iceberg v3](https://duckdb.org/docs/2.0/core_extensions/iceberg/writing_to_iceberg#iceberg-v3-support).
This is a writer/schema migration, not a DuckDB-only switch. Existing files
would need an explicit backfill or a transitional read adapter; bumping the
format version does not merge their columns.

Arrow defines an `arrow.parquet.variant` extension backed by a struct with
binary metadata/value and optional shredded fields. Sending that directly
would change the HTTP schema, so keep value/meta as JSON-text VARCHAR for both
hot/cold responses. [Arrow canonical variant extension](https://arrow.apache.org/docs/format/CanonicalExtensions.html#parquet-variant).

Recommendation: evaluate a parallel local Parquet dataset with one value
VARIANT first, leaving promoted tags and MAP metadata unchanged. Compare against
the current split columns with identical layout, row groups, codec, ordering
and public JSON serialization. Measure file size, scan/projection/sort work,
spill, first batch and total time; include mixed numbers/strings, large integers,
nonfinite floats, nulls and full output compatibility. Test metadata VARIANT
separately so any improvement has an identifiable cause. Do not change production
storage or response types based on documentation alone. No performance benchmark
or end-to-end Iceberg VARIANT interoperability test was performed in this review.


### Stored single-value VARIANT benchmark (exp68)

**Performance finding:** a single stored VARIANT preserves the tested public
output, but does not improve performance or storage size in this local
comparison. The initial performance-only recommendation was to retain the
split typed columns; the schema decision below supersedes that recommendation.
Exp67 metadata optimization remains unchanged; no API/client/publishing code or
remote storage was modified by this experiment.

`exp68_value_variant.py` exports current Go response SQL using
`TestColdProjectionProbe`. The control uses the current exp67 metadata SQL,
not its historical pre-optimization baseline. Both datasets are newly written
by the native DuckDB **2.0.0** engine from the same read-only source, sorted by
time/name, Zstd level 3, row-group size 122,880, with identical row-group row
counts. The candidate removes value_float/value_int/value_str and stores:

```sql
CASE WHEN value_float IS NOT NULL THEN value_float::VARIANT
     WHEN value_int IS NOT NULL THEN value_int::VARIANT
     ELSE value_str::VARIANT END AS value
```

Each branch casts to VARIANT before the CASE resolves its type, avoiding
integer-to-double/string coercion. Response SQL changes only this value
plumbing and serializes `to_json(stored_value)::VARCHAR`; the HTTP schema stays
`timestamp,name,value,meta`. Metadata, filters and the explicit timestamp sort
remain the same. No variant is sent to clients.

Archive datasets: a complete day for name=wxt.env.temp/vsn=W0A0 (1,048,491
rows), and its first hour across all names for that vsn (395,989 rows). Minute
and one-name hour queries read the rewritten day. A synthetic 2-million-row
stress dataset mixes floats, integers above 2^53, strings and unique wide
metadata. All use actual Parquet VARIANT encoding, not a per-query conversion
of the original columns. DuckDB automatically shreds the scalar variants;
physical schemas and compressed column sizes are retained in the artifacts.

Primary timings: **medians of five**, alternating split/variant order, two
DuckDB CPU workers, async=8, 3 GB memory limit, profiling disabled. Fresh native
engine per run, OS cache warm from local writes; native initialization, dataset
rewriting and checksum validation are outside the query timer. Measured work
includes scan/sort/projection and the native Go Zstd IPC writer to a local file.

| Query | Rows | Split total | VARIANT total | Split first batch | VARIANT first batch |
|---|---:|---:|---:|---:|---:|
| Minute, one name | 698 | 0.0068 s | 0.0069 s | 0.0059 s | 0.0060 s |
| Hour, one name | 43,509 | 0.0428 s | 0.0475 s | 0.0363 s | 0.0402 s |
| Day, one name | 1,048,491 | 0.5235 s | 0.5876 s | 0.3928 s | 0.4598 s |
| Hour, all names | 395,989 | 0.2333 s | 0.2646 s | 0.1773 s | 0.2122 s |
| Mixed stress | 2,000,000 | 1.4949 s | 1.7450 s | 0.5555 s | 0.7156 s |

That is 12.2% more total time for the day, 13.4% for all-name hour and 16.7%
for stress. Minute differences are negligible at this scale. These are local
comparisons, not predictions of a VM March query's elapsed time.

| Dataset | Split Parquet bytes | VARIANT Parquet bytes | Change |
|---|---:|---:|---:|
| Day | 4,830,442 | 4,831,677 | +0.026% |
| All-name hour | 658,638 | 659,429 | +0.120% |
| Mixed stress | 58,454,617 | 60,867,587 | +4.13% |

The day VARIANT shreds to a DOUBLE typed_value plus binary metadata/value
fields. The existing three value columns compress to just 3,369 bytes combined
in this repetitive sensor dataset, leaving little saving to capture. In the
mixed stress dataset, auto-shredding chooses an INT64 typed_value and uses a
binary fallback for the other types: value-related compressed bytes rise from
13,849,133 to 16,262,054 (about 17.4%). A single logical field does not eliminate
the need to represent the different physical types.

Separate profiled diagnostics show day projection work rising from 0.718 to
0.802 s and all-name hour projection from 0.240 to 0.280 s; stress scan work
rises from 0.249 to 0.304 s and projection from 0.971 to 1.062 s. Operator work
is not additive wall time or pure CPU. Neither layout spills at 3 GB. Maximum
sampled native RSS is approximately 480/485 MB for day and 860/890 MB for stress
(split/VARIANT). There is no demonstrated memory reduction.

Validation: exact Arrow-table equality against the original response on a
1,024-row fixture covering all 128 promoted-tag presence masks and eight raw
metadata shapes. Value cases include ±infinity/NaN, signed zero, very small and
large doubles, int64 extrema, >2^53 integers, numeric/boolean-looking strings,
empty/escaped Unicode strings, nulls, and multiple populated value columns
(historical float > integer > string precedence). Both persisted layouts match
exactly. Every timed/profiled query also matches full-row hash sum/XOR, schema,
row count and nondecreasing timestamp ordering. This is semantic/layout
validation, not an end-to-end Iceberg v3 interoperability test.

Artifacts: `results/exp68_value_variant.json` and the separate spill result
below. Reproduce using a fresh VM-native test binary, or the matching native
and SQL-export probes used locally:

```sh
(cd beehive-data-api && go test -tags=duckdb_arrow -c -o /tmp/sage-variant.test)
python -I sage-data-rework/tiered_system/experiments/exp68_value_variant.py \
  --native-probe /tmp/sage-variant.test --sql-probe /tmp/sage-variant.test --reps 5
python -I sage-data-rework/tiered_system/experiments/exp68_value_variant.py \
  --native-probe /tmp/sage-variant.test --sql-probe /tmp/sage-variant.test \
  --memory 512MB --stress-rows 4000000 --match stress --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp68_value_variant_spill.json
```

The harness isolates all rewrites/IPC/spill in temporary directories and
cleans them up. It does not access Pelican/NRDStor or /node-data. The comparison
uses one local time-sorted file per dataset; it does not measure remote file
layout, month-scale queries, ingestion throughput, unshredded VARIANT, or all
possible explicit shredding schemas. The existing sort still carries serialized
public JSON fields, so this candidate does not change that working set.


#### Memory-limited spill check

With **512 MB** memory and **4 million** mixed stress rows, medians of three
alternate runs are 3.536 s split versus 3.852 s VARIANT total (**+8.9%**), and
1.427 versus 1.748 s first batch (**+22.5%**). Exact semantic fixtures and all
row signatures/schema/order checks pass again. Parquet grows from 116,797,760
to 121,878,401 bytes (+4.35%). Sampled process RSS peaks at 724/735 MB; the
DuckDB memory limit is not a hard whole-process RSS cap.

Both layouts spill. Separate diagnostic profiles report 666/653 MB peak temp
usage and 515/516 MB buffer memory (split/VARIANT). That small single-profile
spill difference is not a demonstrated memory benefit. Projection work rises
from 1.844 to 1.991 s and scan work from 0.426 to 0.593 s; sort work is
0.896/0.799 s. These are diagnostic operator-work figures, not elapsed-time
components. Artifact: `results/exp68_value_variant_spill.json`.

The single stored VARIANT is feasible for the tested response semantics, but
our current typed columns are faster and at least as compact under these
settings. These results do not justify migration for performance. The decision
below accepts this tradeoff for schema simplicity instead.

#### Schema decision: proceed toward a single stored value VARIANT

Prefer a single stored `value VARIANT` over separate value_float/value_int/
value_str columns. This aligns the logical cold-storage value model more
closely with hot storage, removes type-specific reconstruction from the API,
and avoids adding a separate storage column for each additional supported
value type. VARIANT preserves underlying types; this is a schema improvement
with an accepted performance cost, not a performance optimization.

The observed 9–17% increase in local total query time is an acceptable tradeoff
for this direction. Its effect on a live March request has not been measured;
the local percentage must not be treated as a prediction for that workload.

This simplifies the API adapter but does not eliminate it. Timestamp
normalization, metadata handling and JSON serialization for the existing public
response remain necessary. Initial scope:

- Store one `value VARIANT`; retain MAP metadata and promoted tag columns.
- Preserve the current public Arrow and NDJSON schemas for hot, cold and
  tier-spanning responses. Serialize the stored value at the response boundary;
  do not expose a new Arrow VARIANT field to callers.
- Keep the exp67 metadata optimization. Changing metadata to VARIANT is a
  separate decision and is not included in this migration.

The next step is an isolated **ingestion → Parquet → Iceberg → API** integration
test before migrating existing data. Iceberg VARIANT requires format v3, and
the pinned PyIceberg 0.12.0 writer remains the outstanding integration issue.
Establish a supported publishing path, verify that the deployed DuckDB Iceberg
reader handles its files, and validate exact value types, timestamps, metadata
and public responses through the complete path. Then measure live query
performance and define how existing split-column files will be backfilled or
read during migration. Bumping the table format alone does not merge those
columns.
>NOTE: see [10_iceberg_v3.md](sage-data-rework/plans/10_iceberg_v3.md) for more details on this version upgrade.

Resolve this integration path before the bounded chronological execution
experiment. Additional local serialization benchmarks alone will not establish
end-to-end compatibility. Keep all integration writes under a new isolated
storage folder and do not touch `/node-data`. This records the intended
direction; production schema and publishing changes are not yet implemented.

DuckDB 2.0 can read and write the v3 table, so the format no longer has to
stay at v2 for the reader. The isolated round trip is in plan 10: a local
REST catalog, `timestamptz_ns` plus one `value VARIANT`, then `iceberg_scan`
of the metadata file with no catalog. Nanosecond remainders, an integer above
2^53, a numeric-looking string, and a boolean all came back intact. PyIceberg
0.12.0 still cannot publish that table, and the live warehouse is unchanged.


### Resume API execution work on the adopted warehouse (exp80, 2026-10-26)

**Historical experiment, now closed:** windowing was rejected and its Go
probe, Python harness and dedicated tests were removed. The implementation
description below records what was measured. Retain the result artifacts as
evidence for this decision.

Plan 10 completed the storage migration and selected the warehouse at
`/cold-storage-demo-v3-bounded-full-1793018629285065000`. It has day partitions,
chronological 10-million-row file batches, plugin/vsn/time sorting and
262,144-row plugin-pure groups. Stored value is VARIANT; time keeps nanoseconds.
The storage investigation is complete. All API comparisons now use this layout.

The next unfinished experiment from the exp67 review is **whole-range versus
1-, 3- and 7-day chronological execution**, keeping an explicit timestamp sort
inside each window and one public Arrow IPC stream. Re-establish its effect
on the adopted layout: the earlier time-first sort profiles are historical,
not a current cost attribution. The month response still waits about 71–73 s
for its first record in the full-warehouse Arrow sweep, so first-batch latency
remains worth testing even after the layout's large throughput improvement.

Implemented an opt-in native experiment in
`beehive-data-api/cold_window_experiment_test.go`, invoked by
`exp80_bounded_execution.py`. This is test-only code: no production execution
or HTTP option/default changes. The probe uses the current Go SQL builder and
native streaming engine. The control and candidate both use one Zstd IPC
writer. Sequential half-open windows preserve global timestamp order and
nanosecond boundaries, and release each result/connection before opening the
next. Empty windows are skipped; later errors and cancellation propagate.
Head/tail, aggregations and window functions are rejected by the experiment;
their production whole-query behavior remains unchanged. End defaults resolve
once, and all window SQL uses one pinned metadata marker, replaced with the
same audited local Parquet inventory for this physical execution comparison.

The harness accepts only existing local audited chronological/262144 files,
checks their combined size, and records the audit and native binary SHA-256.
It never reads `/node-data` or performs remote writes. Queries are series hour,
series day, March series month and April series month. Every output undergoes
full decoded value/metadata signatures, nanosecond ordering, schema and count
validation against the whole-range control outside the timer. It records
first batch, total native elapsed time, window count and sampled subprocess
peak RSS. Modes rotate within each repetition; OS caches are uncontrolled and
there is no excluded warmup. Native initialization is outside the timer;
repeated per-window preparation/execution is inside it.

This first phase isolates local scan/sort/projection and IPC costs. It excludes
Iceberg metadata planning, Pelican/HTTP and Sage-client dataframe decoding.
It does not yet collect per-window spill/operator profiles. A local first-batch
win alone cannot select the production policy: repeated live Iceberg planning
may offset it. Keep whole-range execution as default pending local timings,
separate spill/operator diagnostics and then a same-image live comparison.

The former reproduction commands were removed when the experiment was
closed; its result artifacts and measured scope remain below.

#### Measured locally (2026-10-26)

`TestColdWindowBounds` and `TestColdWindowReader` pass, and the probe is
`/tmp/sage-window-execution.test`. A later-window failure used to leave a nil
reader in the interface and panic on release; the experiment reader now
releases that result and keeps the error. Production queries are unchanged.

Hour and day ranges are one window for every setting, including 1, 3 and 7
days. They check content, not windowing. All 24 responses match: 43,509 rows
for the hour and 1,048,491 for the day. Medians sit around 1.02 s and 2.05 s,
with the four settings inside a few tens of milliseconds of each other.

Month responses also match the whole-range control on every repetition:
25,940,207 March rows and 31,189,045 April rows. Medians of three rotated
repetitions, local Parquet, 3 GB engine limit, Zstd, one IPC stream:

| Query | Windows | First batch | Total | Peak RSS |
|---|---:|---:|---:|---:|
| March, whole range | 1 | 26.692 s | 31.557 s | 3.21 GB |
| March, 1 day | 31 | 1.938 s | 56.276 s | 0.55 GB |
| March, 3 days | 11 | 3.843 s | 36.569 s | 1.30 GB |
| March, 7 days | 5 | 8.357 s | 34.322 s | 3.06 GB |
| April, whole range | 1 | 31.879 s | 37.828 s | 3.23 GB |
| April, 1 day | 30 | 1.887 s | 58.452 s | 0.56 GB |
| April, 3 days | 10 | 3.802 s | 41.524 s | 1.33 GB |
| April, 7 days | 5 | 8.207 s | 38.635 s | 3.35 GB |

One-day windows make the first record about 14–17x sooner and cut sampled
RSS from about 3.2 GB to 0.55 GB. They also raise total time from 31.6 s to
56.3 s in March and from 37.8 s to 58.5 s in April. Seven-day windows avoid
most of the one-day total-time penalty (2–9% slower than whole-range), move
the first record to about 8 s, and provide little memory benefit.
Whole-range execution stays the production default: this pass excludes
Iceberg planning and Pelican, and the faster first record costs a slower
complete response. Artifacts: `results/exp80_bounded_short.json` and
`results/exp80_bounded_months.json`. No warehouse, API service, public
schema, commit or push changed.


#### Decision: retain whole-range execution; drop windowing (2026-10-26)

Keep one whole-range DuckDB statement with the existing explicit timestamp
sort for each cold query. Drop the 1-, 3- and 7-day execution alternatives
completely: no windowing setting, HTTP option, adaptive policy or follow-up
window-size benchmark is planned. Exp80 remains historical evidence only;
its native probe, Python harness and dedicated tests have been removed.

The measured first-batch and memory improvements do not justify the slower
complete response and additional execution complexity. Three-day windows
are also rejected, rather than retained as an optional mode. This decision
does not change the adopted chronological Parquet file batches or plugin-pure
row groups; physical layout and query execution are separate decisions.

Continue API optimization on the adopted full warehouse using whole-range
queries. The next diagnostic is live March/April profiling to distinguish
remote reads, preparation, projection, sort/spill and response delivery.
Public Arrow/NDJSON schemas and hot/cold/head/tail semantics stay unchanged.


### Warehouse → API → Sage client compatibility (exp81, 2026-10-26)

The adopted full warehouse remains Iceberg v3 with VARIANT value field ID 11,
nanosecond timestamps, chronological file batches and bounded plugin-pure row
groups. Whole-range execution remains the API path. The saved plan-10 sweep
already validates the deployed API's public Arrow output across both months;
this pass checks the current workspace client and closes the mixed-type and
request-boundary coverage gaps. It does not change storage or the wire schema.

**Found and fixed a client request bug:** `timestr()` used `%f`, which silently
truncated request bounds to microseconds even though response timestamps retain
nanoseconds. A one-nanosecond interval could therefore become empty or select
the wrong rows. Request formatting now retains the last three timestamp digits
and converts timezone-aware bounds to UTC before adding `Z`. Existing naive
bounds still mean UTC, and microsecond inputs retain their previous format.
Regression tests exercise the actual public `query()` request payload, including
`2025-03-01T00:00:00.026403194Z` → `…026403195Z` and a `-06:00` input.

Verification available here: 22 offline client decoder/request tests pass,
including mixed numeric/string/bool/null values, integers above 2^53, metadata,
Zstd, nanosecond responses and the new request checks. Two exp81 harness tests
also pass (mixed values and empty responses). The initial complete client suite
had 25 passes and five environment failures: three HTTP-server tests cannot
bind sockets here, and two existing external-API tests cannot resolve DNS.
The new native-fixture test is explicitly skipped until its export exists.
The API Go suite cannot build here because the pinned Go modules are not in
the accessible cache; Docker is inaccessible and localhost:10000 is unreachable.
No current-source native or live compatibility success is claimed yet.

Two complementary checks are prepared for the VM:

1. `TestIcebergV3RoundTrip` still uses actual DuckDB v3 reads, production SQL,
   native Arrow streaming, NDJSON conversion, tiered merging and HTTP writers.
   With `COLD_V3_HTTP_FIXTURE_DIR` set to a fresh isolated directory it exports
   the ten validated HTTP bodies plus expected values/timestamps/metadata:
   cold, head, tail, exclusive-nanosecond bounds and hot/cold tier-spanning,
   each as Arrow and NDJSON. The Sage client replays those exact bodies through
   its public `query()` entry point and verifies UTC nanoseconds, >2^53 ints,
   numeric-looking strings, booleans and flattened metadata. Only transport is
   replayed; client decoders are real. This fixture uses a synthetic native v3
   table for edge types, not the selected warehouse's physical plugin layout.
2. `exp81_v3_client_compatibility.py` fetches real Arrow/Zstd and NDJSON bodies
   from the adopted warehouse API, validates their public wire signatures,
   and replays the same bytes through the workspace client's public query and
   format dispatch. It also checks the serialized client bounds against the
   exact original instants. It compares decoded dataframe rows/columns/value
   dtypes/content across formats without requiring identical string/timestamp
   backing dtypes. Queries cover March 1 minute/hour, March 31/April 1/April 30
   minutes, head/tail, a one-nanosecond exclusive bound, empty results and an
   optional full series day. The known March 1 minute must contain 698 rows,
   beginning at `1740787200026403194` / `11.7`. No month dataframe is required.

Run from the workspace root. First verify the selected API's configured
COLD_WAREHOUSE is the adopted bounded-full URL; an audit file or successful
response alone cannot prove endpoint configuration. Use its actual endpoint
in the command (10006 was the isolated full-layout sidecar in plan 10):

```sh
python -I sage-data-rework/tiered_system/experiments/exp81_v3_client_compatibility.py \
  --endpoint http://127.0.0.1:10006/api/v1/query --include-day \
  --output sage-data-rework/tiered_system/experiments/results/exp81_v3_client_live.json
```

Use a fresh output filename; the harness never replaces an existing report.
The report records the workspace client source/hash and every wire/dataframe
signature. This checks compatibility, not performance; client decoding is
performed after each HTTP body is spooled, so its timings must not be compared
to the earlier end-to-end Sage-client benchmarks.

For native mixed-type/tiered fixtures, use a fresh isolated REST fixture root,
not the adopted warehouse's writable catalog. Both fixture root and export
must be outside `/node-data`. With the fixture already running and its root
mounted at the same absolute path:

```sh
export COLD_V3_ENDPOINT=http://127.0.0.1:18186
export COLD_V3_ROOT="$PWD/sage-data-rework/scratch/v3-client-native-fixture"
# COLD_V3_ROOT must be the actual fresh REST fixture mount; start it using
# docker-compose.v3.yml with COLD_V3_PORT=18186 before running the test.
export COLD_V3_HTTP_FIXTURE_DIR="$(mktemp -d "$PWD/sage-data-rework/scratch/v3-client-http-XXXXXX")"
(cd beehive-data-api && go test -tags=duckdb_arrow -run '^TestIcebergV3RoundTrip$' -count=1 -v)
(cd sage-data-client && PYTHONPATH=src:tests python -m unittest test_v3_http_fixtures -v)
(cd beehive-data-api && go test -tags=duckdb_arrow ./...)
(cd sage-data-client && PYTHONPATH=src python -m unittest discover -s tests -v)
```

#### Measured (2026-10-26)

The API on port 10006 is the adopted bounded-full warehouse, watermark
`2025-05-01T00:00:00Z`, Zstd, profiling off. All 20 live Arrow and NDJSON
bodies passed the public wire check and the workspace client's `query()`
replay. Arrow and NDJSON agree on rows, columns, value dtype and content.
The March 1 minute is 698 rows beginning at `1740787200026403194` / `11.7`.
The one-nanosecond exclusive bound returns that single row. The absent-name
query is empty. Head and tail return 2 rows. The March 1 series day is
1,048,491 rows in both formats. Other minutes are 43,509 (hour), 725
(March 31), 721 (April 1) and 729 (April 30). Artifact:
`results/exp81_v3_client_live.json`.

`TestIcebergV3RoundTrip` passed in 0.63 s against a fresh REST fixture and
exported the ten HTTP bodies. The workspace client decoded all ten. The full
client suite then passed 34 tests, including that fixture replay. The full
API suite passes with the opt-in catalog variables unset. A second export
into the same fixture directory stops because those body files already
exist. No remote objects, running API configuration, commits or pushes
changed. The temporary catalog container was removed after the export.


### Whole-range live API profiling on the adopted warehouse (exp82, 2026-10-26)

The compatibility pass is complete. Keep the adopted warehouse, single
whole-range DuckDB statement, public schema, and client precision fix. The
next measurement attributes the current March/April request costs; it does
not reintroduce windowing or change the Parquet layout.

`exp82_whole_range_profiles.py` is prepared for two phases on a dedicated
sidecar: first profiling off for primary HTTP timings, then profiling on for
request-paired diagnostics. It reads Docker configuration and cgroup-v2 memory
counters; it never starts/stops/reconfigures containers, writes storage, or
changes the existing API. Sidecar setup below is a separate explicit step.
The script requires the adopted warehouse URL, watermark May 1, async=8,
read-ahead=-1 and Zstd. The profiled phase must use the same image and CPU/
memory limits as its completed unprofiled control. The profile directory must
be mounted from the actual dedicated sidecar and receive no other requests.

Default queries are March and April series months (`wxt.env.temp`, W0A0).
Optional cases are March 1 series hour/day. Each phase excludes one warmup
per query, then measures three repetitions with query order rotated. Full
wire signatures for the first measured response per case must match the
completed full-layout sweep; counts and public Arrow schema are checked on
every response. HTTP output spools to one temporary file at a time. This is
HTTP receive + IPC iteration/file-spool timing; it excludes Sage-client
DataFrame construction. Query-validation work happens outside the receive
clock. OS and Pelican caches remain uncontrolled.

Every profiled HTTP request must produce exactly one fresh complete native
DuckDB/timing pair with the same row count and DuckDB 2.0 version. Historical
profiles are ignored. The report retains every repetition plus separate
medians for response timing, backend elapsed phases, native operator work,
planning/optimizer metrics, row groups, engine buffer memory and temp spill.
Do not add overlapping backend markers or parallel operator times to elapsed
HTTP time. Engine byte counters are not complete Pelican network counters;
zero bytes does not prove zero remote reads. The earlier 32–38 s local versus
77–82 s HTTP runs were not controlled; their difference is not an attributed
network or API overhead measurement.

Container memory is now explicitly captured via cgroup-v2 `memory.peak` and
`memory.events`, plus Docker restart/OOM state before/after the phase. A
fresh container per phase makes the peak useful, but it includes initialization,
warmups, all queries and charged file cache; it is **not per-query RSS**.
Per-query native buffer/temp peaks remain separately available in the profiles.
The run fails on a new oom/oom_kill event, restart, container replacement,
configuration change, missing profile, or content mismatch.

#### VM procedure

Use the same image as the validated adopted-warehouse API on port 10006.
Set `VALIDATED_API_CONTAINER` to that actual container's name/ID (`docker ps`
can show it). Read only the image ID; do not recreate the validated API.
Check available VM RAM first, since existing idle DuckDB instances can retain
buffers. Start only one exp82 sidecar at a time. Do not run seeds, audits,
other benchmarks or parallel requests during this measurement.

```sh
# Set this to the existing validated container name/ID, not a new API image tag.
: "${VALIDATED_API_CONTAINER:?set the validated adopted-warehouse API container name or ID}"
export EXP82_API_IMAGE="$(docker inspect --format '{{.Image}}' "$VALIDATED_API_CONTAINER")"
export EXP82_ID="$(date +%s)"
export EXP82_ROOT="$PWD/sage-data-rework/scratch/exp82-${EXP82_ID}"
export EXP82_RESULTS="$PWD/sage-data-rework/tiered_system/experiments/results/exp82-${EXP82_ID}"
export EXP82_WAREHOUSE=https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse
mkdir -p "$EXP82_ROOT/unprofiled" "$EXP82_RESULTS"
LAYOUT_API_IMAGE="$EXP82_API_IMAGE" LAYOUT_API_PORT=10007 \
  LAYOUT_COLD_WAREHOUSE="$EXP82_WAREHOUSE" \
  LAYOUT_PROFILE_HOST="$EXP82_ROOT/unprofiled" LAYOUT_PROFILE_DIR="" \
  docker compose -p sage-exp82 -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps candidate-api
export EXP82_CONTAINER="$(docker compose -p sage-exp82 -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml ps -q candidate-api)"
python -I sage-data-rework/tiered_system/experiments/exp82_whole_range_profiles.py \
  --endpoint http://127.0.0.1:10007/api/v1/query --container "$EXP82_CONTAINER" \
  --reps 3 --output "$EXP82_RESULTS/unprofiled.json"
```

Wait for sidecar startup before the request run (connection refusal is a
setup failure, not a measured query). This phase must complete before the
profiled phase. Recreate **only the exp82 sidecar**, enabling diagnostics and
using a new mount; the existing port-10006 API is unchanged:

```sh
mkdir -p "$EXP82_ROOT/profiled"
LAYOUT_API_IMAGE="$EXP82_API_IMAGE" LAYOUT_API_PORT=10007 \
  LAYOUT_COLD_WAREHOUSE="$EXP82_WAREHOUSE" \
  LAYOUT_PROFILE_HOST="$EXP82_ROOT/profiled" LAYOUT_PROFILE_DIR=/profiles \
  docker compose -p sage-exp82 -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml up -d --no-deps --force-recreate candidate-api
export EXP82_CONTAINER="$(docker compose -p sage-exp82 -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml ps -q candidate-api)"
python -I sage-data-rework/tiered_system/experiments/exp82_whole_range_profiles.py \
  --endpoint http://127.0.0.1:10007/api/v1/query --container "$EXP82_CONTAINER" \
  --profiles "$EXP82_ROOT/profiled" --control "$EXP82_RESULTS/unprofiled.json" \
  --reps 3 --output "$EXP82_RESULTS/profiled.json"
```

Preserve both reports and the raw profile mount. Use distinct result filenames
for retries. `--case 2025-03-01_series_day` can stage a shorter run, but pass
the identical case selection to both phases. After collecting diagnostics,
stop only this dedicated sidecar to release its engine memory:

```sh
docker stop "$EXP82_CONTAINER"
```

The resulting analysis should lead with primary unprofiled first-batch/total
times, then explain preparation/hint lookup, scan/projection/sort, spill and
memory using the paired profiles. Native EOF/backend lifetime/HTTP completion
markers help inspect response delivery, but the profiler does not separately
measure network-only time or Zstd-only CPU. Profile-on/off differences across
sequential phases are descriptive; they do not isolate profiler overhead.

#### Measured (2026-10-26)

A dedicated sidecar on port 10007 used the validated API image
`sha256:c1c465a46a16`, the adopted warehouse, watermark May 1, async=8,
read-ahead=-1 and Zstd. The API on port 10006 was not recreated. Each phase
had its own container. Neither phase restarted or recorded an OOM event.
Full-body signatures matched the completed full-layout sweep, and every
response kept the public Arrow schema and row count: 25,940,207 in March and
31,189,045 in April. Warmups are excluded from the medians. The sidecar was
stopped afterward.

Unprofiled medians of three rotated repetitions:

| Query | First record | Full response |
|---|---:|---:|
| March series month | 63.556 s | 70.941 s |
| April series month | 66.287 s | 75.227 s |

Individuals are March 62.217/65.582/63.556 s to the first record and
69.115/72.983/70.941 s total; April 66.287/64.627/67.056 s and
75.492/73.042/75.227 s. Container cgroup peak for the whole unprofiled phase,
including warmup and file cache, was 6.54 GiB.

The profiled phase used the same image and limits. Its HTTP medians were
64.268/71.486 s for March and 63.363/72.041 s for April. That is close to the
unprofiled phase and is not a separate overhead measurement. Hint lookup is
0.03–0.14 s. Prepare plus pending is about 0.37 s, and the native result is
ready at 0.53 s in March and 0.43 s in April. The first record still waits
64.2 s and 63.3 s because execution happens while the first batch is fetched.

Operator work, medians of the three measured profiles, is not additive wall
time. March: scan 39.887 s, large projection 28.054 s, sort 9.555 s, second
projection about 1.13 s. April: large projection 34.100 s, scan 26.483 s,
sort 11.955 s, second projection about 1.37 s. Both scans return exactly the
response row count, from 1,357 of 32,924 row groups in March and 1,491 of
30,628 in April. Engine peak buffer memory is 3.02 GB. Temp spill is 2.38 GB
in March and 2.99 GB in April. Blocked-thread time is 25.1 s and 28.1 s.
Engine read counters are 3.68 GB and 4.27 GB and are not a Pelican traffic
measurement. The profiled container's cgroup peak was 6.43 GiB.

The month wait is the ordered scan and projection, not request setup. Sort
and spill remain visible but smaller than those two operators. Artifacts:
`results/exp82-1793050193/unprofiled.json` and `profiled.json`, with the raw
profiles under `scratch/exp82-1793050193/profiled`. No storage change, commit,
or push.


### Value versus metadata projection diagnostics (exp83, 2026-10-26)

Keep the adopted warehouse architecture and whole-range API execution.
`exp83_projection_ablation.py` reads the 879 audited local files without
rewriting them. Current production SQL is exported by the Go projection probe;
only test/diagnostic code was extended. HTTP routes, response schemas, client
behavior and the production projection are unchanged.

There are two complementary four-mode comparisons:

- **Response:** full public output, metadata replaced by `'{}'`, value replaced
  by `'null'`, and both replaced. Each still sorts by timestamp and uses the
  native Go Arrow/Zstd writer. This changes column pruning, sort payload and
  output as well as serialization, so time differences are not pure JSON CPU
  savings or proposals to remove response fields.
- **Checksum:** hash every raw field (timestamp, name, VARIANT value, MAP meta
  and all seven promoted tags) in every mode. Aggregate source hash sum/XOR,
  count and serialized-field hash sums into one tiny public-shaped row. Omit
  metadata/value JSON formation independently as above. Raw-input columns
  stay required; there is no response sort or large IPC body. This measures
  JSON formation plus checksum hashing, not serializer CPU in isolation. Its
  single row's first-batch time is aggregate completion, not streaming latency.

Every response run checks schema, sorted nanoseconds and full retained-field
hash sum/XOR against the full control with matching literal substitutions.
Checksum runs require identical raw source counts/hash sum/XOR and identical
checksums for any retained serialized field. Every measured/profiled repetition
is checked. This permits deliberate diagnostic changes without letting an
accidentally unmodified or otherwise changed query masquerade as an ablation.

Primary timings are three rotated unprofiled repetitions, fresh native engine
per run, two CPU workers, async=8, automatic read-ahead, a 3 GB engine limit and
Zstd. Native initialization and Python checksum validation are outside the
query timer. OS caches are uncontrolled. Separate profiles retain operator
work, buffer memory, temp spill, elapsed backend markers and raw profile/SQL
files alongside the report. Process RSS is sampled for the native subprocess;
it is distinct from container cgroup peaks and the engine buffer limit.

This is a local physical execution diagnostic, excluding Iceberg metadata
planning, Pelican/HTTP and Sage-client dataframe construction. Interpret the
results together with exp82; do not compare absolute times as a controlled
local-versus-remote experiment.

Locally, the native binary is the recorded DuckDB 2.0 Go evaluation probe.
The SQL exporter was freshly built from exact copies of current `cold_sql.go`,
`types.go`, `cold_projection_probe_test.go` and the production filter validator,
using only the standard library so absent native module downloads do not block
SQL export. `cold_sql_test.go` and the new projection-diagnostic test pass in
that isolated build. Its source SHA-256 manifest is retained with the results.
This does not substitute for rebuilding the complete API; no production API
source changed. The Python retained-field and checksum guards also pass.

To reproduce on the VM with its normal native module cache, build one current
API test binary for both export and execution. Use fresh output filenames:

```sh
export PROJECTION_PROBE=/tmp/sage-projection-ablation.test
(cd beehive-data-api && go test -tags=duckdb_arrow -run '^Test(BuildColdSQL|ColdProjectionDiagnostics)$' -count=1)
(cd beehive-data-api && go test -tags=duckdb_arrow -c -o "$PROJECTION_PROBE")
python -I sage-data-rework/tiered_system/experiments/exp83_projection_ablation.py \
  --native-probe "$PROJECTION_PROBE" --sql-probe "$PROJECTION_PROBE" \
  --candidate-audit sage-data-rework/tiered_system/experiments/results/full-layout-1792962354/candidate-audit.json \
  --case day --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp83_projection_day_vm.json
python -I sage-data-rework/tiered_system/experiments/exp83_projection_ablation.py \
  --native-probe "$PROJECTION_PROBE" --sql-probe "$PROJECTION_PROBE" \
  --candidate-audit sage-data-rework/tiered_system/experiments/results/full-layout-1792962354/candidate-audit.json \
  --case march --case april --reps 3 \
  --output sage-data-rework/tiered_system/experiments/results/exp83_projection_months_vm.json
```

Both families run by default; `--family response` or `--family checksum`
selects one. Do not run writers, audits or other benchmarks concurrently.
Reports and paired native profiles are retained locally; no Pelican/NRDStor
objects, `/node-data`, running API configuration, commits or pushes are changed.


#### Completed local projection diagnostics (exp83, 2026-10-26)
All **72 unprofiled runs and 24 separate native profiles** completed for the
day, March and April series queries. Every retained-field/raw-source check
passed. The actual engine reports DuckDB 2.0.0, two workers, async=8, automatic
read-ahead and the configured 3 GB limit. No production SQL or API behavior
was changed.
**Metadata is the next projection optimization target for these queries.**
Value serialization is a materially smaller cost.

##### Full-response ablations
Median unprofiled elapsed seconds (three rotated repetitions):
| Query | Full | Metadata omitted | Value omitted | Both omitted |
|---|---:|---:|---:|---:|
| Series day | 1.703 s | 0.632 s | 1.511 s | 0.456 s |
| March series month | 28.871 s | 6.453 s | 27.350 s | 3.868 s |
| April series month | 35.463 s | 7.788 s | 32.541 s | 4.493 s |

Omitting metadata lowers month elapsed time by **77.6% / 78.0%**; omitting
value lowers it by only **5.3% / 8.2%** (March/April). These deliberately
changed payloads are diagnostic upper bounds, not an achievable speedup claim
for correct public output. They include reading fewer columns and avoiding
sort spill as well as removing JSON work.

##### Fixed-input checksum ablations
Median unprofiled elapsed seconds (three rotated repetitions):
| Query | Full | Metadata omitted | Value omitted | Both omitted |
|---|---:|---:|---:|---:|
| Series day | 1.645 s | 1.091 s | 1.590 s | 1.012 s |
| March series month | 25.229 s | 10.992 s | 23.579 s | 9.261 s |
| April series month | 30.004 s | 12.850 s | 28.171 s | 10.756 s |

Keeping every raw input field required and removing the large sort/output
still shows **56.4% / 57.2%** lower elapsed time without metadata JSON, versus
**6.5% / 6.1%** without value JSON. This isolates JSON formation plus checksum
hashing; it does not isolate serializer CPU alone.

The checksum profiles support that interpretation: March scan work stays
**11.98–12.15 s** across the four modes, while projection work falls from
**37.49 to 9.04 s** when metadata is omitted. April scan work stays
**13.95–14.31 s**, while projection falls
from **45.04 to 11.43 s**. None of the checksum modes spills. Operator work
is parallel work, not additive elapsed time.

The full-response profiles show an additional memory/sort effect. Full
March/April queries spill **2.57 / 3.30 GB** to temp; omitting metadata
eliminates spill in both. Omitting value does not eliminate spill. This does
not imply eliminating metadata from the public response: the objective is
to generate and carry it more efficiently.

The compressed body barely changes when metadata is omitted: approximately
**0.60% smaller** for the day and both months. Thus the large local timing
difference is not explained by fewer transmitted compressed metadata bytes.
Metadata already compresses well; repeated JSON processing and intermediate
sort payloads remain costly.

##### Exact raw metadata cardinality
After the timing/profile sweep finished (no concurrent native benchmark), a
separate read-only aggregate counted distinct tuples of raw MAP metadata plus
all seven promoted tag columns:
| Range | Rows | Distinct raw metadata/tag tuples |
|---|---:|---:|
| day | 1,048,491 | 1 |
| march | 25,940,207 | 1 |
| april | 31,189,045 | 1 |

This is **one metadata combination for millions of rows** in each tested
`wxt.env.temp` / W0A0 range. It explains why computing metadata once per
distinct tag combination is a strong next candidate. It is not evidence
that arbitrary fleet queries have one metadata shape. The exact-count
prepasses themselves took about **0.92 / 8.44 / 9.90 s** (day/March/April);
a production design must account for extra scans/planning rather than assume
metadata discovery is free. Raw-tuple cardinality is not a proof that
regrouping preserves exact serialized key order or collision precedence.

**Next candidate, not implemented here:** compute metadata JSON once per
distinct raw-metadata/promoted-tag combination and reuse it while preserving
the existing `meta` JSON-string column and all hot/cold/head/tail semantics.
Retain the general path when reuse does not help. Validate raw/promoted
collisions, null/empty tags, key order and complete output signatures, and
measure total time including discovery/join work before choosing a policy.
Keep stored VARIANT, Parquet architecture, whole-range execution and public
response schemas unchanged. These local ablations do not predict the live
API speedup; any correct candidate still requires paired live measurement.

Artifacts: `results/exp83_projection_day.json`,
`results/exp83_projection_months.json`, and
`results/exp83_metadata_cardinality.json`. Each report records binary hashes;
source SHA manifests and paired raw native timing/profile/SQL files are under
the corresponding result stem directory. `--family cardinality` repeats the
exact-count diagnostic independently using the same probes and audit.
Two Python validation tests and the two isolated current-Go SQL/diagnostic
tests passed. No warehouse files, remote objects, running APIs, public schemas,
commits or pushes changed.


### Metadata JSON reuse candidate (exp84, 2026-10-26)

**Keep stored metadata as MAP.** No Parquet/Iceberg schema or physical-layout
change is part of this candidate. Public Arrow/NDJSON fields remain
`timestamp,name,value,meta`. There is no execution windowing and no client
request option. The objective is to avoid serializing the same metadata object
millions of times while retaining its exact current bytes and semantics.

#### Approaches evaluated

The explicit materialized dictionary serialized metadata after exact distinct
key discovery and joined it back using null-safe equality. It preserved output
but scanned the source twice: day elapsed increased from 1.710 to 2.108 s.
The correlated ordered-entry/list form also preserved output but grouped too
many outer rows in the observed plan; day elapsed rose to 2.503 s. Neither is
selected for the API.

A primitive raw-JSON correlation key did deduplicate metadata and improved day
elapsed from 1.707 to 1.338 s, but its extra materialization cost erased the
month benefit: March 28.930 → 29.021 s; April 34.505 → 35.740 s. Temp spill
rose from 2.58/3.43 GB to 8.03/10.08 GB. This general correlated lookup is
also rejected for the API. The serialized key was temporary SQL state;
stored metadata remained MAP throughout.

**Selected evaluation candidate: verify uniform metadata, then use its JSON
as a constant in the existing main query.** Discovery returns at most two
distinct tuples of ordered `map_entries(meta)` plus all seven promoted tags.
Only exactly one tuple permits reuse. Zero or two tuples keep the original
SQL. Ordered entries prevent maps with identical pairs in different insertion
orders from being incorrectly merged. NULL/empty maps and NULL/empty tags
remain distinct keys; fallback is conservative even when two tuples happen
to render the same JSON. LIMIT bounds discovery output, not necessarily the
scan/group work, so all discovery time must be included.

The discovery output uses the existing metadata merge/serializer, including
promoted-key collision precedence. Its original JSON bytes are quoted as a
VARCHAR literal without re-marshalling or reordering keys. The main query
still scans/sorts the whole requested range and emits one normal response.
Discovery and response use the same immutable metadata-file URL and absolute
half-open nanosecond bounds. No metadata is cached across requests/snapshots.

#### Completed local measurements

Three rotated unprofiled repetitions per mode, with separate profiles, on the
same adopted 879-file warehouse. DuckDB 2.0.0, two workers, async=8, automatic
read-ahead, 3 GB budget and Zstd. Full response schema/order/count/content
signatures match in every measured/profiled run. Semantic fixtures cover all
128 promoted-tag presence masks, raw/promoted collisions, NULL/empty fields,
large integers and mixed VARIANT values, plus MAP insertion-order and quoted/
escaped/Unicode examples. Ten individual uniform cases explicitly exercise
the literal path, not only heterogeneous fallback. A two-row different-MAP-
order case correctly falls back and retains different metadata JSON strings.
Head/tail use byte-identical original SQL.

| Range | Baseline first | Reuse first, incl. discovery | Baseline total | Reuse total, incl. discovery |
|---|---:|---:|---:|---:|
| March series month | 23.998 s | 13.924 s | 27.881 s | 17.843 s |
| April series month | 29.046 s | 16.630 s | 34.244 s | 21.895 s |

That is about **36% lower total native execution time** in both months.
Discovery alone is 8.476/9.998 s; main query 9.372/11.863 s (March/April).
The combined column is the median of each run's sum; component medians need
not add exactly. The prototype runs discovery and response in separate fresh
native probe processes, excluding both engine startup and validation from
these timers. These are local SQL-component results, not shared-engine API
wall times or a predicted Pelican speedup. The API implementation below uses
its existing request engine/connection and must be measured live.

This is **not a demonstrated memory/spill optimization**. Main query temp
spill is about 2.72/3.43 GB versus baseline 2.63/3.48 GB, and sampled native
RSS remains about 3.2 GB. Metadata strings are still carried by the final
ordered result. No claim is made about solving the NDJSON month OOM.

Short-query overhead is real: forcing the uniform prototype on a minute
increases total from 0.679 to 0.976 s; all-names/vsn hour with multiple
metadata tuples falls back correctly but rises from 0.938 to 1.468 s. Day
improves only 1.697 → 1.574 s. Consequently the API evaluation gate is
conservative: **exact name + vsn, at least 28 days, ordinary extract only**.
It deliberately excludes shorter/unmeasured shapes, wildcard series and
head/tail. This is an evaluation boundary, not an established optimal cutoff;
even a long query can pay discovery cost and then fall back.

Artifacts: `results/exp84_uniform_metadata_short.json`,
`results/exp84_uniform_metadata_months.json`. Rejected variants are retained
in `exp84_metadata_reuse_short.json`, `exp84_metadata_reuse_json_short.json`
and `exp84_metadata_reuse_months.json`. Paired raw profiles and SQL are under
the corresponding result-stem directories. Binary hashes identify the native
and SQL probes. The final production plan's exported discovery/main SQL was
checked byte-for-byte against the measured uniform prototype.

#### API implementation, operator evaluation only

`COLD_UNIFORM_METADATA=true` / `-cold.uniform-metadata=true` enables the gated
candidate. **Default is false.** No existing running API was reconfigured.
The configuration is forwarded by the main demo and layout-trial compose
files; the latter uses `LAYOUT_UNIFORM_METADATA=true|false`.

The Arrow path discovers metadata on its existing native database under the
backend lock, with the same scoped token and pinned source. It fully exhausts
and releases the discovery reader/connection before opening the main reader.
Discovered strings are cloned before native buffers are released. NDJSON
uses its existing database/sql request connection, closing discovery rows
before the main query. Cancellation or fetch/scan errors propagate; a partially
read singleton never authorizes a constant. Hot/tier-spanning composition,
head/tail and public response conversion remain unchanged.

Arrow profiles add `metadata_discovery_seconds`, `metadata_discovery_rows` and
`metadata_reuse_applied` (last two are counts/flags). Discovery native profiling
is disabled explicitly and excluded from the main native profile; its elapsed
cost remains in the parent phases and full HTTP clock. The main pair counts
only response rows. Raw main SQL profiles may contain the reused metadata JSON
literal. Native tests exercise same-engine reader lifetime, profile isolation,
cancellation, partial failure, nanoseconds, collision precedence and the real
NDJSON scanner's large-integer handling.

Verified here: six isolated current-production Go SQL/gate/selection/SQL-row
resource tests and six Python runtime/selection guard tests pass. Native
DuckDB SQL/semantic measurements above pass. The complete API/native Go suite
cannot build here because the pinned native modules are absent from the
accessible cache. The new shared-engine native lifecycle tests and live API
candidate therefore remain **pending VM execution**. Local SQL equality does
not substitute for that integration test. Nothing is enabled by default or
promoted on the strength of the prototype alone.

#### VM verification and same-image live comparison

First run the complete API suite on the VM with the native module cache. The
new native lifecycle test needs no REST/Pelican fixture and must pass, not
skip. Unset stale opt-in REST/export variables so this run cannot accidentally
reuse an old fixture catalog/root:

```sh
(cd beehive-data-api && env -u COLD_V3_ENDPOINT -u COLD_V3_ROOT -u COLD_V3_HTTP_FIXTURE_DIR \
  go test -tags=duckdb_arrow ./...)
export META_ID="$(date +%s)"
export META_API_IMAGE="beehive-data-api:uniform-meta-eval-${META_ID}"
docker build -t "$META_API_IMAGE" beehive-data-api
export META_ROOT="$PWD/sage-data-rework/scratch/meta-eval-${META_ID}"
export META_RESULTS="$PWD/sage-data-rework/tiered_system/experiments/results/meta-eval-${META_ID}"
export META_WAREHOUSE=https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse
mkdir -p "$META_ROOT" "$META_RESULTS"
start_meta_api() {
  mkdir -p "$3"
  LAYOUT_API_IMAGE="$META_API_IMAGE" LAYOUT_API_PORT=10008 \
    LAYOUT_COLD_WAREHOUSE="$META_WAREHOUSE" LAYOUT_UNIFORM_METADATA="$1" \
    LAYOUT_PROFILE_DIR="$2" LAYOUT_PROFILE_HOST="$3" \
    docker compose -p sage-meta-eval -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml \
      up -d --no-deps --force-recreate candidate-api
  export META_CONTAINER="$(docker compose -p sage-meta-eval -f sage-data-rework/tiered_system/demo/docker-compose.layout-trial.yml ps -q candidate-api)"
}
```

Run only one evaluation sidecar at a time, with sufficient VM memory and no
other benchmarks. These commands affect only `sage-meta-eval`, not the validated
API on port 10006. Wait for startup readiness after each recreate.

```sh
start_meta_api false "" "$META_ROOT/control"
python -I sage-data-rework/tiered_system/experiments/exp82_whole_range_profiles.py \
  --endpoint http://127.0.0.1:10008/api/v1/query --container "$META_CONTAINER" \
  --reps 3 --output "$META_RESULTS/control.json"
start_meta_api true "" "$META_ROOT/candidate"
python -I sage-data-rework/tiered_system/experiments/exp82_whole_range_profiles.py \
  --endpoint http://127.0.0.1:10008/api/v1/query --container "$META_CONTAINER" \
  --uniform-metadata --control "$META_RESULTS/control.json" \
  --reps 3 --output "$META_RESULTS/candidate.json"
start_meta_api true /profiles "$META_ROOT/profiled"
python -I sage-data-rework/tiered_system/experiments/exp82_whole_range_profiles.py \
  --endpoint http://127.0.0.1:10008/api/v1/query --container "$META_CONTAINER" \
  --uniform-metadata --control "$META_RESULTS/candidate.json" --profiles "$META_ROOT/profiled" \
  --reps 3 --output "$META_RESULTS/candidate-profiled.json"
docker stop "$META_CONTAINER"
```

The runner requires the same image/resources/runtime settings, verifies the
uniform metadata configuration and source signatures, and checks that every
profiled month request actually records one metadata tuple and applies the
fast path. It captures cgroup peak/OOM/restart counters in each fresh phase.
Primary decisions use unprofiled HTTP totals; profiles explain discovery and
main-query work separately. Keep current full schema/client compatibility
checks from exp81. Do not retry NDJSON months merely to claim memory safety;
that remains a separate measured question.

No MAP-to-VARIANT migration, new Parquet layout, time windows, remote storage
writes, live service changes, commits or pushes were performed here.

#### Measured on a same-image sidecar (2026-10-26)

`go test -tags=duckdb_arrow ./...` passed with the REST/export variables
unset, including `TestColdUniformMetadataNativeLifecycle`. The evaluation
image is `beehive-data-api:uniform-meta-eval-1793060263`. Control and
candidate used that image, the adopted warehouse, two CPUs, a 14 GB cap,
async=8, read-ahead=-1 and Zstd. The API on port 10006 was not recreated.
No phase restarted or recorded an OOM event. Every response matched the
full-layout sweep: 25,940,207 March rows and 31,189,045 April rows. The
sidecar was stopped afterward.

Unprofiled medians of three, warmups excluded:

| Query | Control first | Candidate first | Control total | Candidate total |
|---|---:|---:|---:|---:|
| March series month | 69.881 s | 135.651 s | 76.397 s | 142.589 s |
| April series month | 67.104 s | 155.432 s | 76.019 s | 166.361 s |

Every measured candidate repetition is slower than every control repetition.
March candidate totals are 169.720/139.292/142.589 s against control
76.397/75.929/81.918 s. April candidate totals are 177.419/156.850/166.361 s
against control 76.019/79.404/73.335 s.

The profiled candidate did apply the fast path: one discovery row and
`metadata_reuse_applied=1` on every measured month. Discovery itself takes
70.950 s in March and 78.918 s in April. The native result is ready only
after that, at 71.377 s and 79.379 s. The main query's large projection
falls to 4.516 s and 5.522 s, while its scan operator records 79.983 s and
97.098 s and its sort 10.331 s and 15.086 s. Those operator seconds are not
additive HTTP time. Row groups stay 1,357/32,924 and 1,491/30,628. Engine
buffer memory stays about 3.00–3.02 GB, with temp spill 2.41 GB and 3.00 GB.
Phase cgroup peaks are 6.32, 6.46 and 6.59 GiB.

The constant metadata removes most of the projection work and does not pay
off the cost of a second scan of the month. The flag stays off. Artifacts:
`results/meta-eval-1793060263/control.json`, `candidate.json` and
`candidate-profiled.json`.

### Final iteration comparison (exp85, 2026-10-26)

The [iteration comparison](11_query_performance_summary.md) contains separate
Arrow and NDJSON tables, month response profiles, recorded server operator /
buffer / spill profiles, a separate local-candidate matrix, and an inventory
of the retained exp46–84 reports. Each timing row links its source and labels
its timer. Failed, partial, unsupported and unmeasured results remain explicit;
historical v2, initial v3 and adopted-layout measurements are distinguished.
The [CSV](../tiered_system/experiments/results/exp85_iteration_summary.csv)
and [JSON](../tiered_system/experiments/results/exp85_iteration_summary.json)
provide the same comparison data; the JSON records source SHA-256 hashes.

Production Arrow is `null` because the deployed API does not support it.
The production NDJSON baseline was collected on 2026-10-27 from
`https://data.sagecontinuum.org/api/v1/query` with the HTTP receive/spool
timer. Short queries used one excluded warmup and three measured repetitions.
March, April and May are single series runs. The earlier DNS failure is
retained in `results/exp85_production_ndjson_dns.json`. An intermediate run
stopped when the all-name minute was not time-sorted; that partial file is
`results/exp85_production_ndjson_order_abort.json`. The completed report is
`results/exp85_production_ndjson.json` (same bytes as
`results/exp85_production_ndjson_vm.json`).

Measured NDJSON medians: series minute **0.330 s** (698 rows), hour
**1.632 s** (43,509), day **14.433 s** (1,048,491). All-name minute
**1.055 s** returned **6,513** rows and all-name hour **6.226 s** returned
**416,758** rows. Those two counts differ from the demo warehouse (6,352 and
395,989) and the bodies are not time-ordered, so those cells are not
comparable. Series months match the reference counts and are time-ordered:
March **344.934 s** / 8.95 GB, April **474.271 s** / 10.76 GB, May
**444.530 s** / 10.50 GB. First-byte times are about 0.09–0.30 s. There is
no production operator or memory profile. Demo Influx timings are not used
as this baseline. The adopted-layout NDJSON month remains unmeasured.

The current API, warehouse and configuration remain unchanged. Remote scan
tuning is deferred. No git commits or pushes were made.

### Remove rejected runtime prototypes (2026-10-27)

Removed uniform-metadata discovery from `beehive-data-api`, including its
backend field, flag/environment setting, Arrow/NDJSON prepasses, context plan,
profiling counters and candidate-specific tests/exporter. Both demo compose
files no longer forward the setting. Exp84 results above remain historical;
the candidate is removed rather than left as an operator option.

Also removed the split-value Arrow prototype and its encoder, decoder, SQL
export helper and prototype-only tests. The API now passes an Arrow boolean
through the request context and uses the public four-column schema directly
throughout the handler, encoder and cold-reader validation. The client retains
batched/dictionary JSON decoding and nanosecond bounds, but no longer decodes
obsolete split-value or MAP wire schemas. Stored `meta MAP` is unchanged;
public Arrow `meta` remains a JSON string. NDJSON negotiation/fallback, Zstd,
profiling, head/tail, tier composition and v3 compatibility tests remain.

Validation: 23 offline client tests pass. An isolated build of the current
Arrow/HTTP/tiered/SQL/settings/record/profiling code passes 25 top-level Go
tests (74 including subtests), with the optional compression export probe
skipped. That build uses cached Arrow 18.8 and local dependency replacements
in a temporary directory; repository dependency files were not changed.
The full pinned/native Go build is still pending VM execution because the
pinned modules are unavailable here. Socket-based client tests cannot bind
locally, production-network tests fail DNS, and the opt-in v3 fixture replay
requires its native fixture export. Formatting and whitespace checks pass.

Historical exp61/exp84 exporters depended on the removed prototypes; their
saved measurements remain available, but those probes are no longer part of
the current API/client source. Accepted runtime diagnostics and v3 regression
coverage remain. No service restart, warehouse write, git commit or push was
performed during cleanup.

### Internal Go packages (2026-10-27)

The API now keeps only startup wiring in its root `main.go`. Implementation
and tests are grouped under `internal/query`, `internal/arrowio`,
`internal/backend/{hot,cold,tiered}`, `internal/httpapi`, and
`internal/testutil`. The Influx-backed package is named `hot`.
The Arrow writer uses the shared `query.CompositeResults` contract instead
of accessing the tiered backend's private result fields. Child ordering,
error propagation and caller-owned cleanup are retained. Shared filter
validation lives with query contracts; cold SQL baselines remain test-only.

Docker now copies the nested source tree. CI explicitly enables CGO and runs
build, tests and vet across packages. The root build command, executable,
module path, dependency files, flags, HTTP schemas and warehouse configuration
are unchanged. Probe binaries must be compiled from their owning package:

```sh
cd beehive-data-api
go test -tags=duckdb_arrow -c -o /tmp/sage-cold-probe.test ./internal/backend/cold
go test -tags=duckdb_arrow -c -o /tmp/sage-arrow-probe.test ./internal/arrowio
```

Exp70's automatic SQL-export invocation and the maintained encoder/execution
probe instructions target these packages. Historical experiment results and
source hashes were not regenerated. Removed prototypes remain removed.

Validation here: the selected current Arrow, HTTP, tiered, hot SQL and cold
SQL/settings/record/profile/authentication sources pass 31 top-level tests
(93 including subtests) and `go vet` in an isolated dependency-cache build.
Three opt-in probes skip by default. Separate smoke invocations of the
relocated SQL exporter and IPC encoder pass, preserving SQL markers, public
schema, values and nanoseconds. This validation uses cached Arrow 18.8/JWT
5.3.1 and local module replacements; the temporary copy excludes the
unavailable Influx SDK integration and native DuckDB sources. Repository
`go.mod` and `go.sum` were checked byte-for-byte against the pre-refactor copy.
All actual packages are discovered without import cycles, and formatting,
whitespace and changed Python-runner syntax checks pass.

VM validation on 2026-10-27, in `golang:1.26-bookworm` with `CGO_ENABLED=1`:
`go build -tags=duckdb_arrow ./...`, `go test -tags=duckdb_arrow ./...` and
`go vet -tags=duckdb_arrow ./...` passed. Arrow, hot, tiered and HTTP tests
passed; the cold package passed in 0.586 s. Opt-in probes stayed skipped.
A default build without the tag fails because pinned `duckdb-go/v2`
`arrowmapping` is still behind `//go:build duckdb_arrow`. The cold memory
probe also needed `arrowio.WriteReader` after that helper left package main.
The image `beehive-data-api:internal-20261027` built. A temporary container
on port 10007, pointed at the adopted warehouse, returned the March 1 series
minute as 698 NDJSON rows and 698 Zstd Arrow rows, first timestamp
`2025-03-01T00:00:00.026403194Z`, value 11.7, and the public four-field
schema. That container was removed. The APIs on ports 10000 and 10006 were
not recreated. Sage Data Client was not modified. No warehouse write, commit
or push was performed.
