#!/usr/bin/env python
"""
A cold-storage backend for the Beehive query API, over the Parquet archive.

Goal: take the exact JSON query object that beehive-data-api already accepts
and return the exact NDJSON records it already returns, reading Parquet
instead of InfluxDB. If the logical results differ, the archive cannot be
swapped in underneath users.

Semantics replicated from beehive-data-api (types.go, influx_backend.go,
service.go). The subtle ones, each of which is a way to get this wrong:

  - `start` is INCLUSIVE, `end` is EXCLUSIVE (Flux range()).
  - `name` maps to the measurement column; every other filter key is a tag.
  - Filter dispatch is by substring, checked in this order:
        pattern contains "|"  -> regex  /^(pattern)$/
        pattern contains "*"  -> regex  /^pattern$/
        otherwise             -> exact string equality
    So "env.temperature" is an EXACT match where "." is literal, but
    "env.temp.*" is a REGEX where "." is any character. Same dot, two
    meanings, decided by whether a star appears elsewhere in the string.
  - Patterns are anchored ^...$ and otherwise NOT escaped: they are real
    regexes, not globs.
  - head/tail are PER SERIES, not global. A series is the measurement plus
    the full set of tag values.
  - head and tail together is an error.
  - Response is {timestamp, name, value, meta} with meta holding every tag,
    including the promoted ones.

The archive splits tags across promoted columns and a `meta` map, and splits
values across value_float/value_int/value_str. Both have to be folded back
together to match what the API returns.
"""
import json
import re
from datetime import datetime, timedelta, timezone

# Mirrors validQueryStringRE in influx_backend.go: "^[A-Za-z0-9+-_.*:| ]*$".
# Note +-_ is a character RANGE (0x2B..0x5F), not three literals, so it also
# admits / \ , ; < = > ? @ [ ] ^ and digits. Replicated deliberately.
VALID_QUERY_STRING_RE = re.compile(r"^[A-Za-z0-9+\-_.*:| /\\,;<=>?@\[\]^]*$")
# Mirrors metaRE in service.go.
META_KEY_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

PROMOTED_TAGS = ["plugin", "vsn", "node", "host", "task", "job", "zone"]

DURATION_RE = re.compile(r"^(-|\+)?((\d+)(ns|us|ms|s|m|h|d|w|mo|y))+$")
DURATION_PART_RE = re.compile(r"(\d+)(ns|us|ms|s|m|h|d|w|mo|y)")
_UNIT_SECONDS = {
    "ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1,
    "m": 60, "h": 3600, "d": 86400, "w": 604800,
    "mo": 2592000, "y": 31536000,
}


class QueryError(Exception):
    """Mirrors the API's 400/500 distinction loosely; message text matters."""


def parse_time(s, now=None):
    """Parse a Flux-style time: RFC3339, relative duration, or unix seconds."""
    now = now or datetime.now(timezone.utc)
    s = s.strip()
    if not s:
        raise QueryError("empty timestamp")
    if DURATION_RE.match(s):
        sign = -1 if s.startswith("-") else 1
        body = s.lstrip("+-")
        total = 0.0
        for n, unit in DURATION_PART_RE.findall(body):
            total += int(n) * _UNIT_SECONDS[unit]
        return now + timedelta(seconds=sign * total)
    try:
        return datetime.fromtimestamp(int(s), tz=timezone.utc)
    except ValueError:
        pass
    iso = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def filter_to_sql(field, pattern):
    """Translate one filter entry to a SQL predicate, matching influx_backend.go.

    The column expression differs per field because the archive splits tags
    between real columns and the `meta` map, but the match semantics must not.
    """
    if field == "name":
        col = "name"
    elif field in PROMOTED_TAGS:
        col = field
    else:
        # Not a promoted column: look it up in the meta map. A missing key
        # must not match, which element_at gives us (NULL).
        col = f"meta[{sql_quote(field)}]"

    if "|" in pattern:
        return f"regexp_matches({col}, {sql_quote('^(' + pattern + ')$')})"
    if "*" in pattern:
        return f"regexp_matches({col}, {sql_quote('^' + pattern + '$')})"
    return f"{col} = {sql_quote(pattern)}"


def validate(query):
    """Replicates parseQuery in service.go plus the per-field checks."""
    if "start" not in query or query["start"] == "":
        raise QueryError("missing start field")
    known = {"bucket", "start", "end", "head", "tail",
             "experimental_func", "experimental_window", "filter"}
    unknown = set(query) - known
    if unknown:
        raise QueryError(f'json: unknown field "{sorted(unknown)[0]}"')
    if query.get("head") is not None and query.get("tail") is not None:
        raise QueryError("head and tail cannot both be specified")
    if query.get("experimental_window") and not query.get("experimental_func"):
        raise QueryError("window cannot be used without aggregation function")
    for k, v in (query.get("filter") or {}).items():
        if not META_KEY_RE.match(k):
            raise QueryError(f"invalid filter key: {k!r}")
        if not VALID_QUERY_STRING_RE.match(v):
            raise QueryError(f"invalid filter field pattern {v!r}")
    for key in ("start", "end"):
        if key in query and query[key] and not VALID_QUERY_STRING_RE.match(query[key]):
            raise QueryError(f"invalid {key} timestamp {query[key]!r}")


def day_dirs_for_range(start, end):
    """The date= partitions a [start, end) range can touch, in UTC."""
    d = start.astimezone(timezone.utc).date()
    last = (end - timedelta(microseconds=1)).astimezone(timezone.utc).date()
    out = []
    while d <= last:
        out.append(f"date={d.isoformat()}")
        d += timedelta(days=1)
    return out


def build_sql(query, file_list_sql, now=None):
    """Compile a Beehive query object into one DuckDB SQL statement."""
    validate(query)
    start = parse_time(query["start"], now)
    end = parse_time(query["end"], now) if query.get("end") else (now or datetime.now(timezone.utc))

    preds = [
        f"time >= TIMESTAMPTZ {sql_quote(start.isoformat())}",
        f"time <  TIMESTAMPTZ {sql_quote(end.isoformat())}",
    ]
    for field, pattern in sorted((query.get("filter") or {}).items()):
        preds.append(filter_to_sql(field, pattern))
    where = " AND ".join(preds)

    # Fold the three value columns back into one, and the promoted tag columns
    # back into the meta map the API returns.
    # Build the promoted-tag map from parallel lists so NULL tags can be
    # dropped: a missing tag must be absent from meta, not present as NULL.
    # (DuckDB has no map_filter; list_filter on zipped lists is the equivalent.)
    keys = "[" + ", ".join(sql_quote(t) for t in PROMOTED_TAGS) + "]"
    vals = "[" + ", ".join(PROMOTED_TAGS) + "]"
    promoted_map = (
        f"map_from_entries(list_filter(list_zip({keys}, {vals}), x -> x[2] IS NOT NULL))"
    )
    value_expr = "COALESCE(value_float::VARCHAR, value_int::VARCHAR, value_str)"
    select = f"""
        SELECT
            time AS timestamp,
            name,
            value_float, value_int, value_str,
            map_concat(COALESCE(meta, MAP{{}}::MAP(VARCHAR,VARCHAR)),
                       {promoted_map}) AS meta
        FROM read_parquet({file_list_sql})
        WHERE {where}
    """

    head, tail = query.get("head"), query.get("tail")
    if head is not None or tail is not None:
        n = head if head is not None else tail
        order = "ASC" if head is not None else "DESC"
        # head/tail are per series. Series identity is the measurement plus the
        # full tag set, so partition by name and the whole meta map.
        select = f"""
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY name, meta ORDER BY timestamp {order}
                ) AS rn
                FROM ({select})
            ) WHERE rn <= {int(n)}
        """
    return select + " ORDER BY timestamp"


def record_from_row(row):
    """Build the API's {timestamp, name, value, meta} record."""
    ts, name, vf, vi, vs, meta = row
    value = vf if vf is not None else (vi if vi is not None else vs)
    # Go emits RFC3339Nano with trailing zeros trimmed.
    iso = ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {"timestamp": iso, "name": name, "value": value,
            "meta": {k: v for k, v in sorted((meta or {}).items())}}


def query_archive(con, archive_root, query, now=None):
    """Run a Beehive query against the Parquet archive; yield API records."""
    import os

    validate(query)
    start = parse_time(query["start"], now)
    end = parse_time(query["end"], now) if query.get("end") else (now or datetime.now(timezone.utc))

    files = []
    for d in day_dirs_for_range(start, end):
        p = os.path.join(archive_root, d)
        if os.path.isdir(p):
            files += [os.path.join(p, f) for f in sorted(os.listdir(p)) if f.endswith(".parquet")]
    if not files:
        return [], "", 0

    file_list_sql = "[" + ",".join(sql_quote(f) for f in files) + "]"
    sql = build_sql(query, file_list_sql, now)
    rows = con.execute(sql).fetchall()
    return [record_from_row(r) for r in rows], sql, len(files)
