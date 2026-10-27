"""Archive Parquet row batches -> InfluxDB line protocol.

Inverts `archive-exporter/main.go`: the archive's `meta` map holds every
InfluxDB tag that was not promoted to its own column, so faithful
reconstruction means emitting the seven promoted columns AND every meta
key as tags.

Timestamps stay in NANOSECONDS, matching the Iceberg v3 cold tier.

Measured on 2025-05-01 (full day, 146,129,405 rows), which is why the
escaping below is shaped the way it is:
    meta values needing escape  67,473,789
    meta keys needing escape             0
    measurement names needing escape     0
    value_str with quote/backslash      98
    value_str with newline           1,480   <- would corrupt the batch
    empty tag values                     0
    non-finite floats                    0
"""

PROMOTED_TAGS = ("plugin", "vsn", "node", "host", "task", "job", "zone")

# Line protocol escaping, per InfluxDB 2.x spec. Backslash first in each
# table would be wrong for tags (the spec does not escape backslash in tag
# keys/values), so the tables below are deliberately not symmetric.
_MEAS_ESCAPES = ((",", r"\,"), (" ", r"\ "))
_TAG_ESCAPES = ((",", r"\,"), ("=", r"\="), (" ", r"\ "))
# String FIELD values are quoted, so only the quote and backslash are
# escaped -- but a raw newline would terminate the record, so it must go.
_STR_ESCAPES = (("\\", "\\\\"), ('"', '\\"'))


def esc_measurement(s: str) -> str:
    for a, b in _MEAS_ESCAPES:
        s = s.replace(a, b)
    return s


def esc_tag(s: str) -> str:
    for a, b in _TAG_ESCAPES:
        s = s.replace(a, b)
    return s


def esc_str_field(s: str) -> str:
    for a, b in _STR_ESCAPES:
        s = s.replace(a, b)
    # Newlines are record separators in line protocol. 1,480 value_str
    # rows per day contain one; emitting raw would truncate the record and
    # corrupt everything after it in the batch. Escape rather than strip so
    # the round-trip check can compare against the archive.
    return s.replace("\r", "\\r").replace("\n", "\\n")


def format_value(vf, vi, vs) -> str:
    """Exactly one of the three archive value columns is non-null."""
    if vf is not None:
        # repr() keeps full float64 round-trip precision; InfluxDB parses
        # it as a float field without a type suffix.
        return repr(float(vf))
    if vi is not None:
        return f"{int(vi)}i"
    if vs is not None:
        return '"' + esc_str_field(vs) + '"'
    return ""


def encode_row(name, promoted, meta, vf, vi, vs, time_ns) -> str:
    """One line of line protocol, or '' if the row has no value."""
    field = format_value(vf, vi, vs)
    if not field:
        return ""

    parts = [esc_measurement(name)]

    # Tag set. InfluxDB stores tags sorted by key; sorting here lets the
    # server skip a re-sort and keeps output deterministic for tests.
    tags = []
    for key, val in zip(PROMOTED_TAGS, promoted):
        if val:
            tags.append((key, val))
    if meta:
        # pyarrow surfaces a map<string,string> column as a list of
        # (key, value) tuples, while .to_pylist() on some paths and plain
        # Python callers give a dict. Accept both.
        items = meta.items() if hasattr(meta, "items") else meta
        for key, val in items:
            # An empty tag value is not storable; InfluxDB drops it. The
            # archive has none, but a dropped tag would silently change the
            # series identity, so skip explicitly rather than emit "k=".
            if val:
                tags.append((key, val))
    tags.sort(key=lambda kv: kv[0])

    for key, val in tags:
        parts.append(f",{esc_tag(key)}={esc_tag(val)}")

    return f"{''.join(parts)} value={field} {int(time_ns)}"
