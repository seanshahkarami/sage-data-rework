"""Format timestamps exactly as Go's encoding/json does for time.Time.

Go marshals time.Time as RFC3339Nano, which TRIMS trailing zeros from the
fractional second. Python's isoformat() pads to a fixed 6 digits. Verified
against `go run` on this machine:

    ns=1746061575867813833  Go "…15.867813833Z"  Py "…15.867813Z"
    ns=1746061575500000000  Go "…15.5Z"          Py "…15.500000Z"
    ns=1746061575000000000  Go "…15Z"            Py "…15Z"

Both parse identically in pandas, so sage-data-client does not care. It
matters for the demo's tier-comparison checks, which compare raw ndjson
bytes: without this, every hot record would appear to differ from its cold
counterpart on formatting alone and hide a real mismatch.
"""


def go_rfc3339(dt, extra_ns: int = 0) -> str:
    """RFC3339Nano with trailing zeros trimmed, as Go emits it.

    `dt` is a timezone-aware datetime (µs resolution, as DuckDB and
    pyiceberg give it). `extra_ns` carries the sub-µs remainder when the
    caller has the original nanosecond value and wants full fidelity;
    cold-tier rows have none, since Iceberg stores µs.
    """
    dt = dt.astimezone(_UTC)
    frac = dt.microsecond * 1000 + extra_ns  # nanoseconds within the second
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if frac == 0:
        return base + "Z"
    s = f"{frac:09d}".rstrip("0")
    return f"{base}.{s}Z"


def go_rfc3339_ns(ns: int) -> str:
    """Same, from a raw nanosecond epoch (the hot tier's native form)."""
    from datetime import datetime

    sec, rem = divmod(int(ns), 1_000_000_000)
    base = datetime.fromtimestamp(sec, tz=_UTC).strftime("%Y-%m-%dT%H:%M:%S")
    if rem == 0:
        return base + "Z"
    return f"{base}.{f'{rem:09d}'.rstrip('0')}Z"


from datetime import timezone as _tz

_UTC = _tz.utc
