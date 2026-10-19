"""exp41: can Iceberg bound old metadata.json versions by itself?

exp40 Q7 found that `expire_snapshots()` does not reclaim stored bytes,
and named `write.metadata.delete-after-commit.enabled` as the knob that
does. It did not establish how that knob BEHAVES, and the plans/6 design
hangs on exactly that: the reconciler publishes `version-hint.text`
naming a metadata file, and if Iceberg deletes that file the hint stops
resolving.

So this is not "is there a retention setting" -- there is, it is built
in, nothing needs building. It is "what is the retention WINDOW measured
in, and can it delete a file a published hint still names."

  Q1. Is retention built in, and does it bound storage?        (yes)
  Q2. Is the window count-based or age-based?                  (COUNT)
  Q3. Can it delete the file a published hint names?           (YES)
  Q4. How does the failure present to a consumer?              (hard error)
  Q5. Does a larger window push the failure out predictably?   (yes)

Q3 is the one that matters. A count-based window means the safe setting
depends on the COMMIT RATE, not on the reconciler period in isolation --
a window of 5 is hours at hourly commits and months at daily ones. That
makes `previous-versions-max` a safety parameter coupled to item 1 of
plans/6, which is why exp40 stopped short of recommending a value.

OUTCOME, recorded after the fact. Sage's cadence was stated as MONTHLY
(InfluxDB hot store, one batch per month into cold storage) after this
experiment was written. At 12 commits/yr the default previous-versions-max
of 100 carries ~8.4 years of margin, and the growth retention would bound
is 6.4 MB per decade -- so the recommendation is to leave deletion OFF and
not tune anything. None of the measurements below change; what changes is
that they describe a risk Sage does not run. Kept because the window is
count-based, so a cadence change makes this live again, and Q5's
break-point rule is what sizes it then.

Local only: tmpdir warehouse, nothing uploaded, no Pelican, no archive
access. This is a property of pyiceberg's commit path.

Run:  conda run -n sage_pipeline python -I exp41_metadata_retention.py
"""
import glob
import os
import shutil
import sys
import tempfile

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

SCHEMA = pa.schema([pa.field("id", pa.int64())])
RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def new_table(props=None):
    wd = tempfile.mkdtemp(prefix="exp41-")
    os.makedirs(os.path.join(wd, "wh"))
    from pyiceberg.catalog.sql import SqlCatalog
    cat = SqlCatalog("s", uri=f"sqlite:///{os.path.join(wd, 'c.db')}",
                     warehouse="file://" + os.path.join(wd, "wh"))
    cat.create_namespace("sage")
    t = cat.create_table("sage.obs", schema=SCHEMA, properties=props or {})
    return wd, cat, t


def meta_glob(wd):
    return glob.glob(os.path.join(wd, "wh", "sage", "obs", "metadata",
                                  "*.metadata.json"))


def local(path):
    return path.replace("file://", "")


# --------------------------------------------------------------------------
def q1_builtin_retention():
    """Is bounding old metadata built in, or does it need building?"""
    print("\n--- Q1: is metadata retention built in? ---")
    out = {}
    for label, props in (
        ("default (delete-after-commit off)", {}),
        ("retention on, keep 5", {
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "5"}),
    ):
        wd, cat, t = new_table(props)
        try:
            for i in range(40):
                t.append(pa.table({"id": [i]}, schema=SCHEMA))
            files = meta_glob(wd)
            tot = sum(os.path.getsize(p) for p in files)
            out[label] = (len(files), tot)
            print(f"  {label:<36} {len(files):>3} files, {tot:>8,} B")
        finally:
            shutil.rmtree(wd, ignore_errors=True)

    off_n, off_b = out["default (delete-after-commit off)"]
    on_n, on_b = out["retention on, keep 5"]
    check("Q1: retention is built in and bounds storage", on_n < off_n and on_b < off_b,
          f"{off_n} files/{off_b:,} B -> {on_n} files/{on_b:,} B; no custom tooling")
    print("       Two stock table properties. Nothing to build.")
    return off_n, on_n


def q2_count_not_age():
    """Is the window measured in versions or in time?"""
    print("\n--- Q2: is the window count-based or age-based? ---")
    # If it were age-based, 20 rapid commits would leave ~20 files (all
    # young). If count-based, it settles at the configured count.
    wd, cat, t = new_table({
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "5"})
    try:
        for i in range(20):
            t.append(pa.table({"id": [i]}, schema=SCHEMA))
        n = len(meta_glob(wd))
        print(f"  20 commits in under a second -> {n} metadata.json retained")
        check("Q2: the window is COUNT-based, not age-based", n <= 7,
              f"{n} files after 20 rapid commits; age-based would keep all 20")
        print("       Consequence: the safe value depends on COMMIT RATE.")
        print("       'keep 5' is ~5 hours at hourly commits, ~5 days at daily.")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q3_q4_hint_breaks():
    """Can retention delete the file a published hint names?"""
    print("\n--- Q3/Q4: can retention break a published version-hint? ---")
    keep = 5
    wd, cat, t = new_table({
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": str(keep)})
    try:
        t.append(pa.table({"id": [0]}, schema=SCHEMA))
        t = cat.load_table("sage.obs")

        # The plans/6 reconciler publishes the hint here, naming this file.
        hint = t.metadata_location
        print(f"  reconciler publishes hint -> {os.path.basename(hint)}")
        check("Q3a: the hint target exists when published",
              os.path.exists(local(hint)), "as the design assumes")

        broke_at = None
        for i in range(1, 15):
            t.append(pa.table({"id": [i]}, schema=SCHEMA))
            t = cat.load_table("sage.obs")
            if not os.path.exists(local(hint)):
                broke_at = i + 1
                break

        check("Q3b: retention DELETES the file a published hint names",
              broke_at is not None,
              f"gone after commit #{broke_at} (previous-versions-max={keep})")

        # Q4: what a consumer following that hint actually sees.
        from pyiceberg.table import StaticTable
        err = None
        try:
            StaticTable.from_metadata(hint)
        except Exception as e:
            err = type(e).__name__
        print(f"  consumer following the stale hint: {err or 'read OK'}")
        check("Q4: the failure is a hard error, not stale data",
              err is not None,
              f"{err} -- NOT the recoverable staleness plans/6 accepts")
        print("       plans/6's accepted failure is 'reader sees an OLDER")
        print("       snapshot'. This is 'reader sees nothing at all'.")
        return broke_at
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q5_window_scales():
    """Does a larger window push the failure out predictably?"""
    print("\n--- Q5: does the window behave predictably? ---")
    rows = []
    for keep in (2, 5, 10):
        wd, cat, t = new_table({
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": str(keep)})
        try:
            t.append(pa.table({"id": [0]}, schema=SCHEMA))
            t = cat.load_table("sage.obs")
            hint = t.metadata_location
            broke = None
            for i in range(1, 25):
                t.append(pa.table({"id": [i]}, schema=SCHEMA))
                if not os.path.exists(local(hint)):
                    broke = i + 1
                    break
            rows.append((keep, broke))
            print(f"  previous-versions-max={keep:<3} -> hint survives "
                  f"{broke - 1 if broke else '>24'} further commits")
        finally:
            shutil.rmtree(wd, ignore_errors=True)

    # The relationship is exact, not merely monotonic: a hint published
    # against the current metadata survives keep+1 further commits, and the
    # file is gone on commit keep+2. That is what makes the window sizeable
    # by arithmetic instead of by guessing.
    exact = all(b == k + 2 for k, b in rows)
    check("Q5: the break point is exactly previous-versions-max + 2", exact,
          "; ".join(f"keep={k} -> breaks at #{b}" for k, b in rows))
    print("\n  Sizing rule, from Q5: a published hint stays resolvable for")
    print("  previous-versions-max + 1 further commits. So")
    print("     previous-versions-max > commits per reconciler period + margin")
    print("\n  At Sage's MONTHLY cadence (12 commits/yr) the default of 100")
    print("  carries ~8.4 years of margin, and the growth retention would")
    print("  bound is 6.4 MB/decade -- so the recommendation is to leave")
    print("  delete-after-commit OFF. This rule is what sizes it if the")
    print("  cadence ever rises; the window is count-based, not age-based.")


def main():
    print("exp41: can Iceberg bound old metadata.json by itself?")
    q1_builtin_retention()
    q2_count_not_age()
    q3_q4_hint_breaks()
    q5_window_scales()

    print("\n" + "=" * 60)
    ok = sum(1 for _, r in RESULTS if r)
    print(f"{ok}/{len(RESULTS)} checks passed")
    for name, r in RESULTS:
        if not r:
            print(f"  FAILED: {name}")
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
