import sage_data_client
import os
import os.path
import datetime
import pandas as pd


def date_range(start, end):
    d = start
    while d < end:
        yield d
        d += datetime.timedelta(days=1)

dates = list(date_range(datetime.date(2025, 1, 1), datetime.date.today()))

for d in reversed(dates):
    os.makedirs("inputs", exist_ok=True)
    filename = f"inputs/{d}.csv"
    tempname = f"{filename}.tmp"

    if os.path.exists(filename):
        print(f"csv already exists for {d}. skipping!")
        continue

    print(f"querying data for {d}...") 
    df = sage_data_client.query(
        start=d,
        end=d+datetime.timedelta(days=1),
        filter={
            "name": "env.*",
            "sensor": "bme680",
        }
    )

    if len(df) == 0:
        print(f"no data date {d}!")
        with open(tempname, "w") as f:
            print("time", "vsn", "t", "p", "rh", sep=",", file=f)
        os.rename(tempname, filename)
        continue

    print(f"grouping measurements for {d}...")
    df.sort_values(["meta.vsn", "timestamp"], inplace=True)
    vsn_changed = df["meta.vsn"].ne(df["meta.vsn"].shift())
    time_has_gap = df["timestamp"].diff() > pd.Timedelta(seconds=3)
    df["batch"] = (vsn_changed | time_has_gap).cumsum()

    print(f"writing output for {d}...")
    with open(tempname, "w") as f:
        print("time", "vsn", "t", "p", "rh", sep=",", file=f)

        for _, rows in df.groupby("batch"):
            # TODO Handle cases where data is more frequent and look into cases where you get data from multiple zones.
            if len(rows) != 3:
                continue

            vsn = rows.iloc[0]["meta.vsn"]

            T = 0
            RH = 0
            P = 0

            for i in range(3):
                r = rows.iloc[i]
                if r["name"] == "env.temperature":
                    T = r["value"]
                elif r["name"] == "env.relative_humidity":
                    RH = r["value"]
                elif r["name"] == "env.pressure":
                    P = r["value"]

            time = rows["timestamp"].mean()

            print(time.isoformat(), vsn, T, P, RH, sep=",", file=f)

    os.rename(tempname, filename)