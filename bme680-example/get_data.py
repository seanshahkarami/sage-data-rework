import sage_data_client
import sys
import datetime
import pandas as pd


def date_range(start, end):
    d = start
    while d < end:
        yield d
        d += datetime.timedelta(days=1)

for d in date_range(datetime.date(2026, 1, 1), datetime.date(2026, 2, 1)):
    print(f"working on {d}")

    print("querying data...") 
    df = sage_data_client.query(
        start=d,
        end=d+datetime.timedelta(days=1),
        filter={
            "name": "env.*",
            "sensor": "bme680",
        }
    )

    print("grouping measurements...")
    df.sort_values(["meta.vsn", "timestamp"], inplace=True)

    vsn_changed = df["meta.vsn"].ne(df["meta.vsn"].shift())
    time_has_gap = df["timestamp"].diff() > pd.Timedelta(seconds=3)
    df["batch"] = (vsn_changed | time_has_gap).cumsum()

    print("writing output...")
    with open(f"inputs/{d}.csv", "w") as f:
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
