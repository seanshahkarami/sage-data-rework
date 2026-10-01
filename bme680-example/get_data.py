import sage_data_client
import os
import os.path
import pandas as pd


dates = pd.date_range("2020-01-01", "today", tz="utc")

for d in reversed(dates):
    # we pad query start and end time in case there are any values right on the date boundary
    start_time = d - pd.Timedelta(minutes=5)
    end_time = d + pd.Timedelta(days=1, minutes=5)
    d = d.date()

    os.makedirs("inputs", exist_ok=True)
    filename = f"inputs/{d}.csv"
    tempname = f"{filename}.tmp"

    if os.path.exists(filename):
        print(f"csv already exists for {d}. skipping!")
        continue

    print(f"querying data for {d}...") 
    df = sage_data_client.query(
        start=start_time,
        end=end_time,
        filter={
            "name": "env.*",
            "plugin": "waggle/plugin-iio:.*",
            "sensor": "bme680",
            "zone": "shield",
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
    app_changed = df["meta.plugin"].ne(df["meta.plugin"].shift())
    vsn_changed = df["meta.vsn"].ne(df["meta.vsn"].shift())
    time_has_gap = df["timestamp"].diff() > pd.Timedelta(seconds=3)
    df["batch"] = (app_changed | vsn_changed | time_has_gap).cumsum()

    print(f"writing output for {d}...")
    with open(tempname, "w") as f:
        print("time", "vsn", "t", "p", "rh", "app", sep=",", file=f)

        for _, rows in df.groupby("batch"):
            # TODO Handle cases where data is more frequent and look into cases where you get data from multiple zones.
            # TODO Handle case where data sits a little before or after date. Consider padding and filtering
            # based on min / max batch timestamp.
            if len(rows) < 3:
                print("warning: too few rows! will using null for missing!")
                print(rows)
            if len(rows) > 3:
                print("too many rows")
                print(rows)
                continue

            time = rows["timestamp"].mean()

            # ignore bounary values which would fall on a different date
            if time < start_time or time >= end_time:
                continue

            vsn = rows.iloc[0]["meta.vsn"]
            app = rows.iloc[0]["meta.plugin"]

            T = ""
            RH = ""
            P = ""

            for i in range(len(rows)):
                r = rows.iloc[i]
                if r["name"] == "env.temperature":
                    T = r["value"]
                elif r["name"] == "env.relative_humidity":
                    RH = r["value"]
                elif r["name"] == "env.pressure":
                    P = r["value"]

            print(time.isoformat(), vsn, T, P, RH, app, sep=",", file=f)

    os.rename(tempname, filename)