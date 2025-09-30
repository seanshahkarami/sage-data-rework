from pathlib import Path
from datetime import date, timedelta
from multiprocessing import Pool
import subprocess

INFLUXD = "/home/sean/influxdb2-2.7.12/usr/bin/influxd"
BUCKET_ID = "b3a4e89ad74c5acc"
ENGINE_DIR = Path(
    "/media/local/pvc-6da578ef-e9bc-47fc-9f64-cfe30a24ff5e_shared_influxdb-data-beehive-influxdb-0/engine"
)
OUTPUT_DIR = Path("/media/local/influxdb-export-lp")


def export_date(start_date: date, end_date: date):
    output_path = Path(OUTPUT_DIR, f"{start_date}.lp.gz")
    temp_path = Path(OUTPUT_DIR, f"{start_date}.lp.gz.tmp")

    if output_path.exists():
        print("skip", start_date, end_date)
        return

    try:
        subprocess.check_call(
            [
                INFLUXD,
                "inspect",
                "export-lp",
                "--bucket-id",
                BUCKET_ID,
                "--engine-path",
                ENGINE_DIR,
                "--output-path",
                str(temp_path),
                "--start",
                f"{start_date}T00:00:00Z",
                "--end",
                f"{end_date}T00:00:00Z",
                "--compress",
            ]
        )
    except subprocess.CalledProcessError:
        print("err", start_date, end_date)
        return

    temp_path.rename(output_path)
    print("ok", start_date, end_date)


def main():
    start = date(2025, 1, 1)
    end = date.today()

    dates = []

    currdate = start

    while currdate < end:
        nextdate = currdate + timedelta(days=1)
        dates.append((currdate, nextdate))
        currdate = nextdate

    with Pool(8) as pool:
        pool.starmap(export_date, dates)


if __name__ == "__main__":
    main()
