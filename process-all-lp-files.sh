#!/bin/bash

# find /media/local/influxdb-export-lp -name '*.lp.gz' | xargs -L 1 ..

for f in $*; do
    if test -e "${f}.done"; then
        echo "skip ${f}"
    fi
    echo "processing ${f}"
    if gzip -dc "${f}" | ~/go/bin/convert-lp-to-json | python3 process-lp-files.py --output-dir /media/local/parquet-data-store; then
        touch "${f}.done"
        echo "done ${f}"
    else
        echo "fail ${f}"
    fi
done
