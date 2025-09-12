#!/bin/bash

echo
echo "======== Example of computing stats on measurement ========"
echo

duckdb <<'SQL'
select vsn, host, date, min(value) as min_temperature, max(value) as max_temperature, avg(value) avg_temperature
from 'data/**/*.parquet'
where measurement = 'env.temperature'
group by vsn, host, date;
SQL

echo
echo "======== Number of measurements by measurement ========"
echo

duckdb <<'SQL'
select measurement, count(*) as measurements
from 'data/**/*.parquet'
group by measurement
order by measurements;
SQL

echo
echo "======== Number of measurements by plugin ========"
echo

duckdb <<'SQL'
select plugin, count(*) as measurements
from 'data/**/*.parquet'
group by plugin
order by measurements;
SQL

echo
echo "======== Number of uploads by node ========"
echo

duckdb <<'SQL'
select vsn, count(*) as uploads
from 'data/**/*.parquet'
where measurement = 'upload'
group by vsn
order by uploads;
SQL
