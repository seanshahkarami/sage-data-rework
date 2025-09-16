#!/bin/bash

# In this case, we do the explicitly parquet filter ourselves to ensure we get compatible schemas. In practice,
# this could even be filtered down to the level of plugins.
echo
echo "======== Example of computing stats on measurement ========"
echo

time duckdb <<'SQL'
select vsn, host, date, count(value), min(value) as min_temperature, max(value) as max_temperature, avg(value) avg_temperature
from 'data/*/measurement=env.temperature/*/*.parquet'
where measurement = 'env.temperature'
group by vsn, host, date
order by vsn, host, date;
SQL

echo
echo "======== Number of measurements by measurement ========"
echo

time duckdb <<'SQL'
select measurement, count(*) as measurements
from 'data/**/*.parquet'
group by measurement
order by measurements;
SQL

echo
echo "======== Number of measurements by plugin ========"
echo

time duckdb <<'SQL'
select plugin, count(*) as measurements
from 'data/**/*.parquet'
group by plugin
order by measurements;
SQL

echo
echo "======== Number of uploads by node ========"
echo

time duckdb <<'SQL'
select vsn, count(*) as uploads
from 'data/**/*.parquet'
where measurement = 'upload'
group by vsn
order by uploads;
SQL


echo
echo "======== Temperature counts by sensor ========"
echo

time duckdb <<'SQL'
select meta['sensor'] as sensor, count(measurement)
from 'data/*/measurement=env.temperature/*/*.parquet'
group by sensor;
SQL


echo
echo "======== Temperature counts by sensor ========"
echo

time duckdb <<'SQL'
select count(measurement)
from 'data/*/*/*/*.parquet';
SQL
