#!/usr/bin/env bash

start_date_time=`date '+%Y-%m-%d 00:00:00' -d "3 days ago"`

echo "activating virtual env..."
source venv/bin/activate

echo "updating cruw_new tables..."
python mysql_update_new_schema_tables.py

echo
echo "pushing the timeseries from onwards $start_date_time"
python mysql_push_timeseries_to_new_schema.py -d "$start_date_time"

echo "deactivating virtual env"
deactivate