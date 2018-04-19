#!/usr/bin/env bash

cd /home/uwcc-admin/from-old-curw-scehma-to-new-schema/
touch logs/mysql_push_`date +%Y-%m-%d`.log

start_date_time=`date '+%Y-%m-%d 00:00:00' -d "3 days ago"`
echo $start_date_time

echo "activating virtual env..."
source venv/bin/activate

echo "updating cruw_new tables on $start_date_time" >> logs/mysql_push_`date +%Y-%m-%d`.log
python mysql_update_new_schema_tables.py >> logs/mysql_push_`date +%Y-%m-%d`.log 2>&1

echo "pushing the timeseries from onwards $start_date_time" >> logs/mysql_push_`date +%Y-%m-%d`.log
python mysql_push_timeseries_to_new_schema.py -d "$start_date_time" >> logs/mysql_push_`date +%Y-%m-%d`.log 2>&1

echo "deactivating virtual env"
deactivate