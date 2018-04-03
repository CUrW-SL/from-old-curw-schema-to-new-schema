import csv
import pymysql

from mysql_config import OLD_DB_CONFIG, NEW_DB_CONFIG
from mysql_config import START_DATE_TIME


def create_db_coonections():
    # create database connections.
    old_db_connection = pymysql.connect(host=OLD_DB_CONFIG['host'],
                                  port=OLD_DB_CONFIG['port'],
                                  user=OLD_DB_CONFIG['user'],
                                  password=OLD_DB_CONFIG['password'],
                                  db=OLD_DB_CONFIG['db'],
                                  autocommit=True)

    new_db_connection = pymysql.connect(host=NEW_DB_CONFIG['host'],
                                  port=NEW_DB_CONFIG['port'],
                                  user=NEW_DB_CONFIG['user'],
                                  password=NEW_DB_CONFIG['password'],
                                  db=NEW_DB_CONFIG['db'],
                                  autocommit=True)
    ## end of database connection creation.

    return old_db_connection, new_db_connection


old_curw_timeseries_query = "SELECT `id`, `type`, `name` FROM `run_view` " \
                       "WHERE `station`=%s AND `variable`=%s AND `source`=%s"

old_curw_timseries_data_query = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `time`>=%s"

new_curw_timeseries_query = "INSERT INTO `timeseries` (`sd_id`, `date_time`, `value`, `type`, `sim_tag`) " \
                                 "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`);"

with open('station_descriptor.csv') as csvfile:
    # Read the scv file. will get an array of arrays.
    read_csv = csv.reader(csvfile, delimiter=',')
    # Destructuring the read_csv array to separate meta-data and data.
    meta_data, *data_matrix = read_csv

# iterate over out.csv row, extracts timeseries and push them.
for data in data_matrix:

    old_db_conn, new_db_conn = create_db_coonections()

    sd_id = data[0]
    station_name = data[1]
    parameter = data[2]
    program = data[3]

    # extract timeseries ids from mysql old curw db
    with old_db_conn.cursor() as ol_db_cursor:
        ol_db_cursor.execute(old_curw_timeseries_query, (station_name, parameter, program))
        tss = ol_db_cursor.fetchall()

    for ts in tss:
        ts_id = ts[0]
        type = ts[1]
        sim_tag = ts[2]
        with old_db_conn.cursor() as ol_db_cursor:
            ol_db_cursor.execute(old_curw_timseries_data_query, (ts_id, START_DATE_TIME))
            timeseries = ol_db_cursor.fetchall()

        new_timeseries = []
        for value in timeseries:
            new_timeseries.append((sd_id, value[0], value[1], type, sim_tag))

        print(ts_id, len(new_timeseries))
        with new_db_conn.cursor() as new_db_cursor:
            new_db_cursor.executemany(new_curw_timeseries_query, new_timeseries)
            print("Inserted/Updated %d no of rows of (sd_id: %s, type: %s, sim_tag: %s)"
                  % (new_db_cursor.rowcount, sd_id, type, sim_tag))
        print()

    try:
        old_db_conn.close()
    except Exception as ex:
        print("Error while closing mysql connection...", ex)

    try:
        new_db_conn.close()
    except Exception as ex:
        print("Error while closing psql connection...", ex)