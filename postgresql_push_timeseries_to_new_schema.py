import csv
import psycopg2
import pymysql

from postgresql_config import MYSQL_DB_CONFIG, PSQL_DB_CONFIG
from postgresql_config import START_DATE_TIME


def create_db_coonections():
    # create database connections.
    mysql_conn = pymysql.connect(host=MYSQL_DB_CONFIG['host'],
                                       port=MYSQL_DB_CONFIG['port'],
                                       user=MYSQL_DB_CONFIG['user'],
                                       password=MYSQL_DB_CONFIG['password'],
                                       db=MYSQL_DB_CONFIG['db'],
                                       autocommit=True)

    psql_conn = psycopg2.connect(host=PSQL_DB_CONFIG['host'],
                                       port=PSQL_DB_CONFIG['port'],
                                       user=PSQL_DB_CONFIG['user'],
                                       password=PSQL_DB_CONFIG['password'],
                                       database=PSQL_DB_CONFIG['db'])
    psql_conn.autocommit = True
    ## end of database connection creation.

    return mysql_conn, psql_conn


old_curw_mysql_query = "SELECT `id`, `type`, `name` FROM `run_view` " \
                       "WHERE `station`=%s AND `variable`=%s AND `source`=%s"

old_curw_timseries_data_mysql_query = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `time`>=%s"

new_curw_timeseries_psql_query = "INSERT INTO timeseries (sd_id, date_time, value, type, sim_tag) " \
                                 "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sd_id, date_time, type, sim_tag) " \
                                 "DO UPDATE SET (value)=(EXCLUDED.value);"

with open('out.csv') as csvfile:
    # Read the scv file. will get an array of arrays.
    read_csv = csv.reader(csvfile, delimiter=',')
    # Destructuring the read_csv array to separate meta-data and data.
    meta_data, *data_matrix = read_csv

# iterate over out.csv row, extracts timeseries and push them.
for data in data_matrix:

    mysql_connection, psql_connection = create_db_coonections()

    sd_id = data[0]
    station_name = data[2]
    parameter = data[3]
    program = data[4]

    # extract timeseries ids from mysql old curw db
    with mysql_connection.cursor() as mysql_cursor:
        mysql_cursor.execute(old_curw_mysql_query, (station_name, parameter, program))
        tss = mysql_cursor.fetchall()

    for ts in tss:
        ts_id = ts[0]
        type = ts[1]
        sim_tag = ts[2]
        with mysql_connection.cursor() as mysql_cursor:
            mysql_cursor.execute(old_curw_timseries_data_mysql_query, (ts_id, START_DATE_TIME))
            timeseries = mysql_cursor.fetchall()

        new_timeseries = []
        for value in timeseries:
            new_timeseries.append((sd_id, value[0], value[1], type, sim_tag))

        print(ts_id, len(new_timeseries))
        with psql_connection.cursor() as psql_cursor:
            psql_cursor.executemany(new_curw_timeseries_psql_query, new_timeseries)
            print("Inserted/Updated %d no of rows of (sd_id: %s, type: %s, sim_tag: %s)"
                  % (psql_cursor.rowcount, sd_id, type, sim_tag))
        print()

    try:
        mysql_connection.close()
    except Exception as ex:
        print("Error while closing mysql connection...", ex)

    try:
        psql_connection.close()
    except Exception as ex:
        print("Error while closing psql connection...", ex)