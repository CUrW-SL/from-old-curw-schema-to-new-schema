import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine

from mysql_config import OLD_DB_CONFIG, NEW_DB_CONFIG

# 'mysql+pymysql://<user>:<password>@<host>[:<port>]/<dbname>'
sql_conn_str = 'mysql+pymysql://%s:%s@%s:%d/%s'
curw_db_conn = create_engine(sql_conn_str % (OLD_DB_CONFIG['user'],
                                             OLD_DB_CONFIG['password'],
                                             OLD_DB_CONFIG['host'],
                                             OLD_DB_CONFIG['port'],
                                             OLD_DB_CONFIG['db']))

new_curw_db_conn = create_engine(sql_conn_str % (NEW_DB_CONFIG['user'],
                                                 NEW_DB_CONFIG['password'],
                                                 NEW_DB_CONFIG['host'],
                                                 NEW_DB_CONFIG['port'],
                                                 NEW_DB_CONFIG['db']))


def update_source():
    sql = "SELECT `source` FROM `source`"
    df_curw = pd.read_sql_query(sql=sql, con=curw_db_conn, index_col='source')
    df_curw_new = pd.read_sql_query(sql=sql, con=new_curw_db_conn, index_col='source')
    additions = df_curw.drop(index=df_curw_new.index, errors='ignore')
    additions.to_sql(name='source', con=new_curw_db_conn, if_exists='append', index=True)
    print("Added %d sources." % len(additions.index))


def update_type():
    sql = "SELECT `type` FROM `type`"
    df_curw = pd.read_sql_query(sql=sql, con=curw_db_conn, index_col='type')
    df_curw_new = pd.read_sql_query(sql=sql, con=new_curw_db_conn, index_col='type')
    additions = df_curw.drop(index=df_curw_new.index, errors='ignore')
    additions.to_sql(name='type', con=new_curw_db_conn, if_exists='append', index=True)
    print("Added %d types." % len(additions.index))


def update_unit():
    sql = "SELECT `unit`, `type` FROM `unit`"
    df_curw = pd.read_sql_query(sql=sql, con=curw_db_conn, index_col='unit')
    df_curw_new = pd.read_sql_query(sql=sql, con=new_curw_db_conn, index_col='unit')
    additions = df_curw.drop(index=df_curw_new.index, errors='ignore')
    additions.to_sql(name='unit', con=new_curw_db_conn, if_exists='append', index=True)
    print("Added %d units." % len(additions.index))


def update_parameter():
    """
    This will not ensure that all the variables in curw DB will be there in parameter table at curw_new DB.
    Better check manually and ensure.
    :return: void
    """
    curw_sql = "SELECT `variable`, `unit` FROM `run_view`"
    new_curw_sql = "SELECT `parameter`, `unit` FROM `parameter`"
    df_curw = pd.read_sql_query(sql=curw_sql, con=curw_db_conn, index_col='variable')
    df_curw.drop_duplicates(inplace=True)
    df_curw.index.names = ['parameter']
    df_curw_new = pd.read_sql_query(sql=new_curw_sql, con=new_curw_db_conn, index_col='parameter')
    additions = df_curw.drop(index=df_curw_new.index, errors='ignore')
    additions.to_sql(name='parameter', con=new_curw_db_conn, if_exists='append', index=True)
    print("Added %d parameters." % len(additions.index))


def get_station_descriptor_additions():
    curw_sql = "SELECT `station`.`name` AS `station_name`, `variable`.`variable` AS `parameter`, " \
          "`source`.`source` AS `source`, `station`.`latitude` AS `lat`, `station`.`longitude` AS `lon` " \
          "FROM `run` AS `run` LEFT JOIN `station` AS `station` ON `run`.`station` = `station`.`id` " \
          "LEFT JOIN `variable` AS `variable` ON `run`.`variable` = `variable`.`id` " \
          "LEFT JOIN `source` AS `source` ON `run`.`source` = `source`.`id`"
    df_curw = pd.read_sql_query(sql=curw_sql, con=curw_db_conn)
    df_curw.drop_duplicates(subset=['station_name', 'parameter', 'source'], keep='first', inplace=True)
    # correcting lat lon
    df_curw['lat'], df_curw['lon'] = np.where(
        df_curw['lat'] > df_curw['lon'], [df_curw['lon'], df_curw['lat']], [df_curw['lat'], df_curw['lon']])

    new_curw_sql = "SELECT `station_name`, `parameter`, `source`, `lat`, `lon` FROM `station_descriptor`"
    df_curw_new = pd.read_sql_query(sql=new_curw_sql, con=new_curw_db_conn)

    additions = pd.concat([df_curw, df_curw_new])
    additions.drop_duplicates(subset=['station_name', 'parameter', 'source'], keep=False, inplace=True)

    return additions


def update_station_descriptors(additions):
    """
    :param additions: data-frame, columns=['station_name', 'parameter', 'source', lat, lon]
    :return: void
    """
    # obtaining station_descriptor sd_id
    sources = ['WeatherStation', 'WaterLevelGuage', 'HEC-HMS', 'SHER', 'FLO2D', 'EPM', 'MIKE11', 'WRF', 'EPM', 'ARCGIS']
    source_ranges = {
        'WeatherStation': [100000, 499999],
        'WaterLevelGuage': [500000, 599999],
        'HEC-HMS': [600000, 699999],
        'SHER': [700000, 799999],
        'FLO2D': [800000, 899999],
        'EPM': [900000, 999999],
        'MIKE11': [1000000, 1099999],
        'WRF': [1100000, 1499999],
        'EPM': [1500000, 1599999],
        'ARCGIS': [1600000, 1699999],
        'Other': [10000000]
    }

    source_encounters = {
        'WeatherStation': 0,
        'WaterLevelGuage': 0,
        'HEC-HMS': 0,
        'SHER': 0,
        'FLO2D': 0,
        'EPM': 0,
        'MIKE11': 0,
        'WRF': 0,
        'EPM': 0,
        'ARCGIS': 0,
        'Other': 0
    }

    if additions.empty:
        print("No station descriptor additions")
        return

    sql = "SELECT `sd_id` FROM `station_descriptor` WHERE `sd_id` >= '%d' ORDER BY `sd_id` DESC limit 1"
    additions['sd_id'] = 0
    for index, row in additions.iterrows():
        source_type = None
        for src in sources:
            if src.lower() in row['source'].lower():
                source_type = src
                continue
        if source_type is None:
            source_type = 'Other'

        range_start = source_ranges[source_type][0]
        sd_id_df = pd.read_sql_query(sql=(sql % range_start), con=new_curw_db_conn)
        possible_sd_id = (int(sd_id_df['sd_id'][0]) + 1) if not sd_id_df.empty else range_start
        possible_sd_id = possible_sd_id + source_encounters[source_type]

        additions.set_value(index, 'sd_id', possible_sd_id)

        source_encounters[source_type] = source_encounters[source_type] + 1

    additions.to_sql(name='station_descriptor', con=new_curw_db_conn, if_exists='append', index=False)
    print("Added %d station_descriptors." % len(additions.index))


def export_station_descriptor_data():
    sql = "SELECT `sd_id`, `station_name`, `parameter`, `source`, `lat`, `lon` FROM `station_descriptor`"
    df_station_descriptor = pd.read_sql_query(sql=sql, con=new_curw_db_conn, index_col='sd_id')
    df_station_descriptor.to_csv('station_descriptor.csv', header=True, index=True)
    print("Exported station_descriptor of size: %d rows" % len(df_station_descriptor.index))

def import_data_from_out_csv():
    """
    make sure the column names of the out.csv should be as follows,
    sd_id
    station_id
    station_name
    parameter
    source
    lat
    lon
    """
    df = pd.read_csv('out.csv', index_col='sd_id')
    df.drop('station_id', axis=1, inplace=True)
    df.to_sql(name='station_descriptor', con=new_curw_db_conn, if_exists='append', index=True)


print("\n######Updating Sources######")
update_source()
print("\n######Updating Types######")
update_type()
print("\n######Updating Units######")
update_unit()
print("\n######Updating Parameters######")
update_parameter()
print("\n######Updating station_descriptor table######")
update_station_descriptors(get_station_descriptor_additions())
print("\n######Export station descriptor to csv######")
export_station_descriptor_data()
