from datetime import datetime
import pandas as pd
import numpy as np
import sqlalchemy as sqla
from sqlalchemy import create_engine, update
from sqlalchemy.sql import case
from sqlalchemy.orm import sessionmaker
import re
import os
import pyodbc 
import config
import hashlib
import sys
import time
import geopandas as gpd
import matplotlib.pyplot as plt
import pyproj
from shapely.geometry import Point, LineString
from zipfile import ZipFile, Path
from matplotlib.colors import TwoSlopeNorm
import logging

trips_date_str = "2022-11-17"

trips_dates_str = [
"2022-11-18",
"2022-11-17",
"2022-11-16",
"2022-11-15",
"2022-11-14",
"2022-11-13",
"2022-11-12",
"2022-11-11",
"2022-11-10",
"2022-11-9",
"2022-11-8",
"2022-11-7",
"2022-11-6",
"2022-11-5",
"2022-11-4",
"2022-11-3",
"2022-11-2",
"2022-11-1"]

#remove memory restrictions
low_memory=False

# setup logging
base_folder = config.settings['base_folder']
logs_folder_name = config.settings['logs_folder_name']
logs_file_name = datetime.now().strftime('%Y%m%d_%H%M%S') + "_trips_count_same_date_different_gtfs_" + config.settings['logs_file_name']
logs_file_path = os.path.join(base_folder, logs_folder_name, logs_file_name)

logging.basicConfig(level=logging.DEBUG,
format='%(asctime)s %(levelname)s %(message)s',
datefmt='%Y-%m-%d %H:%M:%S %z',
      filename=logs_file_path)

# Add sys.stdout as an additional output so we can remove all logging.info() statements duplications
root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)   

# database server connection details
server = config.settings['mmsql_db_server']
database = config.settings['mmsql_db_name']
username = config.settings['mmsql_username']
password = config.settings['mmsql_password'] 

engine = create_engine('mssql+pyodbc://'+username+':'+password+'@'+server+'/'+database+'?driver=ODBC+Driver+17+for+SQL+Server'
                        , fast_executemany=True, connect_args={'connect_timeout': 10}, echo=False)
# db table names
trips_tbl_name = config.settings['trips_name']
agency_tbl_name = config.settings['agency_name']
calendar_tbl_name = config.settings['calendar_name']
calendar_dates_tbl_name = config.settings['calendar_dates_name']
notes_tbl_name = config.settings['notes_name']
routes_tbl_name = config.settings['routes_name']
stops_tbl_name = config.settings['stops_name']
stop_times_tbl_name = config.settings['stop_times_name']

trips_history_tbl_name = trips_tbl_name + '_history'
agency_history_tbl_name = agency_tbl_name + '_history'
calendar_history_tbl_name = calendar_tbl_name + '_history'
calendar_dates_history_tbl_name = calendar_dates_tbl_name + '_history'
notes_history_tbl_name = notes_tbl_name + '_history'
routes_history_tbl_name = routes_tbl_name + '_history'
stops_history_tbl_name = stops_tbl_name + '_history'
stop_times_history_tbl_name = stop_times_tbl_name + '_history'


# csv file names
trips_file_name = trips_tbl_name + '.txt'
agency_file_name = agency_tbl_name + '.txt'
calendar_file_name= calendar_tbl_name + '.txt'
calendar_dates_file_name= calendar_dates_tbl_name + '.txt'
notes_file_name = notes_tbl_name + '.txt'
routes_file_name = routes_tbl_name + '.txt'
stops_file_name = stops_tbl_name + '.txt'
stop_times_file_name = stop_times_tbl_name + '.txt'

gtfs_zip_files = [
"gtfs_20220929_0046_GMT.zip",
"gtfs_20220928_1847_GMT.zip",
"gtfs_20220929_1844_GMT.zip",
"gtfs_20220930_1830_GMT.zip",
"gtfs_20221001_1729_GMT.zip",
"gtfs_20221003_0851_GMT.zip",
"gtfs_20221003_1729_GMT.zip",
"gtfs_20221005_1728_GMT.zip",
"gtfs_20221007_1729_GMT.zip",
"gtfs_20221007_2314_GMT.zip",
"gtfs_20221008_1733_GMT.zip",
"gtfs_20221009_1731_GMT.zip",
"gtfs_20221010_1734_GMT.zip",
"gtfs_20221011_1735_GMT.zip",
"gtfs_20221012_1730_GMT.zip",
"gtfs_20221013_1732_GMT.zip",
"gtfs_20221014_1734_GMT.zip",
"gtfs_20221015_1728_GMT.zip",
"gtfs_20221016_1734_GMT.zip",
"gtfs_20221017_1736_GMT.zip",
"gtfs_20221018_1735_GMT.zip",
"gtfs_20221019_1731_GMT.zip",
"gtfs_20221020_1736_GMT.zip",
"gtfs_20221021_1737_GMT.zip",
"gtfs_20221022_1735_GMT.zip",
"gtfs_20221023_1735_GMT.zip",
"gtfs_20221024_1731_GMT.zip",
"gtfs_20221025_1730_GMT.zip",
"gtfs_20221026_1733_GMT.zip",
"gtfs_20221027_0854_GMT.zip",
"gtfs_20221027_1732_GMT.zip",
"gtfs_20221028_1733_GMT.zip",
"gtfs_20221029_1732_GMT.zip",
"gtfs_20221030_1730_GMT.zip",
"gtfs_20221031_1733_GMT.zip",
"gtfs_20221101_1730_GMT.zip",
"gtfs_20221102_1734_GMT.zip",
"gtfs_20221103_0852_GMT.zip",
"gtfs_20221103_1734_GMT.zip",
"gtfs_20221104_1732_GMT.zip",
"gtfs_20221105_1733_GMT.zip",
"gtfs_20221106_1735_GMT.zip",
"gtfs_20221107_1737_GMT.zip",
"gtfs_20221108_1735_GMT.zip",
"gtfs_20221109_1731_GMT.zip",
"gtfs_20221110_0853_GMT.zip",
"gtfs_20221110_1734_GMT.zip",
"gtfs_20221111_1735_GMT.zip",
"gtfs_20221112_1729_GMT.zip",
"gtfs_20221113_1730_GMT.zip",
"gtfs_20221115_1736_GMT.zip",
"gtfs_20221116_1737_GMT.zip",
"gtfs_20221117_1737_GMT.zip"
]

test = [

]

# csv folder
base_folder = config.settings['base_folder']
downloads_folder = config.settings['downloads_folder']
full_cycle_start_time = time.time()

column_labels = [    
    "gtfs_version",
    "target_date",
    "scheduled_services",
    "services_added",
    "services_deleted",
    "final_services",
    "all_trips",
    "bus_trips"
    ]

services_trips_info_df = pd.DataFrame(columns=column_labels)
for trips_date_str in trips_dates_str:
    for gtfs_zip_file in gtfs_zip_files:
        logging.info(f'Working on {gtfs_zip_file} for date {trips_date_str}')

        start_time = time.time()  
        gtfs_zip_file_path = os.path.join(base_folder, downloads_folder, gtfs_zip_file)
        
        with ZipFile(gtfs_zip_file_path) as myzip:
            
            stops_df = pd.read_csv(myzip.open("stops.txt"), dtype={ 'stop_id': 'str', 
                'stop_code': 'str',
                'stop_name': 'str',
                'stop_lat': 'float',
                'stop_lon': 'float',
                'location_type': 'Int64',
                'parent_station': 'str',
                'wheelchair_boarding': 'str', 
                'platform_code': 'str',})
            stops_gdf = gpd.GeoDataFrame(stops_df, 
                geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat)).set_crs(epsg=4326)
            
            routes_df = pd.read_csv(myzip.open(routes_file_name), dtype={
                'route_id': 'str',  
                'agency_id': 'str',  
                'route_short_name': 'str',  
                'route_long_name': 'str', 
                'route_desc': 'str', 
                'route_type': 'Int64',
                'route_color': 'str',  
                'route_text_color': 'str', 
                'exact_times': 'bool'
            })
            
            trips_df = pd.read_csv(myzip.open(trips_file_name), dtype={
                'route_id': 'str', 
                'service_id': 'str',  
                'trip_id': 'str',
                'shape_id': 'str', 
                'trip_headsign': 'str', 
                'direction_id': 'str',  
                'block_id': 'str', 
                'wheelchair_accessible': 'str', 
                'route_direction': 'str', 
                'trip_note': 'str', 
                'bikes_allowed': 'str'
            })
            
            stop_times_df = pd.read_csv(myzip.open(stop_times_file_name), dtype={
                'trip_id': 'str',
                'arrival_time': 'str',
                'stop_id': 'str', 
                'departure_time': 'str', 
                'stop_id': 'str',
                'stop_sequence': 'Int64',
                'stop_headsign': 'str',
                'pickup_type': 'Int64',
                'drop_off_type': 'Int64',
                'shape_dist_traveled': 'float',
                'timepoint': 'bool',
                'stop_note': 'str',
            }).astype({})
            
            agency_df = pd.read_csv(myzip.open(agency_file_name), dtype={
                'agency_id': 'str', 
                'agency_name': 'str', 
                'agency_url': 'str',  
                'agency_timezone': 'str',
                'agency_lang': 'str', 
                'agency_phone': 'str',
            })
            
            calendar_df = pd.read_csv(myzip.open(calendar_file_name), dtype={
                'service_id': 'str',  
                'monday': 'bool',  
                'tuesday': 'bool',  
                'wednesday': 'bool',  
                'thursday': 'bool',  
                'friday': 'bool', 
                'saturday': 'bool',  
                'sunday': 'bool',  
                'start_date': 'str', 
                'end_date': 'str',
            })
            
            calendar_dates_df = pd.read_csv(myzip.open(calendar_dates_file_name), dtype={
                'service_id': 'str',  
                'date': 'str',
                'exception_type': 'Int64',
            })



            date = datetime.strptime(trips_date_str, "%Y-%m-%d")
            date_string = date.strftime("%Y%m%d")
            day_of_week_name = date.strftime('%A').lower()

            services_for_day_1 = calendar_df[(calendar_df[day_of_week_name]) & (date_string >= calendar_df.start_date) & (date_string <= calendar_df.end_date)].service_id.to_numpy()
            logging.info(f"Total scheduled services for {trips_date_str} as per {gtfs_zip_file} calendar: {len(services_for_day_1)}")


            # exception_type
            # 1 - Service has been added for the specified date.
            # 2 - Service has been removed for the specified date.
            services_added_for_day = calendar_dates_df[(calendar_dates_df.date == date_string) & (calendar_dates_df.exception_type == 1)].service_id.to_numpy()
            services_removed_for_day = calendar_dates_df[(calendar_dates_df.date == date_string) & (calendar_dates_df.exception_type == 2)].service_id.to_numpy()
            logging.info(f"services added using {gtfs_zip_file} calendar_dates: {len(services_added_for_day)}")
            logging.info(f"services removed using {gtfs_zip_file} calendar_dates: {len(services_removed_for_day)}")

            services_for_day_2 = np.concatenate([services_for_day_1, services_added_for_day])
            services_for_day = np.setdiff1d(services_for_day_2, services_removed_for_day)
            logging.info(f"final services for {trips_date_str} as per {gtfs_zip_file}: {len(services_for_day)}")

            all_day_trips = trips_df[trips_df.service_id.isin(services_for_day)]
            sydney_bus_route_ids = routes_df[routes_df.route_desc == "Sydney Buses Network"].route_id.unique()
            Bus_day_trips = all_day_trips[all_day_trips.route_id.isin(sydney_bus_route_ids)]
            logging.info(f"All trips for {trips_date_str} as per {gtfs_zip_file}: {len(all_day_trips)}")
            logging.info(f"Bus trips services for {trips_date_str} as per {gtfs_zip_file}: {len(Bus_day_trips)}")

            services_trips_info_df = pd.concat([services_trips_info_df,
                pd.DataFrame({
                            "gtfs_version": [gtfs_zip_file],
                            "target_date": [trips_date_str],
                            "scheduled_services": [len(services_for_day_1)],
                            "services_added": [len(services_added_for_day)],
                            "services_deleted": [len(services_removed_for_day)],
                            "final_services": [len(services_for_day)],
                            "all_trips": [len(all_day_trips)],
                            "bus_trips": [len(Bus_day_trips)] 
                            }
                        )])


            #services_trips_info_df.to_csv(f'services_trips_info_for_{trips_date_str}.csv')

            logging.info('Single loop calculated in %.2f seconds' % (time.time() - start_time))
            logging.info('--------------------------------')

            del services_for_day_1
            del services_added_for_day
            del services_removed_for_day
            del services_for_day_2
            del services_for_day
            del all_day_trips
            del sydney_bus_route_ids
            del Bus_day_trips


services_trips_info_df.to_csv(f'services_trips_info_for_{trips_date_str}.csv')
del services_trips_info_df
logging.info('Full process compeleted in %.2f seconds' % (time.time() - full_cycle_start_time))
