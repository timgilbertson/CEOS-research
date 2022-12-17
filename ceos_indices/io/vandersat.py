import os
import glob
import logging

import geopandas as gpd
import pandas as pd
from vds_api_client import VdsApiV2

OUTPUT_FOLDER = 'data/inputs/vandersat'
API_NAME = 'SM.ER-SMAP-L-DESC_V1.0_100'


def read_vandersat_data(sensors: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    client = VdsApiV2()
    client.set_outfold(OUTPUT_FOLDER)
    client = _generate_requests(client, sensors)
    client.download_async_files()
    return _reimport_vandersat(sensors)


def _reimport_vandersat(sensors: gpd.GeoDataFrame):
    vandersat_files = []
    for filename in glob.glob(os.path.join(OUTPUT_FOLDER, '*.csv')):
        vandersat_files.append(pd.read_csv(filename, index_col=0, parse_dates=True, dayfirst=True, comment='#'))

    return pd.concat(vandersat_files, axis=1).set_axis(sensors["name"].to_list(), axis=1)


def _generate_requests(client: VdsApiV2, sensors: gpd. GeoDataFrame):
    uuid_paths = glob.glob(os.path.join(OUTPUT_FOLDER + "/uuids", '*.uuid'))
    uuids = [uuid.split("/")[-1].split(".")[0] for uuid in uuid_paths]

    if len(uuids) < 1:
        client.gen_time_series_requests(
            products=[API_NAME],
            start_time='2018-01-01',
            end_time='2022-01-01',
            lons=[sensor.coords[0][0] for sensor in sensors.geometry.to_crs("epsg:4326")],
            lats=[sensor.coords[0][1] for sensor in sensors.geometry.to_crs("epsg:4326")]
        )
        client.submit_async_requests()
    else:
        client.uuids = uuids

    return client