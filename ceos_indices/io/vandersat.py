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
    client.gen_time_series_requests(
        products=[API_NAME],
        start_time='2018-01-01',
        end_time='2022-01-01',
        lons=[sensor.coords[0][0] for sensor in sensors.geometry.to_crs("epsg:4326")],
        lats=[sensor.coords[0][1] for sensor in sensors.geometry.to_crs("epsg:4326")]
    )
    client.submit_async_requests()
    client.download_async_files()
    return _reimport_vandersat()


def _reimport_vandersat():
    vandersat_files = []
    path = OUTPUT_FOLDER
    for filename in glob.glob(os.path.join(path, '*.csv')):
        vandersat_files.append(pd.read_csv(filename))

    import pdb; pdb.set_trace()
    return pd.concat(vandersat_files)