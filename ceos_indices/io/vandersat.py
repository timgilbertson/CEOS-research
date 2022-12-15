import os
import time

import json
import requests
import logging

import geopandas as gpd
import numpy as np
import pandas as pd

BASE_URL = "https://maps.vandersat.com/api/v2/"
OUTPUT_FOLDER = 'data/tiff'
API_NAME = 'SM.ER-SMAP-L-DESC_V1.0_100'
AUTH = (os.environ['VDS_USER'], os.environ['VDS_PASS'])
LON_MAX = -85.58254436451233
LON_MIN = -85.62829366453546
LAT_MAX = 10.87960299074829
LAT_MIN = 10.829475065842194
HOR_RESOLUTION = 0.0010892690481698185
VERT_RESOLUTION = 0.0009114168164744802

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _get_content(uri, auth):
    r = requests.get(uri, auth=auth)
    r.raise_for_status()
    response = json.loads(r.content)
    return response


def submit_job(api_call, auth):
    response = _get_content(api_call, auth)
    uuid = response['uuid']
    return uuid


def _get_status(uuid, auth):
    status_uri = ('https://maps.vandersat.com/api/v2/'
                 'api-requests/{uuid}/status'.format(uuid=uuid))
    response = _get_content(status_uri, auth)
    return response


def get_data(uuid, auth):
    status = _get_status(uuid, auth=auth)
    while status['percentage'] < 100:
        print(f"Percentage = {status['percentage']}")
        time.sleep(5)  # wait 5 seconds before requesting new status
        status = _get_status(uuid, auth=auth)
    print('Finished')
    data = status['data']
    import pdb; pdb.set_trace()
    return data


def download_files(files, auth, out_folder=''):
    for fn_link in files:
        file_path = os.path.join(out_folder, os.path.split(fn_link)[1])
        file_uri = (f'https://maps.vandersat.com/{fn_link}/download')
        r = requests.get(file_uri, verify=True, stream=True, auth=auth)
        r.raise_for_status()
        array = np.frombuffer(r.content, dtype=np.uint8)
        reshaped_array = reshape_array(array)
        import pdb; pdb.set_trace()


def reshape_array(array: np.ndarray):
    width = int((LON_MAX - LON_MIN) / HOR_RESOLUTION)
    height = int((LAT_MAX - LAT_MIN) / VERT_RESOLUTION)
    return array.reshape(width, height)


def _read_vandersat_data(lat: float, lon: float) -> pd.DataFrame:
    auth = AUTH
    api_call = f"https://maps.vandersat.com/api/v2/products/{API_NAME}/point-time-series?start_time=2018-01-01&end_time=2021-12-31&lat={lat}&lon={lon}&format=csv&avg_window_days=0&include_masked_data=false&climatology=false"
    
    import pdb; pdb.set_trace()
    uuid = submit_job(api_call, auth=auth)
    files = get_data(uuid, auth=auth) # waits for processing to finish
    download_files(files, auth=auth, out_folder=out_folder)


def read_vandersat_data(sensors: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    vandersat = []
    for sensor in sensors.geometry.to_crs("epsg:4326"):
        vandersat.append(_read_vandersat_data(sensor.coords[0][1], sensor.coords[0][0]))
