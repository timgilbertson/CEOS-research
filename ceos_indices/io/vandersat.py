import os
import glob

import geopandas as gpd
import pandas as pd
from vds_api_client import VdsApiV2

OUTPUT_FOLDER = "data/inputs/vandersat"


def read_vandersat_data(sensors: gpd.GeoDataFrame) -> pd.DataFrame:
    """Load time series soil moisture data from the VanderSat api.

    If there are no UUIDs stored in the /uuids folder it will send an api request.
    Note that this will need to be scheduled because the VDS turnaround on requests is often several hours.

    Args:
        sensors (gpd.GeoDataFrame): Sensor location data

    Returns:
        pd.DataFrame: Soil moisture for each sensor location by date
    """
    client = VdsApiV2()
    client.set_outfold(OUTPUT_FOLDER)
    client = _generate_requests(client, sensors)
    client.download_async_files()
    return _reimport_vandersat(sensors)


def _reimport_vandersat(sensors: gpd.GeoDataFrame):
    """Import the csv files written from VDS"""
    vandersat_files = []
    for filename in glob.glob(os.path.join(OUTPUT_FOLDER, "*.csv")):
        vandersat_files.append(
            pd.read_csv(filename, index_col=0, parse_dates=True, dayfirst=True, comment="#").pipe(
                _calculate_moving_average
            )
        )

    return pd.concat(vandersat_files, axis=1).set_axis(sensors["name"].to_list(), axis=1)


def _generate_requests(client: VdsApiV2, sensors: gpd.GeoDataFrame):
    """Check for existing uuids or generate new ones"""
    uuid_paths = glob.glob(os.path.join(OUTPUT_FOLDER + "/uuids", "*.uuid"))
    uuids = [uuid.split("/")[-1].split(".")[0] for uuid in uuid_paths]

    if len(uuids) < 1:
        client.gen_time_series_requests(
            products=["SM.ER-SMAP-L-DESC_V1.0_100", "TEFF.ER-AMSR2-DESC_V1.0_100"],
            start_time="2018-01-01",
            end_time="2022-01-01",
            lons=[sensor.coords[0][0] for sensor in sensors.geometry.to_crs("epsg:4326")],
            lats=[sensor.coords[0][1] for sensor in sensors.geometry.to_crs("epsg:4326")],
        )
        client.submit_async_requests()
    else:
        client.uuids = uuids

    return client


def _calculate_moving_average(vandersat: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    """Moving average of VanderSat by given window"""
    return vandersat.rolling(window=window, min_periods=1).mean()
