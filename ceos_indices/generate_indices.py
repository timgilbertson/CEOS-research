import logging, coloredlogs
from typing import Dict

from dask.distributed import Client, LocalCluster, progress
from google.cloud import storage
import geopandas as gpd
import pandas as pd

from .io.inbound import read_images, read_tif, load_sensor_locations
from .indices.calculate_indices import calculate_indices
from .validation.run_validation import run_validation

logger = logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)


def indices(params: Dict[str, str]):
    storage_client = storage.Client()

    logger.info("Loading Sensor Locations")
    sensors = load_sensor_locations(params["sensor_locations"])

    logger.info("Calculating Indices")
    if params["distributed"]:
        index_frame, sensor_indices = calculate_indices_distributed(storage_client, sensors)
    else:
        images, dates, sensor_values = read_images(storage_client, sensors)
        indexes = []
        for date in zip((images, dates, sensor_values)):
            # TODO: FIX THIS TUPLE RETURN
            indexes.append(calculate_indices(date))
        index_frame = pd.concat(indexes)

    logger.info("Validating Indices")
    run_validation(index_frame, sensor_indices, params["output_path"])

    logger.info("Writing Results")


def initiate_dask_client(n_workers: int = 14, memory_limit: int = 32) -> Client:
    if n_workers == 1:
        return Client(n_workers=1, threads_per_worker=1, memory_limit=f"{memory_limit}GB", processes=False)

    return Client(
        LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=f"{memory_limit}GB", processes=True)
    )


def calculate_indices_distributed(storage_client: storage.Client, sensors: gpd.GeoDataFrame) -> pd.DataFrame:
    dask_client = initiate_dask_client(n_workers=16, memory_limit=64)
    blobs = storage_client.list_blobs("ceos_planet", prefix="UTM-24000/16N/27E-49N/PF-SR")

    groupings = ["gs://ceos_planet/" + blob.name for blob in blobs]

    dask_futures = []
    for group in groupings:
        dask_futures.append(dask_client.submit(_indices_by_group, group, sensors))

    average_images, sensor_indices = [], []
    progress(dask_futures)
    for future in dask_futures:
        result = future.result()
        average_images.append(result[0])
        sensor_indices.append(result[1])

    return pd.concat(average_images), pd.concat(sensor_indices)
        


def _indices_by_group(blob: str, sensors: gpd.GeoDataFrame) -> pd.DataFrame:
    image, date, sensor_pixels = read_tif(blob, sensors)

    return calculate_indices(image, date, sensor_pixels)
