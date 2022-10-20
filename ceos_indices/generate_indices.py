import logging, coloredlogs
from typing import Dict

from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster
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
        index_frame = calculate_indices_distributed(storage_client, sensors)
    else:
        index_frame = read_images(storage_client, sensors)

    logger.info("Validating Indices")
    run_validation(index_frame)

    logger.info("Writing Results")


def initiate_dask_client(n_workers: int = 14, memory_limit: int = 32) -> Client:
    if n_workers == 1:
        return Client(n_workers=1, threads_per_worker=1, memory_limit=f"{memory_limit}GB", processes=False)

    return Client(
        LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=f"{memory_limit}GB", processes=True)
    )


def calculate_indices_distributed(storage_client: storage.Client, sensors: gpd.GeoDataFrame) -> pd.DataFrame:
    dask_client = initiate_dask_client()
    blobs = storage_client.list_blobs("ceos_planet", prefix="UTM-24000/16N/26E-49N/PF-SR")

    groupings = ["gs://ceos_planet/" + blob.name for blob in blobs]

    dask_futures = []
    for group in groupings[:1]:
        dask_futures.append(dask_client.submit(_indices_by_group, group, sensors))

    with ProgressBar():
        return pd.concat([fut.result() for fut in dask_futures], sort=False)


def _indices_by_group(blob: str, sensors: gpd.GeoDataFrame) -> pd.DataFrame:
    image, date = read_tif(blob)

    return calculate_indices(image, date)
