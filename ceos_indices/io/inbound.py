import os
from typing import List, Tuple

from dask.distributed import Client, LocalCluster, progress
from google.cloud import storage
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rs
from rasterio.mask import mask
from rasterio.enums import Resampling
from tqdm import tqdm

DOWNSCALE_FACTOR = 100 / 3  # Planet to VanderSat resolution TODO: expose this arg or calculate it on the fly.


def read_images(
    image_path: str, sensors: gpd.GeoDataFrame, local: bool = False
) -> Tuple[np.ndarray, List[str], gpd.GeoDataFrame]:
    """Read images in serial.

    Args:
        image_path (str): Bucket path to image
        sensors (gpd.GeoDataFrame): Sensor data
        local (bool, optional): Local image flag. Defaults to False.

    Returns:
        np.ndarray: Images
        List[str]: Dates
        gpd.GeoDataFrame: Sensors with pixel values by date
    """
    image_arrays, date_arrays, sensor_pixels_frames = [], [], []
    if local:
        for image in os.listdir(image_path):
            images, dates, sensor_pixels = read_tif(image_path + image)
            image_arrays.append(images)
            date_arrays.append(dates)
            sensor_pixels_frames.append(sensor_pixels)
    else:
        client = storage.Client()
        blobs = client.list_blobs("ceos_planet", prefix="UTM-24000/16N/27E-49N/PF-SR")
        for blob in tqdm(blobs):
            images, dates, sensor_pixels = read_tif("gs://ceos_planet/" + blob.name, sensors)
            image_arrays.append(images)
            date_arrays.append(dates)
            sensor_pixels_frames.append(sensor_pixels)

    return image_arrays, date_arrays, pd.concat(sensor_pixels_frames)


def read_tif(in_path: str, sensors: gpd.GeoDataFrame) -> Tuple[np.ndarray, List[str], gpd.GeoDataFrame]:
    """Read the images from a bucket location

    Args:
        in_path (str): Bucket path
        sensors (gpd.GeoDataFrame): Sensor locations

    Returns:
        np.ndarray: Images
        List[str]: Dates
        gpd.GeoDataFrame: Sensors with pixel values by date
    """
    path_prefix = "gs://ceos_planet/UTM-24000/16N/"
    path_mids = ["27E-50N/"]
    date = in_path.split("/")[-1].split(".tif")[0][-10:]
    paths = [in_path] + [path_prefix + mid + "PF-SR/" + date + ".tif" for mid in path_mids]

    sensor_pixels = []
    for count, path in enumerate(paths):
        with rs.open(path) as tif_file:
            image = tif_file.read(
                out_shape=(
                    tif_file.count,
                    round(tif_file.height / DOWNSCALE_FACTOR),
                    round(tif_file.width / DOWNSCALE_FACTOR),
                ),
                resampling=Resampling.bilinear,
            )

        horizontal_coords, vertical_coords = _generate_image_coords(tif_file, image)
        pixel_values, sensors = _mask_sensor_data(horizontal_coords, vertical_coords, sensors, image)

        if count == 0:
            image_top = image
        else:  # join all images
            images = np.concatenate([image_top, image], axis=1)

        sensor_pixels.append(pixel_values)

    return images, date, _assign_date(sensor_pixels, date)


def _assign_date(sensor_pixels: list, date: str) -> pd.DataFrame:
    """Cast date column to datetime objects"""
    return pd.concat(sensor_pixels).assign(date=pd.to_datetime(date))


def load_sensor_locations(sensor_path: str) -> gpd.GeoDataFrame:
    """Load the sensor data CSV.

    Args:
        sensor_path (str): Input path to sensor data

    Returns:
        gpd.GeoDataFrame: Georeferenced sensor locations
    """
    sensors = gpd.read_file(sensor_path).to_crs("EPSG:32616")
    return sensors


def _mask_sensor_data(
    x_coords: np.ndarray, y_coords: np.ndarray, sensors: gpd.GeoDataFrame, image: np.ndarray
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Mask pixels to sensor locations"""
    if len(sensors) == 0:
        return gpd.GeoDataFrame(), gpd.GeoDataFrame()

    resolution = round(((x_coords[1] - x_coords[0]) + (y_coords[1] - y_coords[0])) / 2)

    band_values = []
    for _, sensor in sensors.iterrows():
        pixel_value = _find_pixel_value(
            x_coords, y_coords, image, sensor.geometry.coords[0][0], sensor.geometry.coords[0][1], resolution
        )
        band_values.append(pixel_value)

    band_array = np.asarray(band_values)
    if band_array.shape[1] == 4:
        band_cols = pd.DataFrame(band_array, columns=["blue", "green", "red", "infrared"])
    else:
        band_cols = pd.DataFrame(np.array([0, 0, 0, 0]).reshape(1, 4), columns=["blue", "green", "red", "infrared"])
    sensor_pixels = sensors.join(band_cols)

    return (
        sensor_pixels[sensor_pixels[["blue", "green", "red", "infrared"]].sum(axis=1) > 0],
        sensors[sensor_pixels[["blue", "green", "red", "infrared"]].sum(axis=1) <= 0],
    )


def _find_pixel_value(
    x_coords: np.ndarray,
    y_coords: np.ndarray,
    pixel_array: np.ndarray,
    x_value: float,
    y_value: float,
    resolution: float,
) -> int:
    """Find the closest pixel value to a location within a given resolution"""
    x_diff = np.abs(x_coords - x_value)
    y_diff = np.abs(y_coords - y_value)

    if (np.min(x_diff) > resolution) or (np.min(y_diff) > resolution):
        return np.array([0, 0, 0, 0])

    x_idx = (x_diff).argmin()
    y_idx = (y_diff).argmin()
    return pixel_array[:, x_idx, y_idx]


def _generate_image_coords(tif_file: rs.open, image: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Make arrays of image pixel locations"""
    horizontal_res = (tif_file.bounds[2] - tif_file.bounds[0]) / image.shape[1]
    vertical_res = (tif_file.bounds[3] - tif_file.bounds[1]) / image.shape[2]

    horizontal_axis = np.arange(tif_file.bounds[0], tif_file.bounds[2], horizontal_res)
    vertical_axis = np.arange(tif_file.bounds[1], tif_file.bounds[3], vertical_res)
    return horizontal_axis, vertical_axis


def initiate_dask_client(n_workers: int = 14, memory_limit: int = 32) -> Client:
    """Start a dask client"""
    if n_workers == 1:
        return Client(n_workers=1, threads_per_worker=1, memory_limit=f"{memory_limit}GB", processes=False)

    return Client(
        LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=f"{memory_limit}GB", processes=True)
    )


def read_images_distributed(
    storage_client: storage.Client, sensors: gpd.GeoDataFrame
) -> Tuple[np.ndarray, List[str], gpd.GeoDataFrame]:
    """Read Planet Labs images from a Google Bucket distributed.

    Args:
        storage_client (storage.Client): Google storage client
        sensors (gpd.GeoDataFrame): Sensor locations

    Returns:
        np.ndarray: Planet Labs images
        List[str]: Image dates
        gpd.GeoDataFrame: Sensor data with average pixel values by date
    """
    dask_client = initiate_dask_client(n_workers=1, memory_limit=64)
    blobs = storage_client.list_blobs("ceos_planet", prefix="UTM-24000/16N/27E-49N/PF-SR")

    groupings = ["gs://ceos_planet/" + blob.name for blob in blobs]

    dask_futures = []
    for group in groupings[:5]:
        dask_futures.append(dask_client.submit(read_tif, group, sensors))

    image_arrays, dates, sensor_pixels = [], [], []
    progress(dask_futures)
    for future in dask_futures:
        result = future.result()
        image_arrays.append(result[0])
        dates.append(result[1])
        sensor_pixels.append(result[2])

    return np.asarray(image_arrays), dates, pd.concat(sensor_pixels)
