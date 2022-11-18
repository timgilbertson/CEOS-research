import os
from typing import Tuple

from google.cloud import storage
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rs
from rasterio.mask import mask
from rasterio.enums import Resampling
from tqdm import tqdm

DOWNSCALE_FACTOR = 100 / 3  # Planet to VanderSat resolution


def read_images(image_path: str, sensors: gpd.GeoDataFrame, local: bool = False) -> np.array:
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
        count = 0
        for blob in tqdm(blobs):
            images, dates, sensor_pixels = read_tif("gs://ceos_planet/" + blob.name, sensors)
            image_arrays.append(images)
            date_arrays.append(dates)
            sensor_pixels_frames.append(sensor_pixels)

    return image_arrays, date_arrays, pd.concat(sensor_pixels_frames)


def read_tif(in_path: str, sensors: gpd.GeoDataFrame) -> Tuple[np.ndarray, gpd.GeoDataFrame, pd.DataFrame]:
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
    return pd.concat(sensor_pixels).assign(date=pd.to_datetime(date))


def load_sensor_locations(sensor_path: str) -> gpd.GeoDataFrame:
    sensors = gpd.read_file(sensor_path).to_crs("EPSG:32616")
    return sensors


def _mask_sensor_data(x_coords: np.ndarray, y_coords: np.ndarray, sensors: gpd.GeoDataFrame, image: np.ndarray) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if len(sensors) == 0:
        return gpd.GeoDataFrame(), gpd.GeoDataFrame()

    resolution = round(((x_coords[1] - x_coords[0]) + (y_coords[1] - y_coords[0])) / 2)

    band_values = []
    for _, sensor in sensors.iterrows():
        pixel_value = _find_pixel_value(x_coords, y_coords, image, sensor.geometry.coords[0][0], sensor.geometry.coords[0][1], resolution)
        band_values.append(pixel_value)

    band_array = np.asarray(band_values)
    if band_array.shape[1] == 4:
        band_cols = pd.DataFrame(band_array, columns=["blue", "green", "red", "infrared"])
    else:
        band_cols = pd.DataFrame(np.array([0, 0, 0, 0]).reshape(1, 4), columns=["blue", "green", "red", "infrared"])
    sensor_pixels = sensors.join(band_cols)

    return sensor_pixels[sensor_pixels[["blue", "green", "red", "infrared"]].sum(axis=1) > 0], sensors[sensor_pixels[["blue", "green", "red", "infrared"]].sum(axis=1) <= 0]


def _find_pixel_value(x_coords: np.ndarray, y_coords: np.ndarray, pixel_array: np.ndarray, x_value: float, y_value: float, resolution: float) -> int:
    x_diff = np.abs(x_coords - x_value)
    y_diff = np.abs(y_coords - y_value)
    
    if (np.min(x_diff) > resolution) or (np.min(y_diff) > resolution):
        return np.array([0, 0, 0, 0])

    x_idx = (x_diff).argmin()
    y_idx = (y_diff).argmin()
    return pixel_array[:, x_idx, y_idx]


def _generate_image_coords(tif_file, image) -> Tuple[np.ndarray, np.ndarray]:
    horizontal_res = (tif_file.bounds[2] - tif_file.bounds[0]) / image.shape[1]
    vertical_res = (tif_file.bounds[3] - tif_file.bounds[1]) / image.shape[2]

    horizontal_axis = np.arange(tif_file.bounds[0], tif_file.bounds[2], horizontal_res)
    vertical_axis = np.arange(tif_file.bounds[1], tif_file.bounds[3], vertical_res)
    return horizontal_axis, vertical_axis


def old_shit():
    if len(sensors) == 0:
        return gpd.GeoDataFrame(), gpd.GeoDataFrame()

    band_values = []
    for _, sensor in sensors.iterrows():
        import pdb; pdb.set_trace()
        out, _ = mask(image, [sensor.geometry.buffer(100)], invert=False, all_touched=True)
        band_values.append(out.mean(axis=(1, 2), where=out>0))

    band_array = np.asarray(band_values)
    if band_array.shape[1] == 4:
        band_cols = pd.DataFrame(band_array, columns=["blue", "green", "red", "infrared"])
    else:
        band_cols = pd.DataFrame(np.array([0, 0, 0, 0]).reshape(1, 4), columns=["blue", "green", "red", "infrared"])
    sensor_pixels = sensors.join(band_cols)

    return sensor_pixels[sensor_pixels.sum(axis=1) > 0], sensors[sensors.sum(axis=1) <= 0]
