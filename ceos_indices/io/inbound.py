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

DOWNSCALE_FACTOR = 100 / 3.5  # Planet to VanderSat resolution


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
        blobs = client.list_blobs("ceos_planet", prefix="UTM-24000/16N/26E-49N/PF-SR")
        count = 0
        for blob in tqdm(blobs):
            images, dates, sensor_pixels = read_tif("gs://ceos_planet/" + blob.name, sensors)
            image_arrays.append(images)
            date_arrays.append(dates)
            sensor_pixels_frames.append(sensor_pixels)

            count += 1
            if count == 10:
                break

    return image_arrays, date_arrays, pd.concat(sensor_pixels_frames)


def read_tif(in_path: str, sensors: gpd.GeoDataFrame) -> Tuple[np.ndarray, gpd.GeoDataFrame, pd.DataFrame]:
    path_prefix = "gs://ceos_planet/UTM-24000/16N/"
    path_mids = ["26E-50N/", "27E-49N/", "27E-50N/"]
    date = in_path.split("/")[-1].split(".tif")[0][-10:]
    paths = [in_path] + [path_prefix + mid + "PF-SR/" + date + ".tif" for mid in path_mids]

    sensor_pixels = []
    for count, path in enumerate(paths):
        with rs.open(path) as tif_file:
            pixel_values, sensors = _mask_sensor_data(sensors, tif_file)
            sensor_pixels.append(pixel_values)
            image = tif_file.read(
                out_shape=(
                    tif_file.count,
                    int(tif_file.height / DOWNSCALE_FACTOR),
                    int(tif_file.width / DOWNSCALE_FACTOR),
                ),
                resampling=Resampling.bilinear,
            )
        if count == 0:
            image_sw = image
        elif count == 1:  # join SW and NW images
            images_w = np.concatenate([image, image_sw], axis=count)
        elif count == 2:
            image_se = image
        else:  # join all images
            images_e = np.concatenate([image_se, image], axis=1)
            images = np.concatenate([images_w, images_e], axis=2)

    return images, date, _assign_date(sensor_pixels, date)


def _assign_date(sensor_pixels: list, date: str) -> pd.DataFrame:
    return pd.concat(sensor_pixels).assign(date=pd.to_datetime(date))


def load_sensor_locations(sensor_path: str) -> gpd.GeoDataFrame:
    sensors = gpd.read_file(sensor_path).to_crs("EPSG:32616")
    return sensors


def _mask_sensor_data(sensors: gpd.GeoDataFrame, image: np.ndarray) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if len(sensors) == 0:
        return gpd.GeoDataFrame(), gpd.GeoDataFrame()

    band_values = []
    for _, sensor in sensors.iterrows():
        out, _ = mask(image, [sensor.geometry.buffer(100)], invert=False, all_touched=True)
        band_values.append(out.mean(axis=(1,2), where=out>0))

    band_array = np.asarray(band_values)
    if band_array.shape[1] == 4:
        band_cols = pd.DataFrame(band_array, columns=["blue", "green", "red", "infrared"])
    else:
        band_cols = pd.DataFrame(np.array([0, 0, 0, 0]).reshape(1, 4), columns=["blue", "green", "red", "infrared"])
    sensor_pixels = sensors.join(band_cols)

    return sensor_pixels[sensor_pixels.sum(axis=1) > 0], sensors[sensors.sum(axis=1) <= 0]
