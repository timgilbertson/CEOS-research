import os

from google.cloud import storage
import geopandas as gpd
import numpy as np
import rasterio as rs
from rasterio.enums import Resampling
from tqdm import tqdm

DOWNSCALE_FACTOR = 100 / 3.5  # Planet to VanderSat resolution


def read_images(image_path: str, local: bool = False) -> np.array:
    image_arrays, date_arrays = [], []
    if local:
        for image in os.listdir(image_path):
            images, dates = read_tif(image_path + image)
            image_arrays.append(images)
            date_arrays.append(dates)
    else:
        client = storage.Client()
        blobs = client.list_blobs("ceos_planet", prefix="UTM-24000/16N/26E-49N/PF-SR")
        count = 0
        for blob in tqdm(blobs):
            images, dates = read_tif("gs://ceos_planet/" + blob.name)
            image_arrays.append(np.mean(images, axis=(1, 2)))
            date_arrays.append(dates)

            count += 1
            if count == 10:
                break

    return image_arrays, date_arrays


def read_tif(in_path):
    path_prefix = "gs://ceos_planet/UTM-24000/16N/"
    path_mids = ["26E-50N/", "27E-49N/", "27E-50N/"]
    date = in_path.split("/")[-1].split(".tif")[0][-10:]
    paths = [in_path] + [path_prefix + mid + "PF-SR/" + date + ".tif" for mid in path_mids]

    for count, path in enumerate(paths):
        with rs.open(path) as tif_file:
            image = tif_file.read(
                out_shape=(
                    tif_file.count,
                    int(tif_file.height / DOWNSCALE_FACTOR),
                    int(tif_file.width / DOWNSCALE_FACTOR)
                ),
                resampling=Resampling.bilinear
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

    return images, date


def load_sensor_locations(sensor_path: str) -> gpd.GeoDataFrame:
    sensors = gpd.read_file(sensor_path)
    return sensors
