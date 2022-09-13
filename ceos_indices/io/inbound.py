import os

from google.cloud import storage
import numpy as np
import rasterio as rs

def read_images(image_path: str, local: bool = True) -> np.array:
    image_arrays, date_arrays = [], []
    if local:
        for image in os.listdir(image_path):
            images, dates = _read_tif(None, image_path + image)
            image_arrays.append(images)
            date_arrays.append(dates)
    else:
        client = storage.Client()
        bucket = client.get_bucket(image_path)
        blob = bucket.get_blob('remote/path/to/file.txt')

    return image_arrays, date_arrays


def _read_tif(fs, path):
    date = path.split("/")[-1].split(".tif")[0][-10:]
    with rs.open(path) as tif_file:
        images = tif_file.read()

    if fs:
        with fs.open(path, os.O_RDONLY) as file_obj:
            with rs.open(path) as tif_file:
                images = tif_file.read()

    return images, date