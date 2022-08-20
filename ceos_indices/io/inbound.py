import os

from google.cloud import storage
import numpy as np
import rasterio as rs

def read_images(image_path: str, local: bool = True) -> np.array:
    if local:
        import pdb; pdb.set_trace()
        images, dates = _read_tif(os, image_path)

    else:
        client = storage.Client()
        bucket = client.get_bucket(image_path)
        blob = bucket.get_blob('remote/path/to/file.txt')


def _read_tif(fs, path):
    date = path.split("/")[-1].split(".tif")[0]
    with fs.open(path) as file_obj:
        with rs.open(file_obj) as tif_file:
            images = tif_file.read()

    image_list = [images[idx].ravel() for idx in range(images.shape[0])]
    return image_list, date