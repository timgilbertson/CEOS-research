import os

from google.cloud import storage
import numpy as np
import rasterio as rs
from tqdm import tqdm

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
        for blob in tqdm(blobs, total=len(blobs)):
            images, dates = read_tif('gs://ceos_planet/' + blob.name)
            image_arrays.append(np.mean(images, axis=(1, 2)))
            date_arrays.append(dates)

            count += 1
            if count == 10:
                break

    return image_arrays, date_arrays


def read_tif(path):
    date = path.split("/")[-1].split(".tif")[0][-10:]
    with rs.open(path) as tif_file:
        images = tif_file.read()

    return images, date