from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd


def calculate_indices(
    images: np.ndarray, dates: np.ndarray, sensor_values: gpd.GeoDataFrame
) -> Tuple[pd.DataFrame, gpd.GeoDataFrame]:
    """Calculate vegetative indices from raw pixel values.

    Args:
        images (np.ndarray): Planet Labs images
        dates (np.ndarray): Image dates
        sensor_values (gpd.GeoDataFrame): Pixel values at each sensor location by date

    Returns:
        pd.DataFrame: Calculated indices on entire images
        gpd.GeoDataFrame: Indices for each sensor location by date
    """
    indices = []
    for date in zip(images, dates):
        image_indices = _calculate_indices(date[0], date[1])
        indices.append(image_indices)

    sensor_indices = _assign_sensor_indices(sensor_values)
    moving_windowed_indices = sensor_indices.groupby("name", group_keys=False).apply(_calculate_moving_average)

    return pd.concat(indices), moving_windowed_indices


def _calculate_indices(images: List[np.ndarray], dates: List[str]) -> pd.DataFrame:
    """Calculates various vegetation indices from PF bands.

    Args:
        images (List[np.ndarray]): All images
        dates (List[str]): Corresponding image acquisition dates

    Returns:
        pd.DataFrame: NDVI, NIRv indices by image mean
    """
    ndvi = _generate_ndvi(images)
    nirv = _generate_nirv(images, ndvi)

    mean_ndvi = np.mean(ndvi)
    high_ndvi, low_ndvi = _calculate_quantiles(ndvi)

    mean_nirv = np.mean(nirv)
    high_nirv, low_nirv = _calculate_quantiles(nirv)

    return pd.DataFrame(
        {
            "mean_ndvi": mean_ndvi,
            "high_ndvi": high_ndvi,
            "low_ndvi": low_ndvi,
            "mean_nirv": mean_nirv,
            "high_nirv": high_nirv,
            "low_nirv": low_nirv,
        },
        index=[pd.to_datetime(dates)],
    )


def _calculate_moving_average(sensor_values: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    """Moving average of NDVI and NIRv by given window"""
    return sensor_values.assign(
        ndvi_window=sensor_values["mean_ndvi"].rolling(window=window, min_periods=1).mean(),
        nirv_window=sensor_values["mean_nirv"].rolling(window=window, min_periods=1).mean(),
    )


def _assign_sensor_indices(sensor_values: pd.DataFrame) -> pd.DataFrame:
    """Assign indices to sensor locations by date"""
    ndvi = (sensor_values["infrared"] - sensor_values["red"]) / (sensor_values["infrared"] + sensor_values["red"])
    return sensor_values.assign(mean_ndvi=ndvi, mean_nirv=sensor_values["infrared"] * ndvi).set_index("date")


def _generate_ndvi(images: List[np.ndarray]) -> List[np.ndarray]:
    """Calculates NDVI.

    NDVI: Normalized Difference Vegetative Index
        The ratio of the difference between near infrared and red reflectance to the sum of the near infrared and red
        reflectances."""
    band_difference = images[3] - images[2]
    band_sum = images[3] + images[2]
    return np.divide(band_difference, band_sum, out=np.zeros_like(band_difference, dtype=float), where=band_sum != 0)


def _generate_nirv(images: List[np.ndarray], ndvi: List[np.ndarray]) -> List[np.ndarray]:
    """Calculates NIRv

    NIRv: The product of the NDVI and near infrared reflectances"""
    return images[3] * ndvi


def _calculate_quantiles(array: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Calculate high and low standard deviation"""
    high = np.mean(array) + np.std(array)
    low = np.mean(array) - np.std(array)

    return high, low
