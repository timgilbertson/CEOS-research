from typing import List, Dict, Tuple

import numpy as np
import pandas as pd


def calculate_indices(images: List[np.ndarray], dates: List[str]) -> Dict[str, List[np.ndarray]]:
    """Calculates various vegetation indices from PF bands.

    Args:
        images (List[np.ndarray]): All images
        dates (List[str]): Corresponding image acquisition dates

    Returns:
        Dict[str, List[np.ndarray]]: NDVI, NIRv indices
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
    high = np.quantile(array, 0.95)
    low = np.quantile(array, 0.05)

    return high, low
