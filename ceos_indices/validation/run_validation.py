from typing import Dict, List

import geopandas as gpd
import numpy as np
import pandas as pd

from ..plotting.plots import plot_indices


def run_validation(indices: Dict[str, List[np.ndarray]], sensor_indices: gpd.GeoDataFrame, vandersat_data: pd.DataFrame, out_path: str):
    """Currently only generates plots"""
    plot_indices(indices, sensor_indices, vandersat_data, out_path)
