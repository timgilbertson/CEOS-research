import pdb
from typing import Dict, List

import numpy as np
import geopandas as gpd

from ..plotting.plots import plot_indices


def run_validation(indices: Dict[str, List[np.ndarray]], sensor_indices: gpd.GeoDataFrame, out_path: str):
    plot_indices(indices, sensor_indices, out_path)
