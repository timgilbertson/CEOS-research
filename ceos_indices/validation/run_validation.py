import pdb
from typing import Dict, List

import numpy as np

from ..plotting.plots import plot_time_arrays


def run_validation(indices: Dict[str, List[np.ndarray]]):
    plot_time_arrays(indices, "data/time_plot.png")
