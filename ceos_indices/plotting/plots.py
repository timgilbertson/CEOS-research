import pdb
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np


def plot_time_arrays(index_dict: Dict[str, List[np.ndarray]], plot_path: str):
    """Plot calculated vegetation indices.

    Args:
        index_dict (Dict[str, List[np.ndarray]]): _description_
        plot_path (str): _description_
    """

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(index_dict["mean_ndvi"], label="NDVI", color="blue")
    ax1.plot(index_dict["high_ndvi"], label="NDVI (+2 sd)", color="blue", linestyle="dashed")
    ax1.plot(index_dict["low_ndvi"], label="NDVI (-2 sd)", color="blue", linestyle="dashed")
    ax1.set_ylabel("Normalized Difference Vegetative Index")

    ax2.plot(index_dict["mean_nirv"], label="NIRv", color="orange")
    ax2.plot(index_dict["high_nirv"], label="NIRv (+2 sd)", color="orange", linestyle="dashed")
    ax2.plot(index_dict["low_nirv"], label="NIRv (-2 sd)", color="orange", linestyle="dashed")
    ax2.set_ylabel("Near Infrared Radiation Reflected from Vegetation")

    fig.legend()
    plt.savefig(plot_path)
