from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np


def plot_time_arrays(index_dict: Dict[str, List[np.ndarray]], plot_path: str):
    """Plot calculated vegetation indices.

    Args:
        index_dict (Dict[str, List[np.ndarray]]): _description_
        plot_path (str): _description_
    """
    mean_ndvi = np.mean(index_dict["NDVI"], axis=(1, 2))
    mean_nirv = np.mean(index_dict["NIRv"], axis=(1, 2))

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(mean_ndvi, label="NDVI")
    ax1.set_ylabel("Normalized Difference Vegetative Index")

    ax2.plot(mean_nirv, label="NIRv", color="orange")
    ax2.set_ylabel("Near Infrared Radiation Reflected from Vegetation")
    fig.legend()
    fig.save(plot_path)
