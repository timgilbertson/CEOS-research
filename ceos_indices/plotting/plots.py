from typing import Dict, List

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
    ax1.set_ylabel("Normalized Difference Vegetative Index")
    ax1.set_xlabel("Date")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    ax2.plot(index_dict["mean_nirv"], label="NIRv", color="orange")
    ax2.set_ylabel("Near Infrared Radiation Reflected from Vegetation")

    fig.autofmt_xdate(rotation=45)
    fig.legend()
    plt.savefig(plot_path)
