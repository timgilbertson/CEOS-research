from typing import Dict, List

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd


def plot_indices(index_dict: pd.DataFrame, sensor_indices: gpd.GeoDataFrame, plot_path: str):
    _plot_time_arrays(index_dict, plot_path)
    _plot_sensor_indices(sensor_indices, plot_path)


def _plot_time_arrays(index_dict: pd.DataFrame, plot_path: str):
    """Plot calculated vegetation indices.

    Args:
        index_dict (pd.DataFrame): _description_
        plot_path (str): _description_
    """
    fig, ax1 = plt.subplots(figsize=(15, 10))
    ax2 = ax1.twinx()

    ax1.plot(index_dict["mean_ndvi"], label="NDVI", color="blue")
    ax1.set_ylabel("Normalized Difference Vegetative Index")
    ax1.set_xlabel("Date")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    ax2.plot(index_dict["mean_nirv"], label="NIRv", color="orange")
    ax2.set_ylabel("Near Infrared Radiation Reflected from Vegetation")

    fig.autofmt_xdate(rotation=45)
    fig.legend()
    plt.savefig(plot_path + "avg_image_indices.png")


def _plot_sensor_indices(sensor_indices: gpd.GeoDataFrame, plot_path: str):
    fig, ax1 = plt.subplots(figsize=(15, 10))

    for sensor in sensor_indices["name"].unique():
        sensor_group = sensor_indices[sensor_indices["name"] == sensor]
        ax1.plot(sensor_group["mean_ndvi"], label=sensor)
        ax1.set_ylabel("Normalized Difference Vegetative Index")
        ax1.set_xlabel("date")
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    fig.autofmt_xdate(rotation=45)
    fig.legend()
    plt.savefig(plot_path + "sensor_indices.png")
