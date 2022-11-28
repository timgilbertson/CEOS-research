import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd


def plot_indices(index_dict: pd.DataFrame, sensor_indices: gpd.GeoDataFrame, plot_path: str):
    """Plot calculated vegetation indices.

    Args:
        index_dict (pd.DataFrame): _description_
        plot_path (str): _description_
    """
    _plot_time_arrays(index_dict, plot_path)
    _plot_sensor_indices(sensor_indices, plot_path)


def _plot_time_arrays(index_dict: pd.DataFrame, plot_path: str):
    """Generate indices plot for full images"""
    fig, ax1 = plt.subplots(figsize=(15, 10))
    ax2 = ax1.twinx()

    ax1.plot(index_dict["mean_ndvi"], color="blue", label="Mean NDVI")
    ax1.plot(index_dict["low_ndvi"], color="blue", linestyle="dashed", label="-SD NDVI")
    ax1.plot(index_dict["high_ndvi"], color="blue", linestyle="dashdot", label="+SD NDVI")
    ax1.set_ylabel("Normalized Difference Vegetative Index")
    ax1.set_xlabel("Date")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    ax2.plot(index_dict["mean_nirv"], color="orange", label="Mean NIRv")
    ax2.plot(index_dict["low_nirv"], color="orange", linestyle="dashed", label="-SD NIRv")
    ax2.plot(index_dict["high_nirv"], color="orange", linestyle="dashdot", label="+SD NIRv")
    ax2.set_ylabel("Near Infrared Radiation Reflected from Vegetation")

    fig.autofmt_xdate(rotation=45)
    fig.legend()
    plt.savefig(plot_path + "avg_image_indices.png")


def _plot_sensor_indices(sensor_indices: gpd.GeoDataFrame, plot_path: str):
    """generate indices plot for individual sensor locations"""
    fig, ax1 = plt.subplots(figsize=(15, 10))

    for sensor in sensor_indices["name"].unique():
        sensor_group = sensor_indices[sensor_indices["name"] == sensor]
        ax1.plot(sensor_group["ndvi_window"], label=sensor)
        ax1.set_ylabel("7 Day Moving Average Normalized Difference Vegetative Index")
        ax1.set_xlabel("date")
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    fig.autofmt_xdate(rotation=45)
    fig.legend()
    plt.savefig(plot_path + "sensor_indices.png")
