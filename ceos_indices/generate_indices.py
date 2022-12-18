from typing import Dict

import coloredlogs
import logging

from google.cloud import storage

from .io.inbound import read_images_distributed, read_images, load_sensor_locations
from .io.vandersat import read_vandersat_data
from .io.outbound import write_outputs
from .indices.calculate_indices import calculate_indices
from .validation.run_validation import run_validation

logger = logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger)


def indices(params: Dict[str, str]):
    storage_client = storage.Client()

    logger.info("Loading Sensor Locations")
    sensors = load_sensor_locations(params["sensor_locations"])

    logger.info("Loading VanderSat Data")
    vandersat_data = read_vandersat_data(sensors)

    logger.info("Loading Raw Images")
    if params["distributed"]:
        images, dates, sensor_values = read_images_distributed(storage_client, sensors)
    else:
        images, dates, sensor_values = read_images(storage_client, sensors)

    logger.info("Calculating Indices")
    index_frame, sensor_indices = calculate_indices(images, dates, sensor_values)

    logger.info("Validating Indices")
    run_validation(index_frame, sensor_indices, vandersat_data, params["output_path"])

    logger.info("Writing Results")
    write_outputs(sensor_indices, params["output_path"], "sensor_indices")
    write_outputs(vandersat_data, params["output_path"], "soil_moisture")
