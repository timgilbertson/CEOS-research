import logging, coloredlogs
from typing import Dict

from .io.inbound import read_images
from .indices.calculate_indices import calculate_indices
from .validation.run_validation import run_validation

logger = logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)


def indices(params: Dict[str, str]):
    logger.info("Loading Input Data")
    images, dates = read_images(params["input_images"])

    logger.info("Preprocessing Images")

    logger.info("Splitting Validation Data")

    logger.info("Calculating Indices")
    calculated_indices = calculate_indices(images, dates)

    logger.info("Validating Indices")
    run_validation(calculated_indices)

    logger.info("Writing Results")
