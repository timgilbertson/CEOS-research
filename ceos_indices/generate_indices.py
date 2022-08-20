import logging, coloredlogs
from typing import Dict

from .io.inbound import read_images

logger = logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)


def indices(params: Dict[str, str]):
    logger.info("Loading Input Data")
    images = read_images(params["input_images"])

    logger.info("Preprocessing Images")

    logger.info("Splitting Validation Data")

    logger.info("Calculating Indices")

    logger.info("Validating Indices")

    logger.info("Writing Results")
