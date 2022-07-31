import logging, coloredlogs
from typing import Dict

logger = logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger)


def indices(params: Dict[str, str]):
    logger.info("Loading Input Data")

    logger.info("Preprocessing Images")

    logger.info("Splitting Validation Data")

    logger.info("Calculating Indices")

    logger.info("Validating Indices")

    logger.info("Writing Results")
