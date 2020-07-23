import logging


def get_logger(loglevel="INFO"):
    logger = logging.getLogger(__name__)
    if not logger.hasHandlers():
        logger.setLevel(loglevel)
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(funcName)s - %(message)s')
        stdout = logging.StreamHandler()
        stdout.setLevel(loglevel)
        stdout.setFormatter(formatter)
        logger.addHandler(stdout)
    return logger
