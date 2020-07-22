import logging


def get_logger(loglevel="INFO", file_log=False, logger_name="Logger"):
    """ Configure a general logger """
    logger = logging.getLogger(__name__)
    if not logger.hasHandlers():
        logger.setLevel(loglevel)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        if file_log:
            file_handler = logging.FileHandler("comparator.log", mode="w", encoding="utf-8")
            file_handler.setLevel("DEBUG")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        stdout = logging.StreamHandler()
        stdout.setLevel(loglevel)
        stdout.setFormatter(formatter)
        logger.addHandler(stdout)
    return logger
