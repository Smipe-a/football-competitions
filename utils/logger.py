from scripts.constants import PROJECT_DIRECTORY, LOG_CATALOG
import logging
import os


def configure_logger(logger_name: str, file_name: str) -> logging.Logger:
    """
    Configures and returns a logger with a specified file name.

    Args:
        logger_name (str): The name of the module from which the log originated.
        file_name (str): The name of the file where logs are recorded for each competition.

    Returns:
        logging.Logger: Configured logger.
    """

    # Define log formatter
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')

    logs_directory = os.path.join(PROJECT_DIRECTORY, LOG_CATALOG)

    if not os.path.exists(logs_directory):
        os.makedirs(logs_directory)

    # Create a file handler and set the formatter
    handler = logging.FileHandler(filename=os.path.join(logs_directory, file_name), mode='a')
    handler.setFormatter(formatter)

    # Create and configure the logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
