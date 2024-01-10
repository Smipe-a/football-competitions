import logging
import os


def configure_logger(logger_name: str) -> logging.Logger:
    """
    Configures and returns a logger with a specified file name.

    Args:
        logger_name (str): The name of the log file.

    Returns:
        logging.Logger: Configured logger.
    """

    # Define log formatter
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')

    # We obtain the current directory and its parent directory.
    # An absolute path is constructed based on the parent path
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    log_file_path = os.path.join(parent_directory, 'logs', 'epl_log.log')

    # Create a file handler and set the formatter
    handler = logging.FileHandler(filename=log_file_path, mode='a')
    handler.setFormatter(formatter)

    # Create and configure the logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
