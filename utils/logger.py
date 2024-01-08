import logging


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

    # Create a file handler and set the formatter
    handler = logging.FileHandler(filename='../logs/epl_log.log', mode='a')
    handler.setFormatter(formatter)

    # Create and configure the logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
