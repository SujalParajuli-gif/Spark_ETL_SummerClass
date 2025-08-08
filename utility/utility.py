# utility.py

import logging
import time

def setup_logger(log_file_name):
    """
    Set up the logger to log messages to both stdout and a log file.
    
    Args:
    log_file_name (str): The name of the log file to save logs.
    
    Returns:
    logger (logging.Logger): A configured logger instance.
    """
    logger = logging.getLogger()  # Root logger
    logger.setLevel(logging.DEBUG)  # Set the log level to debug
    
    # Create a handler for output to the terminal (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    
    # Create a handler to write logs to a file
    file_handler = logging.FileHandler(log_file_name)
    file_handler.setLevel(logging.DEBUG)
    
    # Create a log formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger


def format_time(seconds):
    """
    Format the time difference into hours, minutes, and seconds.
    
    Args:
    seconds (int): The number of seconds to be formatted.
    
    Returns:
    str: The formatted time in HH:MM:SS format.
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"
