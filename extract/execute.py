import sys
import os
import time
from zipfile import ZipFile
import logging

# Add the 'utility' folder to the Python path (if required for logging)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utility')))

from utility import setup_logger, format_time

# Set up logger
logger = setup_logger('extract.log')

def extract_zip_file(zip_filename, output_dir):
    start_time = time.time()
    with ZipFile(zip_filename, 'r') as zip_file:
        zip_file.extractall(output_dir)
    end_time = time.time()
    elapsed_time = end_time - start_time
    formatted_time = format_time(elapsed_time)
    logger.info(f"Extracted zip file to {output_dir} in {formatted_time}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)

def process_csv_files(output_dir):
    # Check if the expected CSV files are in the extracted folder
    csv_files = ["shopping_behavior_updated.csv", "shopping_trends.csv"]
    
    for csv_file in csv_files:
        file_path = os.path.join(output_dir, csv_file)
        if os.path.exists(file_path):
            logger.info(f"Found CSV file: {csv_file}")
        else:
            logger.warning(f"CSV file {csv_file} is missing!")

if __name__ == "__main__":
    if (len(sys.argv) < 2):
        logger.error("Extraction path is required")
        logger.error("Usage: python3 extract.py /path/to/extract/folder")
        sys.exit(1)

    try:
        logger.info("Starting Extraction Engine...")
        EXTRACT_PATH = sys.argv[1]
        zip_file = os.path.join(EXTRACT_PATH, "archive.zip")

        if not os.path.exists(zip_file):
            raise Exception("archive.zip not found in the specified directory")

        # Extract the zip file
        extract_zip_file(zip_file, EXTRACT_PATH)

        # Process the CSV files
        process_csv_files(EXTRACT_PATH)

        logger.info("Extraction Successfully Completed!!!.")
    except Exception as e:
        logger.error(f"Error: {e}")
