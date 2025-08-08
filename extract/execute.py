import sys
import os
import requests
from zipfile import ZipFile

# Add the 'utility' folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utility')))

from utility import setup_logger, format_time
import time

# Set up logger
logger = setup_logger('extract.log')

def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file : {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

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

def fix_json_dict(output_dir):
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    
    with open(file_path, "r") as f:
        data = json.load(f)

    with open(os.path.join(output_dir, "fixed_da.json"), "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")

    logger.info(f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json")
    logger.info("Removing the original file")
    os.remove(file_path)

if __name__ == "__main__":
    if (len(sys.argv) < 2):
        logger.error("Extraction path is required")
        logger.error("Usage: python3 execute.py /home/sujal/Data/extract")
        sys.exit(1)

    try:
        logger.info("Starting Extraction Engine...")
        EXTRACT_PATH = sys.argv[1]
        zip_file = os.path.join(EXTRACT_PATH, "archive.zip")
        extract_zip_file(zip_file, EXTRACT_PATH)
        fix_json_dict(EXTRACT_PATH)
        logger.info("Extraction Successfully Completed!!!.")
    except Exception as e:
        logger.error(f"Error: {e}")
