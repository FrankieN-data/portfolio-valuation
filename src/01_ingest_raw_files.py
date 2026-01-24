import logging
import shutil
from pathlib import Path
import sys
from utils import get_config, get_smart_logger

# Return status code
# 0 - Success
# 1 - No file 

# Setup logging with a safety check for the argument
logger = get_smart_logger(__name__)

def ingest_raw_files():
    # Define your paths
    config = get_config()
    source_path = config['paths']['source']
    raw_path = config['paths']['raw']    

    # 1. Ensure the raw directory exists
    raw_path.mkdir(parents=True, exist_ok=True)
    
    # 2. Identify files to copy (e.g., all CSVs)
    files_to_copy = list(source_path.glob("*.csv"))
    
    if not files_to_copy:
        logger.warning(f"No files found in {source_path}")
        sys.exit(1)

    for file_path in files_to_copy:
        # 3. Copy file to the raw folder
        shutil.copy(file_path, raw_path / file_path.name)
        logger.info(f"[SUCCESS] Ingested: {file_path.name}")

    logger.info(f"[INGESTION COMPLETED SUCCESSFULLY] Ingestion complete. {len(files_to_copy)} files ready in {raw_path}")
    sys.exit(0)

if __name__ == "__main__":
    ingest_raw_files()