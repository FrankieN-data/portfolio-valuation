import os
import logging
import shutil
import glob
from datetime import datetime
from utils import get_config, run_step, archive_files

config = get_config()
RAW_DIR = config['paths']['raw']
ARCHIVE_DIR = config['paths']['archive']
SRC_DIR = config['paths']['src']
DBT_PROJECT_DIR = config['paths']['dbt_project']
LOG_DIR = config['paths']['logs']

# Create necessary directories
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# --- Logging Setup ---
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = os.path.join(LOG_DIR, f"pipeline_{timestamp_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def cleanup():
    logging.info("CLEANUP: Removing transient DuckDB files...")
    patterns = ['*.duckdb.wal', '*.tmp', 'duckdb_temp_dir']
    for p in patterns:
        for path in glob.glob(p):
            try:
                if os.path.isdir(path): shutil.rmtree(path)
                else: os.remove(path)
            except Exception as e:
                logging.warning(f"Could not remove {path}: {e}")

# --- Main Flow ---
if __name__ == "__main__":
    logging.info(f"Current Working Directory: {os.getcwd()}")
    logging.info(f"Looking for SQL in: {os.path.abspath(SRC_DIR)}")
    logging.info(f"Files found there: {os.listdir(SRC_DIR)}")

    logging.info("="*60)
    logging.info(f"PORTFOLIO PIPELINE START - SESSION {timestamp_str}")
    logging.info("="*60)


    step1_path = os.path.join(SRC_DIR, "01_ingest_raw_files.py")
    step1_ingest_raw_file_cmd = f"python {step1_path} {log_filename}"

    step2_path = os.path.join(SRC_DIR, "02_transform_to_bronze_files.py")
    step2_transform_to_bronze_files_cmd = f"python {step2_path} {log_filename}"

    step3_transform_dbt_cmd = "dbt build"

    success = False
    try:
        # Use run_step for EVERYTHING so logs are consistent
        if run_step("Ingest Raw", step1_ingest_raw_file_cmd):
            if run_step("Transform to Bronze", step2_transform_to_bronze_files_cmd):
                if run_step("dbt Build", step3_transform_dbt_cmd, cwd=DBT_PROJECT_DIR):
                    
                    # Call archive normally (no colon at the end)
                    archive_files(RAW_DIR, ARCHIVE_DIR, "raw")
                    success = True
                else:
                    logging.error("PIPELINE HALTED: dbt build failed.")
            else:
                logging.error("PIPELINE HALTED: Raw-to-Bronze step failed.")
        else:
            logging.error("PIPELINE HALTED: Ingest Raw failed.")
    except Exception as e:
        logging.error(f"UNEXPECTED PIPELINE ERROR: {e}")
    finally:
        cleanup()
        status = "COMPLETED SUCCESSFULLY" if success else "FAILED"
        logging.info("="*60)
        logging.info(f"PIPELINE END - STATUS: {status}")
        logging.info("="*60)