import os
import logging
from datetime import datetime
import subprocess
from utils import get_config, archive_files, duckdb_cleanup, run_step


config = get_config()

ROOT_DIR = config['paths']['root']
RAW_DIR = config['paths']['raw']
ARCHIVE_DIR = config['paths']['archive']
SRC_DIR = config['paths']['src']
DBT_PROJECT_DIR = config['paths']['dbt_project']
LOG_DIR = config['paths']['logs']

# Create necessary directories
os.makedirs(ARCHIVE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

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

def run_step1(name, command, cwd=None):
    logging.info(f"ACTION START: {name}")

    try:
        result = subprocess.run(command, shell=True, check=True, cwd=cwd, capture_output=True, text=True)
        
        if result.stdout:
            logging.info(f"STDOUT ({name}):\n{result.stdout.strip()}")

        logging.info(f"[SUCCESS] ACTION PASSED: {name}")
        return True
    
    except subprocess.CalledProcessError as e:
        logging.error(f"ACTION FAILED: {name}")
        logging.error(f"STDOUT: {e.stdout}")
        logging.error(f"STDERR: {e.stderr}")
        return False

# --- Main Flow ---
if __name__ == "__main__":

    logging.info(f"Current Working Directory: {os.getcwd()}")
    logging.info(f"Looking for workers script in: {os.path.abspath(SRC_DIR)}")

    logging.info("="*60)
    logging.info(f"PORTFOLIO PIPELINE START - SESSION {timestamp_str}")
    logging.info("="*60)

    step0_path = os.path.join(SRC_DIR, "00_validate_schema_contract.py")
    step0_validate_schema_contract_cmd = f"python {step0_path}"

    step1_path = os.path.join(SRC_DIR, "01_ingest_raw_files.py")
    step1_ingest_raw_file_cmd = f"python {step1_path} True"

    step2_path = os.path.join(SRC_DIR, "02_transform_to_bronze_files.py")
    step2_transform_to_bronze_files_cmd = f"python {step2_path} {log_filename}"

    venv_dbt = os.path.join(os.path.dirname(os.sys.executable), "dbt")
    step3_transform_dbt_cmd = f"{venv_dbt} build"

    success = False
    try:
        # Use run_step for EVERYTHING so logs are consistent
        if run_step("0. Check source files against schema contracts", step0_validate_schema_contract_cmd):
            if run_step("1. Ingestion of raw layer", step1_ingest_raw_file_cmd):
                if run_step("2. Transformation raw layer to bronze layer", step2_transform_to_bronze_files_cmd):
                    if run_step("3. Run dbt build", step3_transform_dbt_cmd, cwd=DBT_PROJECT_DIR):
                        os.makedirs(ARCHIVE_DIR, exist_ok=True)
                        archive_files(RAW_DIR, ARCHIVE_DIR, "raw")
                        success = True
                    else:
                        logging.error("PIPELINE HALTED: dbt build failed.")
                else:
                    logging.error("PIPELINE HALTED: Raw-to-Bronze transformation failed.")
            else:
                logging.error("PIPELINE HALTED: Raw layer ingestion failed.")
        else:
            logging.error("PIPELINE HALTED: Schema contract checks failed.")
    except Exception as e:
        logging.error(f"UNEXPECTED PIPELINE ERROR: {e}")
    finally:
        logging.info("CLEANUP: Removing transient DuckDB files...")
        clean_result, error_message = duckdb_cleanup(ROOT_DIR)

        if (not clean_result):
            logging.error(error_message)

        status = "COMPLETED SUCCESSFULLY" if success else "FAILED"
        logging.info("="*60)
        logging.info(f"PIPELINE END - STATUS: {status}")
        logging.info("="*60)
