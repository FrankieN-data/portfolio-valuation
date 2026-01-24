import os
import sys
from utils import get_config, get_smart_logger, run_step

config = get_config()
RAW_DIR = config['paths']['raw']
ARCHIVE_ROOT = config['paths']['archive']
SRC_DIR = config['paths']['src']
DBT_PROJECT_DIR = config['paths']['dbt_project']
LOG_DIR = config['paths']['logs']

logger = get_smart_logger(__name__)

def ingest_bronze_files(logfile_path=None):
    migration_scripts = sorted([f for f in os.listdir(SRC_DIR) if f.startswith('02_raw_to_bronze') ])
    passed, failed = [], []
    
    logger.info(f"--- Starting Migration Loop ({len(migration_scripts)} files) ---")

    for script in migration_scripts:
        script_path = os.path.join(SRC_DIR, script)
        
        cmd = f"python {script_path} {logfile_path}"
        
        if run_step(f"Migrating {script}", cmd):
            passed.append(script)
        else:
            failed.append(script)

    # Log the summary in the file
    logger.info(f"MIGRATION SUMMARY: {len(passed)} Passed, {len(failed)} Failed")
    if failed:
        logger.warning(f"FAILED SCRIPTS: {', '.join(failed)}")
    
    return len(failed) == 0

if __name__ == "__main__":
    ingest_bronze_files(sys.argv[1])