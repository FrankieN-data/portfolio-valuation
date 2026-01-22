import os
import subprocess
import logging
import shutil
import zipfile 
import glob
from datetime import datetime

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_DIR = os.path.join(SCRIPT_DIR, './data/raw')
ARCHIVE_ROOT = os.path.join(SCRIPT_DIR, './data/archive')
SCRIPTS_DIR = os.path.join(SCRIPT_DIR, './scripts/python')
DBT_PROJECT_DIR = os.path.join(SCRIPT_DIR, './dbt_project')
LOG_DIR = os.path.join(SCRIPT_DIR, './logs')

# Create necessary directories
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ARCHIVE_ROOT, exist_ok=True)

# --- Logging Setup ---
timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = os.path.join(LOG_DIR, f"pipeline_{timestamp_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler() # This also prints to your terminal
    ]
)

def run_step(name, command, cwd=None):
    logging.info(f"ACTION START: {name}")
    try:
        result = subprocess.run(command, shell=True, check=True, cwd=cwd, capture_output=True, text=True)
        logging.info(f"ACTION PASSED: {name}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"ACTION FAILED: {name}")
        logging.error(f"STDOUT: {e.stdout}")
        logging.error(f"STDERR: {e.stderr}")
        return False

def run_raw_to_bronze():
    migration_scripts = sorted([f for f in os.listdir(SCRIPTS_DIR) if f.startswith('01') and f.endswith('.py')])
    passed, failed = [], []
    
    logging.info(f"--- Starting Migration Loop ({len(migration_scripts)} files) ---")

    for script in migration_scripts:
        script_path = os.path.join(SCRIPTS_DIR, script)
        # Using the "" at the end tells DuckDB to run the init script and then exit
        cmd = f"python {script_path}"
        
        if run_step(f"Migrating {script}", cmd):
            passed.append(script)
        else:
            failed.append(script)

    # Log the summary in the file
    logging.info(f"MIGRATION SUMMARY: {len(passed)} Passed, {len(failed)} Failed")
    if failed:
        logging.warning(f"FAILED SCRIPTS: {', '.join(failed)}")
    
    return len(failed) == 0

def archive_files():
    files = [f for f in os.listdir(RAW_DIR) if f.endswith('.csv')]
    if files:
        # Create a filename like: raw_20260121_232434.zip
        zip_name = f"raw_{timestamp_str}.zip"
        zip_path = os.path.join(ARCHIVE_ROOT, zip_name)
        
        logging.info(f"ARCHIVING: Creating zip archive {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in files:
                file_full_path = os.path.join(RAW_DIR, f)
                # Add file to zip and then remove the original
                zipf.write(file_full_path, f)
                os.remove(file_full_path)
                
        logging.info(f"ARCHIVING: Compressed {len(files)} files into {zip_name}")

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
    if __name__ == "__main__":
        print(f"📍 Current Working Directory: {os.getcwd()}")
        print(f"📂 Looking for SQL in: {os.path.abspath(SCRIPTS_DIR)}")
        print(f"📄 Files found there: {os.listdir(SCRIPTS_DIR)}")

    logging.info("="*60)
    logging.info(f"PORTFOLIO PIPELINE START - SESSION {timestamp_str}")
    logging.info("="*60)
    
    success = False
    try:
        if run_raw_to_bronze():
            if run_step("dbt Build", "dbt build", cwd=DBT_PROJECT_DIR):
                archive_files()
                success = True
        else:
            logging.error("PIPELINE HALTED: Raw-to-Bronze step failed.")
            
    finally:
        cleanup()
        status = "COMPLETED SUCCESSFULLY" if success else "FAILED"
        logging.info("="*60)
        logging.info(f"PIPELINE END - STATUS: {status}")
        logging.info("="*60)