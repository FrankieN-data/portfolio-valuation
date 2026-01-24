# scripts/utils.py
from datetime import datetime
import glob
import logging
from pathlib import Path
import subprocess
import yaml
import os
import zipfile 
from datetime import datetime
import shutil
import sys

def get_config():
    base_dir = Path(__file__).resolve().parent.parent
    with open(base_dir / "config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Pre-join the base_dir to the paths so the scripts don't have to
    for key, value in config['paths'].items():
        config['paths'][key] = base_dir / value
        
    return config

def archive_files(file_dir=None, archive_dir=None, zip_prefix="archive", timestamp_str=None):    

    if file_dir is None or archive_dir is None:
        raise ValueError("archive_files: file_dir and archive_dir must be provided")

    # Si aucun timestamp n'est passé, on le génère à l'instant T de l'appel
    if timestamp_str is None:
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')

    files = [f for f in os.listdir(file_dir) if os.path.isfile(os.path.join(file_dir, f))]
    if files:
        # Create a filename like: raw_20260121_232434.zip
        zip_name = f"{zip_prefix}_{timestamp_str}.zip"
        zip_path = os.path.join(archive_dir, zip_name)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in files:
                file_full_path = os.path.join(file_dir, f)
                # Add file to zip and then remove the original
                zipf.write(file_full_path, f)
                os.remove(file_full_path)
                
        return True
    return False
    
def duckdb_cleanup():

    patterns = ['*.duckdb.wal', '*.tmp', 'duckdb_temp_dir']
    for p in patterns:
        for path in glob.glob(p):
            try:
                if os.path.isdir(path): shutil.rmtree(path)
                else: os.remove(path)
            except Exception as e:
                return f"Could not remove {path}: {e}"
    return None

def get_smart_logger(logger_name):
    # If the orchestrator passed a path, use it. Otherwise, use basic config.
    if len(sys.argv) > 1:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(sys.argv[1]), logging.StreamHandler()]
        )
    else:
        logging.basicConfig(level=logging.INFO)
    
    return logging.getLogger(logger_name)

def run_step(name, command, cwd=None):
    logger = get_smart_logger(__name__)

    logger.info(f"ACTION START: {name}")
    try:
        result = subprocess.run(command, shell=True, check=True, cwd=cwd, capture_output=True, text=True)
        logger.info(f"[SUCCESS] ACTION PASSED: {name}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ACTION FAILED: {name}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        return False