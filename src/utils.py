# scripts/utils.py
from datetime import datetime
import glob
import logging
from pathlib import Path
import yaml
import os
import zipfile 
from datetime import datetime
import shutil
import sys
import subprocess

def get_config():
    """
    Loads and pre-processes the project configuration from the root config.yaml.

    This function performs dynamic path resolution by locating the project 
    root directory and converting relative paths in the YAML into absolute 
    pathlib.Path objects.

    Returns:
    --------
    config : dict
        A dictionary containing all configuration parameters.
        Note: Items under the ['paths'] key are converted from strings 
        to absolute pathlib.Path objects, making them cross-platform compatible.

    Example:
    --------
    >>> cfg = get_config()
    >>> print(cfg['paths']['raw'])
    PosixPath('/home/user/project/data/raw')
    """
        
    base_dir = Path(__file__).resolve().parent.parent
    with open(base_dir / "config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Pre-join the base_dir to the paths so the scripts don't have to
    for key, value in config['paths'].items():
        config['paths'][key] = base_dir / value
        
    return config


def archive_files(file_dir=None, archive_dir=None, zip_prefix="archive", timestamp_str=None):    
    """
        Compresses processed files into a ZIP archive and removes the original files.

        This function serves as the 'finalization' step of the ingestion pipeline. 
        It ensures that the landing zone (raw folder) is cleared, preventing 
        duplicate processing while maintaining a timestamped audit trail of 
        the source data.

        Parameters:
        -----------
        file_dir : pathlib.Path or str
            The directory containing the files to be archived (e.g., 'data/raw').
        
        archive_dir : pathlib.Path or str
            The destination directory where the ZIP file will be stored (e.g., 'data/archive').
        
        zip_prefix : str, optional
            A prefix for the ZIP filename (default is "archive"). Useful for 
            identifying the source system (e.g., "vanguard").

        timestamp_str : str, optional
            A specific timestamp to use in the filename (Format: YYYYMMDD_HHMMSS).
            If None, the current system time is used.

        Returns:
        --------
        bool, msg
            True if files were found and archived successfully, False if the 
            source directory was empty.
    """

    if file_dir is None or archive_dir is None:
        return False, f"archive_files missing required positional arguments: 'file_dir' and 'archive_dir'"

    if not os.path.exists(archive_dir):
        return False, f"{archive_dir} doesn't exist"
            
    # Si aucun timestamp n'est passé, on le génère à l'instant T de l'appel
    if timestamp_str is None:
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')

    try:
        files = [f for f in os.listdir(file_dir) if os.path.isfile(os.path.join(file_dir, f))]

        if not files:
            return False, f"{file_dir}: No file found to archive"
        
        zip_name = f"{zip_prefix}_{timestamp_str}.zip"
        zip_path = os.path.join(archive_dir, zip_name)
            
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f in files:
                file_full_path = os.path.join(file_dir, f)
                # Add file to zip and then remove the original
                zipf.write(file_full_path, f)
                os.remove(file_full_path)
                 
        return True, None

    except Exception as e:
        return False, f"Archive process failed: {e}"
    

def duckdb_cleanup(project_root_path=None):
    """
        Cleans up transient DuckDB artifacts to prevent database locks and workspace clutter.

        This function searches for and removes Write-Ahead Log (.wal) files, 
        temporary spill-to-disk directories, and other transient files generated 
        during a DuckDB/dbt execution. It is designed to be called in a 'finally' 
        block to ensure hygiene regardless of process success.

        Parameters:
        -----------
        project_root_path : pathlib.Path or str, optional
            The directory where DuckDB execution takes place. If None, it defaults 
            to the current working directory (".").

        Returns:
        --------
        tuple (bool, str or None)
            - bool: True if cleanup completed without errors (even if no files were found).
            - str/None: An error message if a file was found but could not be deleted.

        Notes:
        ------
        - Uses `shutil.rmtree` for directories (like duckdb_temp_dir).
        - Uses `os.remove` for individual files (like .wal files).
        - Uses `glob` for pattern matching, making it resilient to varying filenames.
    """

    search_dir = project_root_path if project_root_path else "."
    patterns = ['*.duckdb.wal', '*.tmp', 'duckdb_temp_dir']

    for p in patterns:
        full_pattern = os.path.join(search_dir, p)
        for path in glob.glob(full_pattern):
            try:
                if os.path.isdir(path): shutil.rmtree(path)
                else: os.remove(path)
            except Exception as e:
                return False, f"Could not remove {path}: {e}"
    return True, None


def get_smart_logger(logger_name):
    """
        Initializes a logger that dynamically toggles between console and file output.

        If a file path is provided as the first command-line argument (sys.argv[1]), 
        the logger will output to both that file and the console. Otherwise, 
        it defaults to basic console-only logging.

        Parameters:
        -----------
        logger_name : str
            The name of the logger, typically passed as __name__ to identify 
            which module is generating the logs.

        Returns:
        --------
        logging.Logger
            A configured logger instance ready for info, warning, and error calls.
        
        Notes:
        ------
        This 'smart' behavior is designed for orchestration:
        - Manual run: `python script.py` -> Logs to console.
        - Orchestrated run: `python script.py logs/run.log` -> Logs to file AND console.
    """    
    
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