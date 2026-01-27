import shutil
import sys
import os
from pathlib import Path
from utils import get_smart_logger, get_config

logger = get_smart_logger("RAW INGESTION")
config = get_config()

def ingest_raw_files(source_validated_flag):
    """
        Physically moves a list of pre-validated source files to the raw directory.
        
        Parameters:
        -----------
        source_validated_flag : str
            Expects values True or False, representing the result of the schema scan on source files
    """
    
    if not source_validated_flag.lower() == 'true':
        logger.error("At least one source file not validated.")
        sys.exit(1)

    # Convert strings from config into Path objects
    source_path = Path(config['paths']['source'])
    raw_path = Path(config['paths']['raw'])
    file_paths = list(source_path.glob("*.csv"))

    # Source files validated : there are some files and they are all checked
    for file_path in file_paths:
        try:
            shutil.copy(file_path, raw_path / os.path.basename(file_path))
            logger.info(f"File {file_path} moved successfully")
        except PermissionError:
            logger.error(f"Permission Denied: Is the file {file_path} open in another program?")
            sys.exit(2)
        except OSError as e:
            logger.error(f"System error while moving file {file_path}: {e}.")
            sys.exit(3)
        except Exception as e:
            logger.error(f"Unexpected error moving file {file_path}: {e}.")
            sys.exit(4)

    logger.info(f"Successfully ingested {len(file_paths)} files.")
    sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ingest_raw_files(sys.argv[1])
    else:
        logger.error("No validation flag provided.")
        sys.exit(1)