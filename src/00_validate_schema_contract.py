import polars as pl
import sys
from pathlib import Path
import fnmatch
from utils import get_config, get_smart_logger

logger = get_smart_logger("SCHEMA CONTRACT")
config = get_config()

def validate_source_schema_contracts():
    """
        Validates source files against contracts defined in the YAML config.

        Exit code:
        -----------
        0: Success
        1: Read access issue - no permission or no file
        2: Contract violated
        3: Contract not found
    """

    schema_contracts = config.get('schema_contracts', {})
    source_path = Path(config['paths']['source'])

    try:
        file_paths = list(source_path.glob("*.csv"))
    except Exception as e:
        logger.error(f"Could not access source path {source_path}: {e}")
        sys.exit(1)

    if not file_paths:
        logger.error(f"No CSV files found in {source_path}")
        sys.exit(1)

    error_count = 0
    files_checked_count = 0
    unmatched_contract_count = 0

    for file_path in file_paths:
        file_name = file_path.name
        matched_any_contract = False
        
        for contract_name, contract_info in schema_contracts.items():
            pattern = contract_info.get('file_pattern')
            
            # Check if this file is governed by this contract
            if pattern and fnmatch.fnmatch(file_name, pattern):
                matched_any_contract = True
                files_checked_count += 1
                required_cols = contract_info.get('required_columns', [])
                
                try:
                    # Use scan_csv for the fastest possible header extraction
                    actual_cols = pl.scan_csv(file_path).collect_schema().names()
                    missing = [c for c in required_cols if c not in actual_cols]
                
                    if missing:
                        error_count += 1
                        logger.error(f"CONTRACT VIOLATION [{contract_name}]: Missing columns in file {file_name} ({missing})")
                    else:
                        logger.info(f"PASSED: {file_name} matches contract '{contract_name}'")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Could not read {file_name}: {e}")

        if not matched_any_contract:
            unmatched_contract_count += 1
            logger.warning(f"SKIPPED: {file_name} does not match any known schema contract.")

    if error_count > 0:
        logger.error(f"Validation failed: {error_count} file(s) violated schema contracts.")
        sys.exit(2)

    if unmatched_contract_count > 0:
        logger.error(f"Skipped files: {unmatched_contract_count} file(s) have no schema contract.")
        sys.exit(3)

    logger.info(f"Success: {files_checked_count} files validated against contracts.")
    sys.exit(0)

if __name__ == "__main__":
    validate_source_schema_contracts()