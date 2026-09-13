#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-12"


"""
SQL Loader Utility Package.
Quick helper module to grab local .sql query 
files and pass 'em to BigQuery.
"""

from pathlib  import Path
from ..config import logger

MODULE_DIR = Path(__file__).resolve().parent

def ListTemplate(directory: str | Path = MODULE_DIR) -> list[str]:
    """
    Scan target directory and list all 
    available .sql filenames.
    Returns a list of SQL file names 
    (e.g., ['chicago_taxi_trips.sql', ...]).
    """
    try:
        target_path = Path(directory)
        if not target_path.exists() or not target_path.is_dir():
            raise FileNotFoundError(f"Directory not found: {target_path}")
        sql_files = [f.name for f in 
                    target_path.glob("*.sql") if f.is_file()]  # nosec B101
        logger.info(f"Successfully found {len(sql_files)} SQL files in {target_path}")
        return sorted(sql_files)
    except Exception as err:
        logger.error(f"Failed to list SQL files in '{directory}': {err}")
        raise err

def ReadSQL(
        file_name: str | Path,
        directory: str | Path = MODULE_DIR,
    ) -> str:
    """
    Read and return the raw string query 
    from a specific .sql file.
    Accepts filename with or without '.sql' extension.
    """
    QueryText = str()
    try:
        file_path = Path(file_name).resolve()
        if not file_path.is_absolute():
            file_path = Path(directory) / file_name
        if file_path.suffix != ".sql":
            file_path = file_path.with_suffix(".sql")
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file does not exist: {file_path}")
        with open(file_path, "r", encoding="utf-8") as fsql:  # nosec B108
            QueryText = fsql.read()
        logger.debug(f"Loaded SQL query from '{file_path.name}' successfully.")
    except Exception as Arc:
        logger.error(f"Failed to read SQL query file '{file_name}': {Arc}.")
        raise Arc
    finally:
        return QueryText

__all__ = ["ListTemplate", "ReadSQL"]

if __name__ == '__main__':
    sql_files = ListTemplate()
    print("Available queries:", sql_files)
    print('\n'*2)
    query_content = ReadSQL("chicago_taxi_trips.sql")
    print(query_content[:10])
