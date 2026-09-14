import os
import json
import google.auth            as gauth
from   pathlib                import Path
from   typing                 import Optional
from   google.oauth2          import service_account
from   google.auth.exceptions import GoogleAuthError
from   .bigquery_connect      import BigQueryExporter
from   ..configs              import logger

try:
    from dotenv import load_dotenv
    LocDir = Path(__file__).resolve().parents[3]
except Exception:
    load_dotenv = None

def BQcred(
        project_id       : Optional[str] = None,
        credentials_path : Optional[str] = None,
        credentials_json : Optional[str] = None,
        dotenv_path      : Optional[str] = None,
    ) -> BigQueryExporter:
    """
    Initializes BigQueryExporter supporting 
    local .env files and GitHub Actions Secrets
    (both JSON string content and file paths).
    :param project_id: Optional GCP Project ID. 
           Fallbacks to GCP_PROJECT_ID env var.
    :param credentials_path: Optional path to JSON 
           key file. Fallbacks to GOOGLE_APPLICATION_CREDENTIALS.
    :param credentials_json: Optional raw JSON 
           string from GitHub Secret. Fallbacks to GCP_SA_KEY env var.
    :param dotenv_path: Optional custom path to 
           .env file.
    :return: Initialized BigQueryExporter instance
    """
    try:
        if load_dotenv is not None:
            dote = LocDir / '.env' if dotenv_path is None else dotenv_path
            if Path(dote).is_file():
                load_dotenv(dotenv_path = dote)
            else:
                load_dotenv()

        # 1. Resolve Project ID
        resolved_project_id = project_id or os.getenv("GCP_PROJECT_ID")
        if not resolved_project_id:
            raise ValueError(
            "GCP Project ID is required. Pass it as an "
            "argument or set GCP_PROJECT_ID env variable.")

        # 2. Check for direct JSON string content (Common for GitHub Secrets)
        credentials  = None
        json_content = (credentials_json
                        or os.getenv("GCP_SA_KEY")
                        or os.getenv("GOOGLE_CREDENTIALS_JSON"))
        if json_content:
            try:
                service_account_info = json.loads(json_content)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info)
                logger.debug("Successfully loaded GCP credentials "
                "from JSON environment string.")
            except json.JSONDecodeError as err:
                raise ValueError("Invalid JSON string "
                "provided for GCP credentials.") from err

        # 3. Check for File Path (Common for local .env or mounted secret file)
        if credentials is None:
            file_path_str = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if file_path_str:
                resolved_path = Path(file_path_str).resolve()
                if not resolved_path.is_file():
                    raise FileNotFoundError(
                    f"Credentials file not found at: {resolved_path}")
                credentials = service_account.Credentials.from_service_account_file(
                    str(resolved_path))
                logger.info("Successfully loaded GCP "
                f"credentials from file: {resolved_path}")

        # 4. Fallback to Application Default Credentials (ADC)
        if credentials is None:
            logger.info("No explicit key found. Falling "
            "back to Application Default Credentials (ADC).")
            credentials, _ = gauth.default()
        exporter = BigQueryExporter(
                   project_id  = resolved_project_id, 
                   credentials = credentials)
        return exporter

    except (ValueError, FileNotFoundError, GoogleAuthError) as err:
        logger.error(f"Initialization error: {err}")
        raise
    except Exception as err:  # nosec B110
        logger.error("Unexpected error while "
        f"initializing BigQueryExporter: {err}")
        raise RuntimeError("Failed to initialize "
        f"BigQueryExporter: {err}") from err

if __name__ == '__main__':
    pass
