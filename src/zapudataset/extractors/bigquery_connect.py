#!/usr/bin/env python3

__author__     = "Aryanto"
__copyright__  = "Copyright 2026, AryDataLabs/ZaPuDia Series"
__credits__    = ["aryanto"]
__license__    = "GNU_Public"
__version__    = "0.0.1"
__maintainer__ = "Aryanto, M.Si"
__email__      = "aryanto.dandan@gmail.com"
__created__    = "2026-08-31"
__modified__   = "2026-09-12"

"""
BigQuery Exporter Helper Class.
Designed for BigQuery queries (Public Datasets / 
Free Sandbox without Credit Card)
and saving results to Local PC (CSV) or Google Drive.
"""

import os
import io
import pandas       as pd
import google.auth  as gauth
from   typing       import Optional
from   google.cloud import bigquery
from   googleapiclient.http      import MediaIoBaseUpload
from   googleapiclient.discovery import build  # nosec B410
from   ..configs    import logger


class BigQueryExporter:
    """
    Class to execute BigQuery queries and 
    export results to CSV locally or to Google Drive.
    """

    def __init__(
            self, 
            project_id: str, 
            credentials: Optional[gauth.credentials.Credentials] = None,
        ):
        """
        Initialize BigQuery Exporter.

        :param project_id: GCP Project ID
        :param credentials: Optional auth credentials (defaults to Application Default Credentials)
        """
        self.project_id = project_id
        if credentials is None:
            credentials, _ = gauth.default()
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.df: Optional[pd.DataFrame] = None

    def run_query(self, sql_query: str) -> pd.DataFrame:
        """
        Executes SQL query on BigQuery and stores the result in self.df.
        :param sql_query: SQL query string to be executed
        :return: pandas.DataFrame stored in self.df
        """
        logger.debug("Sending query to BigQuery...")
        query_job = self.client.query(sql_query)
        self.df   = query_job.to_dataframe()
        logger.debug(f"Query executed successfully! ({len(self.df)} rows fetched)")
        return self.df

    def export_to_local_csv(
            self, 
            filepath  : str           = "query_result.csv", 
            sql_query : Optional[str] = None,
        ) -> str:
        """
        Saves the stored DataFrame (or executes a new query) to a local CSV file.
        :param filepath: Target CSV file path on local PC
        :param sql_query: Optional SQL query string. If provided, executes it first.
        :return: Absolute file path of saved CSV
        """
        if sql_query is not None:
            self.run_query(sql_query)
        if self.df is None:
            logger.error("No DataFrame available.\n"
            "Run a query first or provide sql_query.")
            raise ValueError()
        self.df.to_csv(filepath, index = False)
        abs_path = os.path.abspath(filepath)
        logger.info(f"File successfully saved to Local PC: {abs_path}")
        return abs_path

    def export_to_gdrive(
        self,
        filename: str = "query_result.csv",
        sql_query: Optional[str] = None,
        folder_id: Optional[str] = None,
        gdrive_mount_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Saves the stored DataFrame (or executes a new query) to Google Drive.
        :param filename: Target file name in Google Drive
        :param sql_query: Optional SQL query string. If provided, executes it first.
        :param folder_id: (Optional) Google Drive target folder ID when using API
        :param gdrive_mount_path: (Optional) Google Drive mount path (e.g., '/content/drive/MyDrive' in Google Colab)
        :return: Google Drive web link or saved path
        """
        if sql_query is not None:
            self.run_query(sql_query)
        if self.df is None:
            logger.error("No DataFrame available. "
            "Run a query first or provide sql_query.")
            raise ValueError()

        # Option 1: Save via Google Colab mounted drive path
        if gdrive_mount_path:
            full_path = os.path.join(gdrive_mount_path, filename)
            self.df.to_csv(full_path, index=False)
            logger.info("File successfully saved to "
            f"Google Drive (Mounted) at: {full_path}")
            return full_path

        # Option 2: Upload via Google Drive API
        link              = str()
        try:
            # Request credentials with Google Drive scope
            scopes        = ['https://www.googleapis.com/auth/drive.file',
                             'https://www.googleapis.com/auth/cloud-platform']
            creds, _      = gauth.default(scopes=scopes)
            drive_service = build('drive', 'v3', credentials=creds)
            # Create CSV in-memory buffer
            csv_buffer    = io.StringIO()
            self.df.to_csv(csv_buffer, index = False)
            csv_bytes     = csv_buffer.getvalue().encode('utf-8')
            media         = MediaIoBaseUpload(
                            io.BytesIO(csv_bytes), 
                            mimetype  = 'text/csv', 
                            resumable = True)
            file_metadata = {'name': filename}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            file          = drive_service.files().create(
                body      = file_metadata,
                media_body= media,
                fields    = 'id, webViewLink'
                ).execute()
            link          = file.get('webViewLink')
            logger.debug("File successfully uploaded to Google Drive via API!")
            logger.info(f"File Link: {link}")
        except Exception as e:  # nosec B110
            logger.error(f"Failed to upload via Drive API: {e}")
            logger.error("Note: Ensure gcloud login supports "
            "Drive scopes or pass gdrive_mount_path if "
            "running in Google Colab.")
        finally:
            return link


if __name__ == '__main__':
    logger.debug("BigQueryExporter class loaded and ready to use.")
    exporter = BigQueryExporter(project_id = "your-gcp-project-id")
    sql = """
        SELECT word, word_count
        FROM `bigquery-public-data.samples.shakespeare`
        WHERE corpus = 'hamlet'
        ORDER BY word_count DESC
        LIMIT 10
    """
    exporter.run_query(sql)
    exporter.export_to_local_csv(filepath="hamlet_top10.csv")
    # exporter.export_to_gdrive(filename="hamlet_top10.csv")
