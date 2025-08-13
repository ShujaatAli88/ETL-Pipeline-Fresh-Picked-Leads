import os
import csv
import traceback
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account
from log_handler import logger
from config import Config


# ---------- Helpers to build safe casts ----------
# Map BigQuery types to SAFE_CAST targets
# (Covers the common primitive types you’ll see. Add more if your tables use them.)
_CAST_TARGETS = {
    "STRING": "STRING",
    "BOOL": "BOOL",
    "BOOLEAN": "BOOL",
    "INT64": "INT64",
    "INTEGER": "INT64",
    "FLOAT64": "FLOAT64",
    "FLOAT": "FLOAT64",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIME": "TIME",
    "GEOGRAPHY": "GEOGRAPHY",
}


def _safe_cast_expr(staging_alias: str, col_name: str, target_type: str) -> str:
    """
    Build a SELECT expression that converts staging.{col} into the target_type.
    Uses SAFE_CAST so bad values become NULL instead of failing the job.
    """
    tgt = _CAST_TARGETS.get(target_type.upper(), "STRING")
    if tgt == "STRING":
        return f"CAST({staging_alias}.{col_name} AS STRING) AS {col_name}"
    else:
        # SAFE_CAST for numeric/date/time/timestamp/bool/etc.
        return f"SAFE_CAST({staging_alias}.{col_name} AS {tgt}) AS {col_name}"


class BigQueryUploader:
    def __init__(self, project_id: str, dataset_id: str, download_path: str, credentials_path: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.download_path = download_path

        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(project=project_id, credentials=credentials)

    # ---------- Table utilities ----------

    def table_exists(self, table_id: str) -> bool:
        try:
            self.client.get_table(table_id)
            return True
        except Exception:
            return False

    def get_table(self, table_id: str) -> bigquery.Table:
        return self.client.get_table(table_id)

    # ---------- Main orchestrator ----------

    def upload_all_csvs(self):
        logger.info(f"📂 Scanning folder: {self.download_path}")
        for file_name in os.listdir(self.download_path):
            if not file_name.lower().endswith(".csv"):
                continue

            table_name = os.path.splitext(file_name)[0].lower()
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            file_path = os.path.join(self.download_path, file_name)

            try:
                self.upload_csv(file_path, table_id)
                logger.info(f"✅ Finished processing: {file_name}")
            except Exception as e:
                logger.error(f"❌ Failed to process {file_name}: {e}")
                logger.error(traceback.format_exc())
            finally:
                try:
                    os.remove(file_path)
                    logger.info(f"🗑️ Deleted file: {file_name}")
                except Exception as e:
                    logger.error(f"❌ Could not delete {file_name}: {e}")

    # ---------- Upload strategies ----------

    def upload_csv(self, file_path: str, table_id: str):
        logger.info(f"⬆️ Uploading: {file_path} → {table_id}")
        ingestion_date = date.today()

        if self.table_exists(table_id):
            # Existing table: use staging + SAFE_CAST to target schema
            self._append_via_staging_with_casts(file_path, table_id)
            # If the table already has Ingestion_date, keep your post-update
            self._set_ingestion_date_if_exists(table_id, ingestion_date)
        else:
            # New table: create directly with autodetect
            self._create_table_and_load(file_path, table_id)
            # Add Ingestion_date if the column exists
            self._set_ingestion_date_if_exists(table_id, ingestion_date)

    def _create_table_and_load(self, file_path: str, table_id: str):
        logger.info(f"🆕 Table doesn't exist. Creating table by loading with autodetect: {table_id}")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(file_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
        logger.info(f"📘 Table created & data loaded: {table_id}")

        # Ensure Ingestion_date column exists; if not, create it (nullable)
        self._ensure_ingestion_date_column(table_id)

    def _append_via_staging_with_casts(self, file_path: str, target_table_id: str):
        """Load CSV into a staging table (autodetect), then insert into target with SAFE_CASTs to the target schema."""
        staging_table_id = f"{target_table_id}__stg_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        logger.info(f"📥 Loading into staging table: {staging_table_id}")

        load_cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(file_path, "rb") as source_file:
            self.client.load_table_from_file(source_file, staging_table_id, job_config=load_cfg).result()

        # Read schemas
        tgt_table = self.get_table(target_table_id)
        stg_table = self.get_table(staging_table_id)

        tgt_cols = [f.name for f in tgt_table.schema]
        tgt_types = {f.name: f.field_type for f in tgt_table.schema}
        stg_cols_set = {f.name for f in stg_table.schema}

        # Build ordered select list for target columns only
        select_exprs = []
        for col in tgt_cols:
            tgt_type = tgt_types[col]
            if col in stg_cols_set:
                select_exprs.append(_safe_cast_expr("stg", col, tgt_type))
            else:
                # Column missing in CSV → insert NULL
                select_exprs.append(f"CAST(NULL AS {_CAST_TARGETS.get(tgt_type.upper(), 'STRING')}) AS {col}")

        select_sql = ",\n            ".join(select_exprs)

        insert_sql = f"""
        INSERT INTO `{target_table_id}` ({", ".join(tgt_cols)})
        SELECT
            {select_sql}
        FROM `{staging_table_id}` AS stg
        """
        logger.info("🔄 Inserting from staging into target with SAFE_CASTs…")
        self.client.query(insert_sql).result()

        # Clean up staging
        self.client.delete_table(staging_table_id, not_found_ok=True)
        logger.info(f"🗑️ Staging table dropped: {staging_table_id}")

    # ---------- Ingestion_date helpers ----------

    def _ensure_ingestion_date_column(self, table_id: str):
        """Add Ingestion_date DATE column if it's not already present."""
        table = self.get_table(table_id)
        if any(f.name == "Ingestion_date" for f in table.schema):
            return
        new_schema = list(table.schema) + [bigquery.SchemaField("Ingestion_date", "DATE")]
        table.schema = new_schema
        self.client.update_table(table, ["schema"])
        logger.info(f"🧩 Added Ingestion_date column to: {table_id}")

    def _set_ingestion_date_if_exists(self, table_id: str, date_value: date):
        """Set Ingestion_date if that column exists."""
        table = self.get_table(table_id)
        if not any(f.name == "Ingestion_date" for f in table.schema):
            return
        query = f"""
        UPDATE `{table_id}`
        SET Ingestion_date = DATE('{date_value}')
        WHERE Ingestion_date IS NULL
        """
        self.client.query(query).result()
        logger.info(f"🗓️ Ingestion_date set for rows in: {table_id}")


# ---------- Entry Point ----------
if __name__ == "__main__":
    logger.info("🚀 Starting BigQuery upload process...")

    download_path = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(download_path, "wholesaling-data-warehouse-cd2929689ac2.json")

    uploader = BigQueryUploader(
        project_id=Config.PROJECT_ID,
        dataset_id=Config.DATASET_ID,
        download_path=download_path,
        credentials_path=credentials_path
    )

    uploader.upload_all_csvs()
    logger.info("✅ All CSVs processed.")