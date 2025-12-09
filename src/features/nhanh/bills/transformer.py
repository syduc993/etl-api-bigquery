"""
Transformer cho Bills feature.
Transform data từ Raw (External) -> Clean (Flattened).
"""
import os
from pathlib import Path
from typing import Dict, Any, List
from src.shared.bigquery import BigQueryClient
from src.shared.logging import get_logger
from src.config import settings

logger = get_logger(__name__)


class BillTransformer:
    """
    Transformer cho bills data.
    Chạy SQL transformations trong BigQuery để flatten data.
    """
    
    def __init__(self):
        """Khởi tạo transformer với BigQuery client."""
        self.bq_client = BigQueryClient()
        self.sql_dir = Path(__file__).parent / "sql"
        self.sql_path = self.sql_dir # Alias for compatibility
    
    def _get_template_params(self) -> Dict[str, str]:
        """Lấy parameters cho SQL template."""
        return {
            "project_id": settings.gcp_project,
            "dataset": settings.target_dataset,
            "dataset_raw": settings.bronze_dataset
        }

    def transform_flatten(self) -> Dict[str, Any]:
        """
        Flatten data from Raw (External Table) to Clean Tables.
        Executes sql/query_flatten.sql
        """
        logger.info("Starting Flatten transformation...")
        
        try:
            # Ensure Schema Exists
            self.bq_client.execute_script_from_file(
                script_path=self.sql_dir / "schema_clean.sql",
                params=self._get_template_params()
            )
            
            # Execute Flatten Query
            job_id = self.bq_client.execute_script_from_file(
                script_path=self.sql_dir / "query_flatten.sql",
                params=self._get_template_params()
            )
            
            logger.info(f"Flatten transformation completed. Job ID: {job_id}")
            return {"status": "success", "job_id": job_id}
            
        except Exception as e:
            logger.error(f"Flatten transformation failed: {e}")
            raise e
