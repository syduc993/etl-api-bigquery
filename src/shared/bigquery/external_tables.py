"""
Utility để tự động setup BigQuery External Tables sau khi upload data lên GCS.
File này tạo/cập nhật External Tables pointing đến GCS files.
"""
from typing import List, Optional
from google.cloud import bigquery
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


class BigQueryExternalTableSetup:
    """
    Utility class để setup BigQuery External Tables.
    """
    
    def __init__(self):
        """Khởi tạo BigQuery client."""
        self.client = bigquery.Client(project=settings.gcp_project)
        self.bronze_dataset = settings.bronze_dataset
        self.bronze_bucket = settings.bronze_bucket
        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self):
        """Ensure the Bronze dataset exists."""
        dataset_id = f"{settings.gcp_project}.{self.bronze_dataset}"
        try:
            self.client.get_dataset(dataset_id)
        except Exception:
            logger.info(f"Dataset {dataset_id} not found, creating...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = settings.gcp_region
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_id}")
    
    def setup_external_table(
        self,
        platform: str,
        entity: str,
        table_name: Optional[str] = None
    ) -> str:
        """
        Tạo hoặc cập nhật External Table cho một entity.
        
        Args:
            platform: Tên platform (ví dụ: 'nhanh')
            entity: Tên entity (ví dụ: 'bills', 'bill_products')
            table_name: Tên table trong BigQuery (mặc định: {platform}_{entity}_raw)
            
        Returns:
            str: Full table ID đã tạo/cập nhật
        """
        if not table_name:
            table_name = f"{platform}_{entity}_raw"
        
        table_id = f"{settings.gcp_project}.{self.bronze_dataset}.{table_name}"
        gcs_uri = f"gs://{self.bronze_bucket}/{platform}/{entity}/*.parquet"
        
        sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{table_id}`
        OPTIONS (
          format = 'PARQUET',
          uris = ['{gcs_uri}']
        )
        """
        
        try:
            query_job = self.client.query(sql)
            query_job.result()
            
            logger.info(
                f"External table created/updated",
                table_id=table_id,
                gcs_uri=gcs_uri,
                platform=platform,
                entity=entity
            )
            return table_id
        except Exception as e:
            logger.error(
                f"Failed to create/update external table",
                table_id=table_id,
                error=str(e),
                platform=platform,
                entity=entity
            )
            raise
    
    def setup_all_tables(self, platforms: List[str] = None):
        """
        Setup tất cả External Tables cho các platforms và entities.
        
        Args:
            platforms: Danh sách platforms (None = chỉ nhanh)
        """
        if platforms is None:
            platforms = ["nhanh"]
        
        # Define entities for each platform
        platform_entities = {
            "nhanh": ["bills", "bill_products", "products", "customers"]
        }
        
        for platform in platforms:
            entities = platform_entities.get(platform, [])
            for entity in entities:
                try:
                    self.setup_external_table(platform, entity)
                except Exception as e:
                    logger.warning(
                        f"Failed to setup table for {platform}.{entity}",
                        error=str(e),
                        platform=platform,
                        entity=entity
                    )
                    continue
        
        logger.info("Completed setting up all external tables")
