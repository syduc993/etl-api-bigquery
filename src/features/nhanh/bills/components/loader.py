"""
Loader cho Bills feature.
Upload data lên GCS và setup BigQuery external tables.
Sử dụng shared utilities nhưng logic nằm trong feature.
"""
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from src.shared.gcs import GCSLoader
from src.shared.bigquery import BigQueryExternalTableSetup
from src.shared.logging import get_logger
from src.config import settings

logger = get_logger(__name__)


class BillLoader:
    """
    Loader cho bills data.
    - Upload bills/bill_products lên GCS (Parquet format)
    - Setup BigQuery external tables
    """
    
    def __init__(self):
        """Khởi tạo loader với GCS và BigQuery clients."""
        self.gcs_loader = GCSLoader(bucket_name=settings.bronze_bucket)
        self.bq_setup = BigQueryExternalTableSetup()
        self.platform = "nhanh"
        self.entity = "bills"
    
    def load_bills(
        self,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Upload bills data lên GCS.
        
        Args:
            data: Danh sách bills
            partition_date: Ngày partition
            metadata: Metadata bổ sung
            
        Returns:
            str: GCS path của file đã upload
        """
        if not data:
            logger.warning("No bills data to load")
            return ""
        
        entity_path = f"{self.platform}/{self.entity}"
        
        upload_metadata = {
            "platform": self.platform,
            "entity": self.entity,
            "extraction_timestamp": datetime.utcnow().isoformat(),
            "record_count": len(data),
            **(metadata or {})
        }
        
        gcs_path = self.gcs_loader.upload_parquet_by_date(
            entity=entity_path,
            data=data,
            partition_date=partition_date,
            metadata=upload_metadata,
            overwrite_partition=True
        )
        
        logger.info(
            f"Loaded {len(data)} bills to GCS",
            path=gcs_path,
            records=len(data)
        )
        
        return gcs_path
    
    def load_bill_products(
        self,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Upload bill_products data lên GCS."""
        if not data:
            logger.warning("No bill_products data to load")
            return ""
        
        entity_path = f"{self.platform}/bill_products"
        
        upload_metadata = {
            "platform": self.platform,
            "entity": "bill_products",
            "extraction_timestamp": datetime.utcnow().isoformat(),
            "record_count": len(data),
            **(metadata or {})
        }
        
        gcs_path = self.gcs_loader.upload_parquet_by_date(
            entity=entity_path,
            data=data,
            partition_date=partition_date,
            metadata=upload_metadata,
            overwrite_partition=True
        )
        
        logger.info(
            f"Loaded {len(data)} bill products to GCS",
            path=gcs_path,
            records=len(data)
        )
        
        return gcs_path
    
    def setup_external_tables(self) -> Dict[str, str]:
        """Setup BigQuery external tables cho bills và bill_products."""
        tables = {}
        
        # Bills table
        tables["bills"] = self.bq_setup.setup_external_table(
            platform=self.platform,
            entity=self.entity
        )
        
        # Bill products table
        tables["bill_products"] = self.bq_setup.setup_external_table(
            platform=self.platform,
            entity="bill_products"
        )
        
        logger.info("Setup external tables for bills", tables=tables)
        return tables
