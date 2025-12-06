"""
Transformer từ Bronze sang Silver layer.
File này xử lý việc transform data từ Bronze (raw JSON) sang Silver (cleaned data)
bằng cách chạy SQL transformations trong BigQuery.
"""
from typing import List, Optional
from google.cloud import bigquery
from src.config import settings
from src.utils.logging import get_logger
from src.utils.exceptions import DataValidationError

logger = get_logger(__name__)


class BronzeToSilverTransformer:
    """
    Transformer để chuyển data từ Bronze sang Silver layer.
    
    Transformer này chạy SQL scripts để:
    - Type casting
    - Deduplication
    - Flatten nested JSON
    - Data validation
    """
    
    def __init__(self):
        """
        Khởi tạo transformer.
        
        Tự động tạo BigQuery client và load SQL scripts.
        """
        self.client = bigquery.Client(project=settings.gcp_project)
        self.bronze_dataset = settings.bronze_dataset
        self.silver_dataset = settings.silver_dataset
    
    def _load_sql_file(self, file_path: str) -> str:
        """
        Load SQL file content.
        
        Args:
            file_path: Đường dẫn đến SQL file
            
        Returns:
            str: SQL query content
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error loading SQL file {file_path}", error=str(e))
            raise
    
    def transform_bills(self, incremental: bool = True) -> dict:
        """
        Transform bills từ Bronze sang Silver.
        
        Args:
            incremental: Có chạy incremental hay không (hiện tại luôn full)
            
        Returns:
            dict: Kết quả transformation với số records processed
        """
        logger.info("Starting bills transformation: Bronze → Silver")
        
        try:
            # Load SQL script
            sql = self._load_sql_file("sql/silver/transform_bills.sql")
            
            # Replace project ID nếu cần
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            # Execute query
            query_job = self.client.query(sql)
            query_job.result()  # Wait for completion
            
            # Get table stats
            bills_table = self.client.get_table(
                f"{settings.gcp_project}.{self.silver_dataset}.bills"
            )
            bill_products_table = self.client.get_table(
                f"{settings.gcp_project}.{self.silver_dataset}.bill_products"
            )
            
            result = {
                "bills_count": bills_table.num_rows,
                "bill_products_count": bill_products_table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Completed bills transformation",
                bills_count=result["bills_count"],
                bill_products_count=result["bill_products_count"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error transforming bills", error=str(e))
            raise DataValidationError(f"Bills transformation failed: {str(e)}")
    
    def transform_products(self, incremental: bool = True) -> dict:
        """
        Transform products từ Bronze sang Silver.
        
        Args:
            incremental: Có chạy incremental hay không (hiện tại luôn full)
            
        Returns:
            dict: Kết quả transformation với số records processed
        """
        logger.info("Starting products transformation: Bronze → Silver")
        
        try:
            # Load SQL script
            sql = self._load_sql_file("sql/silver/transform_products.sql")
            
            # Replace project ID nếu cần
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            # Execute query
            query_job = self.client.query(sql)
            query_job.result()  # Wait for completion
            
            # Get table stats
            products_table = self.client.get_table(
                f"{settings.gcp_project}.{self.silver_dataset}.products"
            )
            
            result = {
                "products_count": products_table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Completed products transformation",
                products_count=result["products_count"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error transforming products", error=str(e))
            raise DataValidationError(f"Products transformation failed: {str(e)}")
    
    def transform_customers(self, incremental: bool = True) -> dict:
        """
        Transform customers từ Bronze sang Silver.
        
        Args:
            incremental: Có chạy incremental hay không (hiện tại luôn full)
            
        Returns:
            dict: Kết quả transformation với số records processed
        """
        logger.info("Starting customers transformation: Bronze → Silver")
        
        try:
            # Load SQL script
            sql = self._load_sql_file("sql/silver/transform_customers.sql")
            
            # Replace project ID nếu cần
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            # Execute query
            query_job = self.client.query(sql)
            query_job.result()  # Wait for completion
            
            # Get table stats
            customers_table = self.client.get_table(
                f"{settings.gcp_project}.{self.silver_dataset}.customers"
            )
            
            result = {
                "customers_count": customers_table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Completed customers transformation",
                customers_count=result["customers_count"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error transforming customers", error=str(e))
            raise DataValidationError(f"Customers transformation failed: {str(e)}")
    
    def transform_all(self) -> dict:
        """
        Transform tất cả entities từ Bronze sang Silver.
        
        Returns:
            dict: Kết quả transformation cho tất cả entities
        """
        logger.info("Starting full transformation: Bronze → Silver")
        
        results = {}
        
        try:
            results["bills"] = self.transform_bills()
        except Exception as e:
            logger.error(f"Error transforming bills", error=str(e))
            results["bills"] = {"status": "error", "error": str(e)}
        
        try:
            results["products"] = self.transform_products()
        except Exception as e:
            logger.error(f"Error transforming products", error=str(e))
            results["products"] = {"status": "error", "error": str(e)}
        
        try:
            results["customers"] = self.transform_customers()
        except Exception as e:
            logger.error(f"Error transforming customers", error=str(e))
            results["customers"] = {"status": "error", "error": str(e)}
        
        logger.info("Completed full transformation", results=results)
        
        return results

