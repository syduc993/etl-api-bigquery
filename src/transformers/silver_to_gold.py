"""
Transformer từ Silver sang Gold layer.
File này xử lý việc tạo các business aggregates và curated data
từ Silver layer bằng cách chạy SQL aggregations trong BigQuery.
"""
from typing import Dict, Optional
from google.cloud import bigquery
from src.config import settings
from src.utils.logging import get_logger
from src.utils.exceptions import DataValidationError

logger = get_logger(__name__)


class SilverToGoldTransformer:
    """
    Transformer để chuyển data từ Silver sang Gold layer.
    
    Transformer này chạy SQL scripts để tạo:
    - Daily revenue summary
    - Customer lifetime value
    - Product sales metrics
    - Inventory analytics
    - Materialized views cho performance
    """
    
    def __init__(self):
        """
        Khởi tạo transformer.
        
        Tự động tạo BigQuery client và load SQL scripts.
        """
        self.client = bigquery.Client(project=settings.gcp_project)
        self.silver_dataset = settings.silver_dataset
        self.gold_dataset = settings.gold_dataset
    
    def _load_sql_file(self, file_path: str) -> str:
        """
        Load SQL file content.
        
        Args:
            file_path: Đường dẫn đến SQL file
            
        Returns:
            str: SQL query content
            
        Raises:
            Exception: Nếu không thể đọc file
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error loading SQL file {file_path}", error=str(e))
            raise
    
    def create_daily_revenue_summary(self) -> dict:
        """
        Tạo daily revenue summary từ Silver layer.
        
        Returns:
            dict: Kết quả với số records và status
        """
        logger.info("Creating daily revenue summary")
        
        try:
            sql = self._load_sql_file("sql/gold/daily_revenue_summary.sql")
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            query_job = self.client.query(sql)
            query_job.result()
            
            table = self.client.get_table(
                f"{settings.gcp_project}.{self.gold_dataset}.daily_revenue_summary"
            )
            
            result = {
                "records_count": table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Daily revenue summary created",
                records_count=result["records_count"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error creating daily revenue summary", error=str(e))
            raise DataValidationError(f"Daily revenue summary failed: {str(e)}")
    
    def create_customer_lifetime_value(self) -> dict:
        """
        Tạo customer lifetime value từ Silver layer.
        
        Returns:
            dict: Kết quả với số records và status
        """
        logger.info("Creating customer lifetime value")
        
        try:
            sql = self._load_sql_file("sql/gold/customer_lifetime_value.sql")
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            query_job = self.client.query(sql)
            query_job.result()
            
            table = self.client.get_table(
                f"{settings.gcp_project}.{self.gold_dataset}.customer_lifetime_value"
            )
            
            result = {
                "records_count": table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Customer lifetime value created",
                records_count=result["records_count"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error creating customer lifetime value", error=str(e))
            raise DataValidationError(f"Customer lifetime value failed: {str(e)}")
    
    def create_product_sales_metrics(self) -> dict:
        """
        Tạo product sales metrics từ Silver layer.
        
        Returns:
            dict: Kết quả với số records và status
        """
        logger.info("Creating product sales metrics")
        
        try:
            sql = self._load_sql_file("sql/gold/product_sales_metrics.sql")
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            query_job = self.client.query(sql)
            query_job.result()
            
            metrics_table = self.client.get_table(
                f"{settings.gcp_project}.{self.gold_dataset}.product_sales_metrics"
            )
            summary_table = self.client.get_table(
                f"{settings.gcp_project}.{self.gold_dataset}.product_summary"
            )
            
            result = {
                "metrics_records": metrics_table.num_rows,
                "summary_records": summary_table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Product sales metrics created",
                metrics_records=result["metrics_records"],
                summary_records=result["summary_records"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error creating product sales metrics", error=str(e))
            raise DataValidationError(f"Product sales metrics failed: {str(e)}")
    
    def create_inventory_analytics(self) -> dict:
        """
        Tạo inventory analytics từ Silver layer.
        
        Returns:
            dict: Kết quả với số records và status
        """
        logger.info("Creating inventory analytics")
        
        try:
            sql = self._load_sql_file("sql/gold/inventory_analytics.sql")
            sql = sql.replace("sync-nhanhvn-project", settings.gcp_project)
            
            query_job = self.client.query(sql)
            query_job.result()
            
            table = self.client.get_table(
                f"{settings.gcp_project}.{self.gold_dataset}.inventory_analytics"
            )
            
            result = {
                "records_count": table.num_rows,
                "status": "success"
            }
            
            logger.info(
                "Inventory analytics created",
                records_count=result["records_count"]
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error creating inventory analytics", error=str(e))
            raise DataValidationError(f"Inventory analytics failed: {str(e)}")
    
    def transform_all(self) -> dict:
        """
        Tạo tất cả Gold layer tables.
        
        Returns:
            dict: Kết quả transformation cho tất cả tables
        """
        logger.info("Starting full Gold layer transformation")
        
        results = {}
        
        try:
            results["daily_revenue_summary"] = self.create_daily_revenue_summary()
        except Exception as e:
            logger.error(f"Error creating daily revenue summary", error=str(e))
            results["daily_revenue_summary"] = {"status": "error", "error": str(e)}
        
        try:
            results["customer_lifetime_value"] = self.create_customer_lifetime_value()
        except Exception as e:
            logger.error(f"Error creating customer lifetime value", error=str(e))
            results["customer_lifetime_value"] = {"status": "error", "error": str(e)}
        
        try:
            results["product_sales_metrics"] = self.create_product_sales_metrics()
        except Exception as e:
            logger.error(f"Error creating product sales metrics", error=str(e))
            results["product_sales_metrics"] = {"status": "error", "error": str(e)}
        
        try:
            results["inventory_analytics"] = self.create_inventory_analytics()
        except Exception as e:
            logger.error(f"Error creating inventory analytics", error=str(e))
            results["inventory_analytics"] = {"status": "error", "error": str(e)}
        
        logger.info("Completed full Gold layer transformation", results=results)
        
        return results

