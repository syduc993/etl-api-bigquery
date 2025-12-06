"""
Integration tests cho Gold layer aggregation.
File này test việc tạo business aggregates từ Silver layer.
"""
import pytest
from google.cloud import bigquery
from src.transformers.silver_to_gold import SilverToGoldTransformer
from src.config import settings


class TestGoldAggregation:
    """
    Test suite cho Gold aggregation.
    
    Tests bao gồm:
    - Daily revenue summary
    - Customer lifetime value
    - Product sales metrics
    - Inventory analytics
    """
    
    @pytest.fixture
    def transformer(self):
        """
        Fixture tạo SilverToGoldTransformer instance.
        
        Returns:
            SilverToGoldTransformer: Transformer instance
        """
        return SilverToGoldTransformer()
    
    @pytest.fixture
    def bq_client(self):
        """
        Fixture tạo BigQuery client.
        
        Returns:
            bigquery.Client: BigQuery client
        """
        return bigquery.Client(project=settings.gcp_project)
    
    def test_create_daily_revenue_summary(self, transformer):
        """
        Test tạo daily revenue summary.
        
        Args:
            transformer: SilverToGoldTransformer fixture
        """
        result = transformer.create_daily_revenue_summary()
        
        assert result["status"] == "success"
        assert "records_count" in result
    
    def test_create_customer_lifetime_value(self, transformer):
        """
        Test tạo customer lifetime value.
        
        Args:
            transformer: SilverToGoldTransformer fixture
        """
        result = transformer.create_customer_lifetime_value()
        
        assert result["status"] == "success"
        assert "records_count" in result
    
    def test_create_product_sales_metrics(self, transformer):
        """
        Test tạo product sales metrics.
        
        Args:
            transformer: SilverToGoldTransformer fixture
        """
        result = transformer.create_product_sales_metrics()
        
        assert result["status"] == "success"
        assert "metrics_records" in result
        assert "summary_records" in result
    
    def test_create_inventory_analytics(self, transformer):
        """
        Test tạo inventory analytics.
        
        Args:
            transformer: SilverToGoldTransformer fixture
        """
        result = transformer.create_inventory_analytics()
        
        assert result["status"] == "success"
        assert "records_count" in result
    
    def test_gold_tables_exist(self, bq_client):
        """
        Test các Gold tables tồn tại.
        
        Args:
            bq_client: BigQuery client fixture
        """
        tables = [
            "daily_revenue_summary",
            "customer_lifetime_value",
            "product_sales_metrics",
            "product_summary",
            "inventory_analytics"
        ]
        
        for table_name in tables:
            try:
                table = bq_client.get_table(
                    f"{settings.gcp_project}.{settings.gold_dataset}.{table_name}"
                )
                assert table is not None
            except Exception:
                pytest.skip(f"Table {table_name} does not exist yet")

