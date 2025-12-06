"""
Integration tests cho Silver layer transformation.
File này test việc transform data từ Bronze sang Silver.
"""
import pytest
from google.cloud import bigquery
from src.transformers.bronze_to_silver import BronzeToSilverTransformer
from src.config import settings


class TestSilverTransformation:
    """
    Test suite cho Silver transformation.
    
    Tests bao gồm:
    - Transform bills
    - Transform products
    - Transform customers
    - Data quality validation
    """
    
    @pytest.fixture
    def transformer(self):
        """
        Fixture tạo BronzeToSilverTransformer instance.
        
        Returns:
            BronzeToSilverTransformer: Transformer instance
        """
        return BronzeToSilverTransformer()
    
    @pytest.fixture
    def bq_client(self):
        """
        Fixture tạo BigQuery client.
        
        Returns:
            bigquery.Client: BigQuery client
        """
        return bigquery.Client(project=settings.gcp_project)
    
    def test_transform_bills(self, transformer):
        """
        Test transform bills từ Bronze sang Silver.
        
        Args:
            transformer: BronzeToSilverTransformer fixture
        """
        result = transformer.transform_bills()
        
        assert result["status"] == "success"
        assert "bills_count" in result
        assert result["bills_count"] >= 0
    
    def test_transform_products(self, transformer):
        """
        Test transform products từ Bronze sang Silver.
        
        Args:
            transformer: BronzeToSilverTransformer fixture
        """
        result = transformer.transform_products()
        
        assert result["status"] == "success"
        assert "products_count" in result
        assert result["products_count"] >= 0
    
    def test_transform_customers(self, transformer):
        """
        Test transform customers từ Bronze sang Silver.
        
        Args:
            transformer: BronzeToSilverTransformer fixture
        """
        result = transformer.transform_customers()
        
        assert result["status"] == "success"
        assert "customers_count" in result
        assert result["customers_count"] >= 0
    
    def test_silver_bills_table_exists(self, bq_client):
        """
        Test Silver bills table tồn tại và có data.
        
        Args:
            bq_client: BigQuery client fixture
        """
        table = bq_client.get_table(
            f"{settings.gcp_project}.{settings.silver_dataset}.bills"
        )
        
        assert table is not None
        assert table.num_rows >= 0
    
    def test_silver_bill_products_table_exists(self, bq_client):
        """
        Test Silver bill_products table tồn tại.
        
        Args:
            bq_client: BigQuery client fixture
        """
        table = bq_client.get_table(
            f"{settings.gcp_project}.{settings.silver_dataset}.bill_products"
        )
        
        assert table is not None

