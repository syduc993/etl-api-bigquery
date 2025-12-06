"""
Unit tests cho transformers module.
File này test BronzeToSilverTransformer và SilverToGoldTransformer với mocked BigQuery.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
import os


class TestBronzeToSilverTransformer:
    """Test suite cho BronzeToSilverTransformer."""
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_init(self, mock_settings, mock_bigquery):
        """Test __init__ tạo BigQuery client và set datasets."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        mock_client = MagicMock()
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        
        transformer = BronzeToSilverTransformer()
        
        assert transformer.bronze_dataset == 'bronze'
        assert transformer.silver_dataset == 'silver'
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_load_sql_file_success(self, mock_settings, mock_bigquery):
        """Test _load_sql_file đọc file SQL thành công."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        mock_client = MagicMock()
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        
        transformer = BronzeToSilverTransformer()
        
        sql_content = "SELECT * FROM bills"
        
        with patch('builtins.open', mock_open(read_data=sql_content)):
            result = transformer._load_sql_file('test.sql')
        
        assert result == sql_content
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_load_sql_file_not_found(self, mock_settings, mock_bigquery):
        """Test _load_sql_file raise exception khi file không tồn tại."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        mock_client = MagicMock()
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        
        transformer = BronzeToSilverTransformer()
        
        with patch('builtins.open', side_effect=FileNotFoundError):
            with pytest.raises(FileNotFoundError):
                transformer._load_sql_file('nonexistent.sql')
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_transform_bills_success(self, mock_settings, mock_bigquery):
        """Test transform_bills thành công."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        # Mock BigQuery client
        mock_table = MagicMock()
        mock_table.num_rows = 1000
        
        mock_job = MagicMock()
        mock_job.result.return_value = None
        
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client.get_table.return_value = mock_table
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        
        transformer = BronzeToSilverTransformer()
        
        sql_content = "CREATE OR REPLACE TABLE silver.bills AS SELECT * FROM bronze.bills_raw"
        
        with patch('builtins.open', mock_open(read_data=sql_content)):
            result = transformer.transform_bills()
        
        assert result['status'] == 'success'
        assert 'bills_count' in result
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_transform_products_success(self, mock_settings, mock_bigquery):
        """Test transform_products thành công."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        mock_table = MagicMock()
        mock_table.num_rows = 500
        
        mock_job = MagicMock()
        mock_job.result.return_value = None
        
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client.get_table.return_value = mock_table
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        
        transformer = BronzeToSilverTransformer()
        
        sql_content = "CREATE OR REPLACE TABLE silver.products AS SELECT * FROM bronze.products_raw"
        
        with patch('builtins.open', mock_open(read_data=sql_content)):
            result = transformer.transform_products()
        
        assert result['status'] == 'success'
        assert result['products_count'] == 500
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_transform_all_success(self, mock_settings, mock_bigquery):
        """Test transform_all chạy tất cả transformations."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        mock_table = MagicMock()
        mock_table.num_rows = 100
        
        mock_job = MagicMock()
        mock_job.result.return_value = None
        
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_client.get_table.return_value = mock_table
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        
        transformer = BronzeToSilverTransformer()
        
        sql_content = "SELECT 1"
        
        with patch('builtins.open', mock_open(read_data=sql_content)):
            results = transformer.transform_all()
        
        # Should have results for all entities
        assert 'bills' in results
        assert 'products' in results
        assert 'customers' in results
    
    @patch('src.transformers.bronze_to_silver.bigquery')
    @patch('src.transformers.bronze_to_silver.settings')
    def test_transform_handles_error(self, mock_settings, mock_bigquery):
        """Test transform xử lý error gracefully."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        mock_settings.silver_dataset = 'silver'
        
        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("Query failed")
        
        mock_client = MagicMock()
        mock_client.query.return_value = mock_job
        mock_bigquery.Client.return_value = mock_client
        
        from src.transformers.bronze_to_silver import BronzeToSilverTransformer
        from src.utils.exceptions import DataValidationError
        
        transformer = BronzeToSilverTransformer()
        
        sql_content = "SELECT 1"
        
        with patch('builtins.open', mock_open(read_data=sql_content)):
            with pytest.raises(DataValidationError):
                transformer.transform_bills()
