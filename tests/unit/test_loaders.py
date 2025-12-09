"""
Unit tests cho loaders module.
File này test GCSLoader và WatermarkTracker với mocked GCS/BigQuery clients.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta


class TestGCSLoader:
    """Test suite cho GCSLoader."""
    
    @patch('src.shared.gcs.loader.storage')
    @patch('src.shared.gcs.loader.settings')
    def test_get_partition_path_month(self, mock_settings, mock_storage):
        """Test _get_partition_path với strategy='month'."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.partition_strategy = 'month'
        
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        
        from src.shared.gcs import GCSLoader
        
        loader = GCSLoader(bucket_name='test-bucket')
        
        timestamp = datetime(2024, 3, 15, 10, 30, 0)
        path = loader._get_partition_path('bills', timestamp)
        
        assert path == 'bills/year=2024/month=03/'
    
    @patch('src.shared.gcs.loader.storage')
    @patch('src.shared.gcs.loader.settings')
    def test_get_partition_path_day(self, mock_settings, mock_storage):
        """Test _get_partition_path với strategy='day'."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.partition_strategy = 'day'
        
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        
        from src.shared.gcs import GCSLoader
        
        loader = GCSLoader(bucket_name='test-bucket')
        
        timestamp = datetime(2024, 3, 15, 10, 30, 0)
        path = loader._get_partition_path('bills', timestamp, partition_strategy='day')
        
        assert path == 'bills/year=2024/month=03/day=15/'
    
    @patch('src.shared.gcs.loader.storage')
    @patch('src.shared.gcs.loader.settings')
    def test_upload_json_empty_data(self, mock_settings, mock_storage):
        """Test upload_json với empty data trả về empty string."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.partition_strategy = 'month'
        
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        
        from src.shared.gcs import GCSLoader
        
        loader = GCSLoader(bucket_name='test-bucket')
        
        result = loader.upload_json('bills', [])
        
        assert result == ''
    
    @patch('src.shared.gcs.loader.storage')
    @patch('src.shared.gcs.loader.settings')
    def test_upload_json_success(self, mock_settings, mock_storage):
        """Test upload_json với data thành công."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.partition_strategy = 'month'
        
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage.Client.return_value = mock_client
        
        from src.shared.gcs import GCSLoader
        
        loader = GCSLoader(bucket_name='test-bucket')
        
        data = [{'id': 1, 'name': 'Test'}]
        result = loader.upload_json('bills', data, compress=True)
        
        assert result != ''
        assert 'bills/' in result
        mock_blob.upload_from_string.assert_called_once()
    
    @patch('src.shared.gcs.loader.storage')
    @patch('src.shared.gcs.loader.settings')
    def test_upload_json_file_exists(self, mock_settings, mock_storage):
        """Test upload_json khi file đã tồn tại (idempotent)."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.partition_strategy = 'month'
        
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True  # File already exists
        
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage.Client.return_value = mock_client
        
        from src.shared.gcs import GCSLoader
        
        loader = GCSLoader(bucket_name='test-bucket')
        
        data = [{'id': 1, 'name': 'Test'}]
        result = loader.upload_json('bills', data)
        
        # Should return path without uploading
        assert result != ''
        mock_blob.upload_from_string.assert_not_called()


class TestWatermarkTracker:
    """Test suite cho WatermarkTracker."""
    
    @patch('src.loaders.watermark.bigquery')
    @patch('src.loaders.watermark.settings')
    def test_init_creates_client(self, mock_settings, mock_bigquery):
        """Test __init__ tạo BigQuery client."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        
        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("Table not found")
        mock_client.create_table.return_value = MagicMock()
        mock_bigquery.Client.return_value = mock_client
        
        from src.loaders.watermark import WatermarkTracker
        
        tracker = WatermarkTracker()
        
        assert tracker.dataset_id == 'bronze'
        assert tracker.table_id == 'extraction_watermarks'
    
    @patch('src.loaders.watermark.bigquery')
    @patch('src.loaders.watermark.settings')
    def test_get_watermark_exists(self, mock_settings, mock_bigquery):
        """Test get_watermark khi có watermark."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        
        expected_time = datetime(2024, 3, 15, 10, 0, 0)
        
        mock_row = MagicMock()
        mock_row.last_extracted_at = expected_time
        
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_result.__next__ = lambda self: mock_row
        
        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        
        mock_client = MagicMock()
        mock_client.get_table.return_value = MagicMock()  # Table exists
        mock_client.query.return_value = mock_job
        mock_bigquery.Client.return_value = mock_client
        
        from src.loaders.watermark import WatermarkTracker
        
        tracker = WatermarkTracker()
        result = tracker.get_watermark('bills')
        
        assert result == expected_time
    
    @patch('src.loaders.watermark.bigquery')
    @patch('src.loaders.watermark.settings')
    def test_get_watermark_not_exists(self, mock_settings, mock_bigquery):
        """Test get_watermark khi không có watermark."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        
        # Empty result
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        
        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        
        mock_client = MagicMock()
        mock_client.get_table.return_value = MagicMock()
        mock_client.query.return_value = mock_job
        mock_bigquery.Client.return_value = mock_client
        
        from src.loaders.watermark import WatermarkTracker
        
        tracker = WatermarkTracker()
        result = tracker.get_watermark('bills')
        
        assert result is None
    
    @patch('src.loaders.watermark.bigquery')
    @patch('src.loaders.watermark.settings')
    def test_get_incremental_range_with_watermark(self, mock_settings, mock_bigquery):
        """Test get_incremental_range khi có watermark."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        
        watermark_time = datetime(2024, 3, 15, 10, 0, 0)
        
        mock_row = MagicMock()
        mock_row.last_extracted_at = watermark_time
        
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([mock_row])
        mock_result.__next__ = lambda self: mock_row
        
        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        
        mock_client = MagicMock()
        mock_client.get_table.return_value = MagicMock()
        mock_client.query.return_value = mock_job
        mock_bigquery.Client.return_value = mock_client
        
        from src.loaders.watermark import WatermarkTracker
        
        tracker = WatermarkTracker()
        from_date, to_date = tracker.get_incremental_range('bills')
        
        assert from_date == watermark_time
        assert to_date is not None
        assert to_date > from_date
    
    @patch('src.loaders.watermark.bigquery')
    @patch('src.loaders.watermark.settings')
    def test_get_incremental_range_no_watermark(self, mock_settings, mock_bigquery):
        """Test get_incremental_range khi không có watermark (uses lookback)."""
        mock_settings.gcp_project = 'test-project'
        mock_settings.bronze_dataset = 'bronze'
        
        # Empty result - no watermark
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        
        mock_job = MagicMock()
        mock_job.result.return_value = mock_result
        
        mock_client = MagicMock()
        mock_client.get_table.return_value = MagicMock()
        mock_client.query.return_value = mock_job
        mock_bigquery.Client.return_value = mock_client
        
        from src.loaders.watermark import WatermarkTracker
        
        tracker = WatermarkTracker()
        from_date, to_date = tracker.get_incremental_range('bills', lookback_hours=2)
        
        # from_date should be approximately 2 hours ago
        assert from_date is not None
        assert to_date is not None
        
        # Difference should be approximately 2 hours
        delta = to_date - from_date
        assert 1.9 <= delta.total_seconds() / 3600 <= 2.1
