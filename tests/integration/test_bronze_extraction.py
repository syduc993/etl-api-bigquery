"""
Integration tests cho Bronze layer extraction.
File này test việc extract data từ Nhanh API và upload lên GCS.
"""
import pytest
from datetime import datetime, timedelta
from src.extractors.nhanh.bill import BillExtractor
from src.extractors.nhanh.product import ProductExtractor
from src.extractors.nhanh.customer import CustomerExtractor
from src.shared.gcs import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.config import settings


class TestBronzeExtraction:
    """
    Test suite cho Bronze extraction.
    
    Tests bao gồm:
    - Extract bills với date range
    - Extract products
    - Extract customers
    - Upload lên GCS
    - Watermark tracking
    """
    
    @pytest.fixture
    def bill_extractor(self):
        """
        Fixture tạo BillExtractor instance.
        
        Returns:
            BillExtractor: Extractor instance
        """
        return BillExtractor()
    
    @pytest.fixture
    def product_extractor(self):
        """
        Fixture tạo ProductExtractor instance.
        
        Returns:
            ProductExtractor: Extractor instance
        """
        return ProductExtractor()
    
    @pytest.fixture
    def customer_extractor(self):
        """
        Fixture tạo CustomerExtractor instance.
        
        Returns:
            CustomerExtractor: Extractor instance
        """
        return CustomerExtractor()
    
    @pytest.fixture
    def gcs_loader(self):
        """
        Fixture tạo GCSLoader instance.
        
        Returns:
            GCSLoader: Loader instance
        """
        return GCSLoader(bucket_name=settings.bronze_bucket)
    
    @pytest.fixture
    def watermark_tracker(self):
        """
        Fixture tạo WatermarkTracker instance.
        
        Returns:
            WatermarkTracker: Tracker instance
        """
        return WatermarkTracker()
    
    def test_extract_bills_small_range(self, bill_extractor):
        """
        Test extract bills với date range nhỏ (7 ngày).
        
        Args:
            bill_extractor: BillExtractor fixture
        """
        to_date = datetime.now()
        from_date = to_date - timedelta(days=7)
        
        bills = bill_extractor.fetch_bills(
            from_date=from_date,
            to_date=to_date
        )
        
        assert isinstance(bills, list)
        # Nếu có data, kiểm tra structure
        if bills:
            assert "id" in bills[0]
            assert "date" in bills[0]
    
    def test_extract_bills_large_range(self, bill_extractor):
        """
        Test extract bills với date range lớn (60 ngày) - phải tự động chia.
        
        Args:
            bill_extractor: BillExtractor fixture
        """
        to_date = datetime.now()
        from_date = to_date - timedelta(days=60)
        
        bills = bill_extractor.fetch_bills(
            from_date=from_date,
            to_date=to_date
        )
        
        assert isinstance(bills, list)
        # Nên có data nếu có bills trong range này
    
    def test_extract_products(self, product_extractor):
        """
        Test extract products.
        
        Args:
            product_extractor: ProductExtractor fixture
        """
        products = product_extractor.fetch_products(incremental=False)
        
        assert isinstance(products, list)
        if products:
            assert "id" in products[0]
            assert "name" in products[0]
    
    def test_extract_customers(self, customer_extractor):
        """
        Test extract customers.
        
        Args:
            customer_extractor: CustomerExtractor fixture
        """
        customers = customer_extractor.fetch_customers(incremental=False)
        
        assert isinstance(customers, list)
        if customers:
            assert "id" in customers[0]
            assert "name" in customers[0]
    
    def test_upload_to_gcs(self, gcs_loader):
        """
        Test upload data lên GCS.
        
        Args:
            gcs_loader: GCSLoader fixture
        """
        test_data = [
            {"id": 1, "name": "Test", "value": 100},
            {"id": 2, "name": "Test 2", "value": 200}
        ]
        
        path = gcs_loader.upload_json(
            entity="test",
            data=test_data,
            metadata={"test": True}
        )
        
        assert path != ""
        assert "test/" in path
    
    def test_watermark_tracking(self, watermark_tracker):
        """
        Test watermark tracking.
        
        Args:
            watermark_tracker: WatermarkTracker fixture
        """
        # Test update watermark
        watermark_tracker.update_watermark(
            entity="test_entity",
            extracted_at=datetime.utcnow(),
            records_count=100
        )
        
        # Test get watermark
        watermark = watermark_tracker.get_watermark("test_entity")
        assert watermark is not None
        
        # Test get incremental range
        from_date, to_date = watermark_tracker.get_incremental_range("test_entity")
        assert from_date is not None
        assert to_date is not None

