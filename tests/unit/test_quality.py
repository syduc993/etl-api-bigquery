"""
Unit tests cho quality module.
File này test validators (Pydantic models) và data quality checks.
"""
import pytest
from datetime import datetime


class TestBillRecord:
    """Test suite cho BillRecord Pydantic model."""
    
    def test_valid_bill(self):
        """Test BillRecord với valid data."""
        from src.quality.validators import BillRecord
        
        data = {
            'id': 12345,
            'date': '2024-03-15',
            'type': 1,
            'mode': 2,
            'depotId': 100
        }
        
        bill = BillRecord.model_validate(data)
        
        assert bill.id == 12345
        assert bill.date == '2024-03-15'
        assert bill.type == 1
    
    def test_bill_with_nested_objects(self):
        """Test BillRecord với nested customer và products."""
        from src.quality.validators import BillRecord
        
        data = {
            'id': 12345,
            'date': '2024-03-15',
            'customer': {'id': 1, 'name': 'Test Customer'},
            'products': [{'id': 1, 'name': 'Product A'}]
        }
        
        bill = BillRecord.model_validate(data)
        
        assert bill.customer == {'id': 1, 'name': 'Test Customer'}
        assert len(bill.products) == 1
    
    def test_bill_missing_required_field(self):
        """Test BillRecord raise ValidationError khi thiếu required field."""
        from src.quality.validators import BillRecord
        from pydantic import ValidationError
        
        data = {
            'date': '2024-03-15'
            # Missing 'id' - required field
        }
        
        with pytest.raises(ValidationError):
            BillRecord.model_validate(data)
    
    def test_bill_allows_extra_fields(self):
        """Test BillRecord cho phép extra fields."""
        from src.quality.validators import BillRecord
        
        data = {
            'id': 12345,
            'custom_field': 'custom_value'
        }
        
        bill = BillRecord.model_validate(data)
        
        assert bill.id == 12345


class TestProductRecord:
    """Test suite cho ProductRecord Pydantic model."""
    
    def test_valid_product(self):
        """Test ProductRecord với valid data."""
        from src.quality.validators import ProductRecord
        
        data = {
            'idNhanh': 12345,
            'name': 'Test Product',
            'code': 'PROD001',
            'price': 100000
        }
        
        product = ProductRecord.model_validate(data)
        
        assert product.id == 12345
        assert product.name == 'Test Product'
        assert product.price == 100000
    
    def test_product_id_coercion(self):
        """Test ProductRecord coerce string ID to int."""
        from src.quality.validators import ProductRecord
        
        data = {
            'idNhanh': '12345',  # String instead of int
            'name': 'Test Product'
        }
        
        product = ProductRecord.model_validate(data)
        
        assert product.id == 12345
        assert isinstance(product.id, int)


class TestCustomerRecord:
    """Test suite cho CustomerRecord Pydantic model."""
    
    def test_valid_customer(self):
        """Test CustomerRecord với valid data."""
        from src.quality.validators import CustomerRecord
        
        data = {
            'id': 12345,
            'name': 'Test Customer',
            'mobile': '0901234567',
            'email': 'test@example.com'
        }
        
        customer = CustomerRecord.model_validate(data)
        
        assert customer.id == 12345
        assert customer.name == 'Test Customer'
        assert customer.mobile == '0901234567'


class TestValidateRecords:
    """Test suite cho validate_records function."""
    
    def test_all_valid(self):
        """Test validate_records với tất cả records valid."""
        from src.quality.validators import validate_records, BillRecord
        
        records = [
            {'id': 1, 'date': '2024-01-01'},
            {'id': 2, 'date': '2024-01-02'},
            {'id': 3, 'date': '2024-01-03'}
        ]
        
        result = validate_records(records, BillRecord)
        
        assert result.total_records == 3
        assert result.valid_records == 3
        assert result.invalid_records == 0
        assert result.success_rate == 1.0
    
    def test_some_invalid(self):
        """Test validate_records với một số records invalid."""
        from src.quality.validators import validate_records, BillRecord
        
        records = [
            {'id': 1, 'date': '2024-01-01'},
            {'date': '2024-01-02'},  # Missing id
            {'id': 3, 'date': '2024-01-03'}
        ]
        
        result = validate_records(records, BillRecord)
        
        assert result.total_records == 3
        assert result.valid_records == 2
        assert result.invalid_records == 1
        assert len(result.errors) == 1
    
    def test_empty_records(self):
        """Test validate_records với empty list."""
        from src.quality.validators import validate_records, BillRecord
        
        result = validate_records([], BillRecord)
        
        assert result.total_records == 0
        assert result.valid_records == 0
        assert result.success_rate == 0.0


class TestDataQualityChecker:
    """Test suite cho DataQualityChecker."""
    
    def test_check_nulls_pass(self):
        """Test check_nulls pass với đủ required fields."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'},
            {'id': 3, 'name': 'C'}
        ]
        
        checker = DataQualityChecker('test', records)
        result = checker.check_nulls(['id', 'name'])
        
        assert result.passed is True
    
    def test_check_nulls_fail(self):
        """Test check_nulls fail khi thiếu nhiều required fields."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': None},
            {'id': None, 'name': None}
        ]
        
        checker = DataQualityChecker('test', records)
        result = checker.check_nulls(['id', 'name'], threshold=0.95)
        
        assert result.passed is False
    
    def test_check_duplicates_no_duplicates(self):
        """Test check_duplicates với no duplicates."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'},
            {'id': 3, 'name': 'C'}
        ]
        
        checker = DataQualityChecker('test', records)
        result = checker.check_duplicates(['id'])
        
        assert result.passed is True
        assert result.details['duplicate_count'] == 0
    
    def test_check_duplicates_with_duplicates(self):
        """Test check_duplicates với duplicates."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'name': 'A'},
            {'id': 1, 'name': 'B'},  # Duplicate id
            {'id': 2, 'name': 'C'}
        ]
        
        checker = DataQualityChecker('test', records)
        result = checker.check_duplicates(['id'], max_duplicate_rate=0.01)
        
        assert result.passed is False
        assert result.details['duplicate_count'] == 1
    
    def test_check_data_types_correct(self):
        """Test check_data_types với correct types."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'}
        ]
        
        checker = DataQualityChecker('test', records)
        result = checker.check_data_types({'id': int, 'name': str})
        
        assert result.passed is True
    
    def test_check_data_types_incorrect(self):
        """Test check_data_types với incorrect types."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 'not_an_int', 'name': 'A'},
            {'id': 2, 'name': 123}  # name should be str
        ]
        
        checker = DataQualityChecker('test', records)
        result = checker.check_data_types({'id': int, 'name': str})
        
        assert result.passed is False
    
    def test_run_all_checks_bills(self):
        """Test run_all_checks cho bills entity."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'date': '2024-01-01'},
            {'id': 2, 'date': '2024-01-02'}
        ]
        
        checker = DataQualityChecker('bills', records)
        report = checker.run_all_checks()
        
        assert report.entity == 'bills'
        assert report.total_records == 2
        assert len(report.checks) >= 3  # null, duplicate, type checks
    
    def test_quality_report_score(self):
        """Test QualityReport score calculation."""
        from src.quality.checks import DataQualityChecker
        
        records = [
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'}
        ]
        
        checker = DataQualityChecker('products', records)
        report = checker.run_all_checks()
        
        # All checks should pass with valid data
        assert report.score == 100.0
        assert report.passed is True
    
    def test_quality_report_to_dict(self):
        """Test QualityReport to_dict."""
        from src.quality.checks import DataQualityChecker
        
        records = [{'id': 1, 'name': 'A'}]
        
        checker = DataQualityChecker('test', records)
        checker.check_nulls(['id'])
        
        report_dict = checker.report.to_dict()
        
        assert 'entity' in report_dict
        assert 'total_records' in report_dict
        assert 'checks' in report_dict
        assert 'score' in report_dict
