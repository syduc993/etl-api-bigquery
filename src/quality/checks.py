"""
Data quality checks module.
File này cung cấp các checks để đảm bảo chất lượng data:
- Null checks cho required fields
- Duplicate detection
- Data type validation
- Range checks
"""
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class QualityCheck:
    """Kết quả của một quality check."""
    check_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class QualityReport:
    """Báo cáo tổng hợp quality checks."""
    entity: str
    total_records: int
    checks: List[QualityCheck] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def passed(self) -> bool:
        """Tất cả checks đều pass."""
        return all(check.passed for check in self.checks)
    
    @property
    def score(self) -> float:
        """Tính quality score (0-100)."""
        if not self.checks:
            return 100.0
        passed_count = sum(1 for check in self.checks if check.passed)
        return (passed_count / len(self.checks)) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'entity': self.entity,
            'total_records': self.total_records,
            'passed': self.passed,
            'score': self.score,
            'timestamp': self.timestamp.isoformat(),
            'checks': [
                {
                    'name': c.check_name,
                    'passed': c.passed,
                    'message': c.message,
                    'details': c.details
                }
                for c in self.checks
            ]
        }


class DataQualityChecker:
    """
    Data quality checker cho ETL pipeline.
    
    Cung cấp các checks:
    - check_nulls: Kiểm tra null values trong required fields
    - check_duplicates: Kiểm tra duplicate records
    - check_data_types: Validate data types
    - run_all_checks: Chạy tất cả checks
    """
    
    def __init__(self, entity: str, records: List[Dict[str, Any]]):
        """
        Khởi tạo checker.
        
        Args:
            entity: Tên entity (bills, products, customers)
            records: Danh sách records cần check
        """
        self.entity = entity
        self.records = records
        self.report = QualityReport(
            entity=entity,
            total_records=len(records)
        )
    
    def check_nulls(
        self,
        required_fields: List[str],
        threshold: float = 0.95
    ) -> QualityCheck:
        """
        Kiểm tra null values trong required fields.
        
        Args:
            required_fields: Danh sách fields bắt buộc
            threshold: Tỷ lệ tối thiểu records có đủ required fields (0-1)
            
        Returns:
            QualityCheck result
        """
        if not self.records:
            return QualityCheck(
                check_name='null_check',
                passed=True,
                message='No records to check'
            )
        
        null_counts = {field: 0 for field in required_fields}
        
        for record in self.records:
            for field_name in required_fields:
                value = record.get(field_name)
                if value is None or value == '':
                    null_counts[field_name] += 1
        
        # Calculate completeness rate
        total_checks = len(required_fields) * len(self.records)
        total_nulls = sum(null_counts.values())
        completeness = 1 - (total_nulls / total_checks) if total_checks > 0 else 1.0
        
        passed = completeness >= threshold
        
        check = QualityCheck(
            check_name='null_check',
            passed=passed,
            message=f'Completeness: {completeness:.2%} (threshold: {threshold:.2%})',
            details={
                'null_counts': null_counts,
                'completeness': completeness,
                'threshold': threshold
            }
        )
        
        self.report.checks.append(check)
        
        logger.info(
            f"Null check completed",
            entity=self.entity,
            passed=passed,
            completeness=completeness
        )
        
        return check
    
    def check_duplicates(
        self,
        key_fields: List[str],
        max_duplicate_rate: float = 0.01
    ) -> QualityCheck:
        """
        Kiểm tra duplicate records dựa trên key fields.
        
        Args:
            key_fields: Fields để xác định unique record
            max_duplicate_rate: Tỷ lệ duplicate tối đa cho phép (0-1)
            
        Returns:
            QualityCheck result
        """
        if not self.records:
            return QualityCheck(
                check_name='duplicate_check',
                passed=True,
                message='No records to check'
            )
        
        seen_keys: Set[tuple] = set()
        duplicates = 0
        duplicate_examples = []
        
        for record in self.records:
            key = tuple(record.get(f) for f in key_fields)
            if key in seen_keys:
                duplicates += 1
                if len(duplicate_examples) < 5:
                    duplicate_examples.append({f: record.get(f) for f in key_fields})
            else:
                seen_keys.add(key)
        
        duplicate_rate = duplicates / len(self.records) if self.records else 0
        passed = duplicate_rate <= max_duplicate_rate
        
        check = QualityCheck(
            check_name='duplicate_check',
            passed=passed,
            message=f'Duplicate rate: {duplicate_rate:.2%} ({duplicates} duplicates)',
            details={
                'duplicate_count': duplicates,
                'duplicate_rate': duplicate_rate,
                'max_allowed': max_duplicate_rate,
                'examples': duplicate_examples
            }
        )
        
        self.report.checks.append(check)
        
        logger.info(
            f"Duplicate check completed",
            entity=self.entity,
            passed=passed,
            duplicates=duplicates
        )
        
        return check
    
    def check_data_types(
        self,
        type_specs: Dict[str, type]
    ) -> QualityCheck:
        """
        Validate data types của các fields.
        
        Args:
            type_specs: Dict mapping field name -> expected type
            
        Returns:
            QualityCheck result
        """
        if not self.records:
            return QualityCheck(
                check_name='type_check',
                passed=True,
                message='No records to check'
            )
        
        type_errors = {field: 0 for field in type_specs}
        
        for record in self.records:
            for field_name, expected_type in type_specs.items():
                value = record.get(field_name)
                if value is not None and not isinstance(value, expected_type):
                    type_errors[field_name] += 1
        
        total_checks = len(type_specs) * len(self.records)
        total_errors = sum(type_errors.values())
        type_correctness = 1 - (total_errors / total_checks) if total_checks > 0 else 1.0
        
        # Pass if >95% correct types
        passed = type_correctness >= 0.95
        
        check = QualityCheck(
            check_name='type_check',
            passed=passed,
            message=f'Type correctness: {type_correctness:.2%}',
            details={
                'type_errors': type_errors,
                'correctness': type_correctness
            }
        )
        
        self.report.checks.append(check)
        
        logger.info(
            f"Type check completed",
            entity=self.entity,
            passed=passed,
            correctness=type_correctness
        )
        
        return check
    
    def run_all_checks(self) -> QualityReport:
        """
        Chạy tất cả quality checks dựa trên entity type.
        
        Returns:
            QualityReport với kết quả tất cả checks
        """
        if self.entity == 'bills':
            self.check_nulls(['id', 'date'])
            self.check_duplicates(['id'])
            self.check_data_types({'id': int})
            
        elif self.entity == 'products':
            self.check_nulls(['id', 'name'])
            self.check_duplicates(['id'])
            self.check_data_types({'id': int, 'name': str})
            
        elif self.entity == 'customers':
            self.check_nulls(['id', 'name'])
            self.check_duplicates(['id'])
            self.check_data_types({'id': int})
        
        else:
            # Generic checks for unknown entities
            self.check_nulls(['id'])
            self.check_duplicates(['id'])
        
        logger.info(
            f"All quality checks completed",
            entity=self.entity,
            score=self.report.score,
            passed=self.report.passed
        )
        
        return self.report
