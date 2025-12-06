"""
Metrics tracking cho ETL pipeline.
File này cung cấp các hàm để track và log metrics
cho monitoring và alerting.
"""
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import MetricServiceClient
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


class MetricsTracker:
    """
    Tracker cho ETL pipeline metrics.
    
    Metrics được gửi lên Cloud Monitoring để:
    - Track job execution status
    - Track records processed
    - Track API rate limit hits
    - Track data quality scores
    - Track pipeline latency
    """
    
    def __init__(self):
        """
        Khởi tạo metrics tracker.
        
        Tự động tạo Cloud Monitoring client.
        """
        self.client = MetricServiceClient()
        self.project_name = f"projects/{settings.gcp_project}"
    
    def log_job_status(
        self,
        job_name: str,
        status: str,
        entity: Optional[str] = None,
        error_message: Optional[str] = None
    ):
        """
        Log job execution status.
        
        Args:
            job_name: Tên job (ví dụ: 'bronze-extraction', 'silver-transformation')
            status: Status ('success', 'failure', 'running')
            entity: Entity name nếu có (ví dụ: 'bills', 'products')
            error_message: Error message nếu status là 'failure'
        """
        try:
            # Log structured data để Cloud Logging có thể parse
            log_data = {
                "job_name": job_name,
                "status": status,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if entity:
                log_data["entity"] = entity
            
            if error_message:
                log_data["error"] = error_message
            
            if status == "success":
                logger.info(f"Job {job_name} completed successfully", **log_data)
            elif status == "failure":
                logger.error(f"Job {job_name} failed", **log_data)
            else:
                logger.info(f"Job {job_name} status: {status}", **log_data)
        
        except Exception as e:
            logger.error(f"Error logging job status", error=str(e))
    
    def log_records_processed(
        self,
        job_name: str,
        entity: str,
        records_count: int,
        duration_seconds: float
    ):
        """
        Log số lượng records đã xử lý.
        
        Args:
            job_name: Tên job
            entity: Entity name
            records_count: Số lượng records
            duration_seconds: Thời gian xử lý (giây)
        """
        try:
            logger.info(
                f"Processed {records_count} {entity} records",
                job_name=job_name,
                entity=entity,
                records_count=records_count,
                duration_seconds=duration_seconds,
                records_per_second=records_count / duration_seconds if duration_seconds > 0 else 0
            )
        except Exception as e:
            logger.error(f"Error logging records processed", error=str(e))
    
    def log_rate_limit_hit(
        self,
        endpoint: str,
        locked_seconds: int,
        unlocked_at: Optional[int] = None
    ):
        """
        Log khi bị rate limit.
        
        Args:
            endpoint: API endpoint bị rate limit
            locked_seconds: Số giây bị khóa
            unlocked_at: Timestamp khi được unlock
        """
        try:
            logger.warning(
                f"Rate limit hit for {endpoint}",
                endpoint=endpoint,
                locked_seconds=locked_seconds,
                unlocked_at=unlocked_at
            )
        except Exception as e:
            logger.error(f"Error logging rate limit hit", error=str(e))
    
    def log_data_quality_score(
        self,
        entity: str,
        quality_score: float,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Log data quality score.
        
        Args:
            entity: Entity name
            quality_score: Quality score (0.0 - 1.0)
            details: Chi tiết quality checks
        """
        try:
            log_data = {
                "entity": entity,
                "quality_score": quality_score,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if details:
                log_data.update(details)
            
            if quality_score < 0.95:
                logger.warning(
                    f"Data quality below threshold for {entity}",
                    **log_data
                )
            else:
                logger.info(
                    f"Data quality score for {entity}",
                    **log_data
                )
        except Exception as e:
            logger.error(f"Error logging data quality score", error=str(e))
    
    def log_pipeline_latency(
        self,
        phase: str,
        duration_seconds: float,
        entity: Optional[str] = None
    ):
        """
        Log pipeline latency.
        
        Args:
            phase: Phase name ('bronze', 'silver', 'gold')
            duration_seconds: Thời gian thực hiện (giây)
            entity: Entity name nếu có
        """
        try:
            log_data = {
                "phase": phase,
                "duration_seconds": duration_seconds,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if entity:
                log_data["entity"] = entity
            
            logger.info(
                f"Pipeline latency for {phase}",
                **log_data
            )
        except Exception as e:
            logger.error(f"Error logging pipeline latency", error=str(e))

