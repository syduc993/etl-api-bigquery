"""
Pipeline For 1Office ETL.
Orchestrate the flow: Extract -> Load (Snapshot).
"""
from typing import Dict, Any
from .components.extractor import OneOfficeExtractor
from .components.loader import OneOfficeLoader
from src.shared.logging import get_logger

logger = get_logger(__name__)


class OneOfficePipeline:
    """
    Pipeline ETL cho dữ liệu 1Office.
    Flow hiện tại: Extract All -> Load Snapshot (BigQuery).
    """
    
    def __init__(self):
        self.extractor = OneOfficeExtractor()
        self.loader = OneOfficeLoader()
        
    def run_daily_snapshot(self) -> Dict[str, Any]:
        """
        Thực hiện lấy dữ liệu và lưu snapshot cho ngày hôm nay.
        
        Returns:
            Dict kết quả thực thi.
        """
        logger.info("Starting Daily Snapshot Pipeline for 1Office...")
        
        try:
            # Step 1: Extract
            data = self.extractor.fetch_all_personnel()
            count_extracted = len(data)
            
            # Step 2: Load
            count_loaded = 0
            if count_extracted > 0:
                count_loaded = self.loader.load_snapshots(data)
                
            result = {
                "status": "success",
                "extracted": count_extracted,
                "loaded": count_loaded
            }
            logger.info("Pipeline completed successfully.", **result)
            return result
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
