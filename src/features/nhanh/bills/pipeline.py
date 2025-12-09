"""
Pipeline cho Bills feature.
Orchestrate toàn bộ ETL flow: Extract → Transform → Load.
"""
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional
from .extractor import BillExtractor
from .transformer import BillTransformer
from .loader import BillLoader
from src.shared.logging import get_logger

logger = get_logger(__name__)


class BillPipeline:
    """
    Pipeline ETL cho bills.
    Kết hợp Extractor, Transformer, Loader thành một flow hoàn chỉnh.
    """
    
    def __init__(self):
        """Khởi tạo pipeline với các components."""
        self.extractor = BillExtractor()
        self.transformer = BillTransformer()
        self.loader = BillLoader()
    
    def run_extract_load(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        process_by_day: bool = True
    ) -> Dict[str, Any]:
        """
        Chạy Extract và Load (Bronze layer) theo từng ngày.
        
        Args:
            from_date: Ngày bắt đầu
            to_date: Ngày kết thúc
            process_by_day: Xử lý theo từng ngày (default True, now enforced)
            
        Returns:
            Dict với kết quả extraction và loading
        """
        logger.info("Starting Extract-Load pipeline for bills (Day-by-Day)")
        
        # Determine date range - use client's split function
        if from_date is None:
            from_date = datetime.now() - timedelta(days=30)
        if to_date is None:
            to_date = datetime.now()
        
        date_chunks = self.extractor.client.split_date_range_by_day(from_date, to_date)
        
        total_bills = 0
        total_products = 0
        processed_days = 0
        
        for chunk_idx, (day_start, day_end) in enumerate(date_chunks, 1):
            partition_date = day_start.date()
            logger.info(
                f"Processing day {chunk_idx}/{len(date_chunks)}: {partition_date}"
            )
            
            try:
                # Step 1: Extract for this day
                bills, products = self.extractor.extract_with_products(
                    from_date=day_start,
                    to_date=day_end,
                    process_by_day=False  # Already split, don't split again
                )
                
                logger.info(
                    f"Day {partition_date}: Extracted {len(bills)} bills, {len(products)} products"
                )
                
                # Step 2: Load bills for this day
                if bills:
                    self.loader.load_bills(data=bills, partition_date=partition_date)
                
                # Step 3: Load products for this day
                if products:
                    self.loader.load_bill_products(data=products, partition_date=partition_date)
                
                total_bills += len(bills)
                total_products += len(products)
                processed_days += 1
                
                logger.info(
                    f"Day {partition_date}: Load completed. Running total: {total_bills} bills, {total_products} products"
                )
                
            except Exception as e:
                logger.error(
                    f"FAILED on day {partition_date}: {e}. Stopping pipeline."
                )
                raise e  # Fail fast
        
        result = {
            "bills_extracted": total_bills,
            "products_extracted": total_products,
            "days_processed": processed_days,
            "status": "success"
        }
        
        logger.info("Completed Extract-Load pipeline", **result)
        return result

    
    def run_transform(self) -> Dict[str, Any]:
        """
        Chạy Transform: Raw -> Flattened (Clean).
        """
        logger.info("Starting Transform pipeline (Raw -> Clean)")
        return self.transformer.transform_flatten()
    
    def run_full_pipeline(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Chạy full ETL pipeline: Extract -> Load Raw -> Setup External Tables -> Transform Clean.
        """
        logger.info("Starting full ETL pipeline for bills")
        
        result = {
            "extract_load": {},
            "external_tables": {},
            "transform": {},
            "status": "success"
        }
        
        try:
            # Step 1: Extract + Load to Raw (GCS)
            result["extract_load"] = self.run_extract_load(
                from_date=from_date,
                to_date=to_date
            )
            
            # Step 2: Setup External Tables in BigQuery
            logger.info("Setting up BigQuery External Tables...")
            result["external_tables"] = self.loader.setup_external_tables()
            
            # Step 3: Flatten Data (Transform)
            result["transform"] = self.run_transform()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            result["status"] = "error"
            result["error"] = str(e)
        
        logger.info("Completed full ETL pipeline", status=result["status"])
        return result
