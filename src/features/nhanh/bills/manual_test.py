
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from src.features.nhanh.bills.pipeline import BillPipeline
from src.shared.logging import get_logger

logger = get_logger(__name__)

def main():
    pipeline = BillPipeline()
    
    # 06/12/2025 - 07/12/2025
    from_date = datetime(2025, 12, 6)
    to_date = datetime(2025, 12, 7)
    
    logger.info(f"Running manual test for bills from {from_date} to {to_date}")
    
    try:
        # Run full pipeline: Extract -> Load -> Setup External Tables -> Transform
        result = pipeline.run_full_pipeline(
            from_date=from_date,
            to_date=to_date
        )
        print("\nFull Pipeline Result:")
        print(result)
        
    except Exception as e:
        logger.error(f"Manual test failed: {e}")
        raise e

if __name__ == "__main__":
    main()
