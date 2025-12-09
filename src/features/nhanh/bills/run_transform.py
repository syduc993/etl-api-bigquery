"""
Script to transform Nhanh bills data from Raw to Clean/Fact tables.
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))

from src.shared.logging import get_logger
from src.features.nhanh.bills.transformer import BillTransformer

logger = get_logger(__name__)

def main():
    try:
        logger.info("Starting Bills Transformation...")
        transformer = BillTransformer()
        result = transformer.transform_flatten()
        logger.info(f"Transformation finished: {result}")
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

