"""Run transform to sync data from bronze to fact tables."""
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from src.features.nhanh.bills.components.transformer import BillTransformer
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """Run transform step."""
    try:
        logger.info("🔄 Starting transform from bronze to fact tables...")
        
        transformer = BillTransformer()
        result = transformer.transform_flatten()
        
        logger.info(
            "✅ Transform completed successfully",
            job_id=result.get('job_id'),
            status=result.get('status')
        )
        
        print("✅ Transform completed!")
        print(f"   Job ID: {result.get('job_id')}")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Transform failed: {e}", exc_info=True)
        print(f"❌ Transform failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

