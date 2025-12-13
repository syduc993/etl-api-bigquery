"""
Script để repopulate fact tables từ external tables sau khi clear data.
Chạy transform step để đọc từ bronze external tables và insert vào fact tables.
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))))

from src.features.nhanh.bills.components.transformer import BillTransformer
from src.shared.logging import get_logger

logger = get_logger(__name__)


def main():
    """Repopulate fact tables from external tables."""
    print("🔄 Repopulating fact tables from external tables...")
    print("   This will read from bronze external tables and transform into fact tables")
    print()
    
    try:
        transformer = BillTransformer()
        result = transformer.transform_flatten()
        
        print("\n" + "="*60)
        print("✅ FACT TABLES REPOPULATED SUCCESSFULLY")
        print("="*60)
        print(f"Job ID: {result.get('job_id')}")
        print("\nFact tables now contain data from external tables (GCS)")
        
        return 0
        
    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        print(f"\n❌ FAILED: {e}")
        return 1


if __name__ == "__main__":
    exit(main())

