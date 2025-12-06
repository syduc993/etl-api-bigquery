"""
Entry point cho Gold layer transformation.
File này chạy transformation từ Silver sang Gold layer
để tạo các business aggregates và curated data.
Có thể chạy độc lập hoặc được gọi từ orchestration script.
"""
import sys
import argparse
from src.transformers.silver_to_gold import SilverToGoldTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """
    Main entry point cho Gold transformation.
    
    Parse command line arguments và chạy transformation
    cho các aggregates được chỉ định.
    """
    parser = argparse.ArgumentParser(description="Silver to Gold Transformation")
    parser.add_argument(
        "--aggregate",
        choices=["daily_revenue", "customer_clv", "product_metrics", "inventory", "all"],
        default="all",
        help="Aggregate to create"
    )
    
    args = parser.parse_args()
    
    try:
        logger.info(
            f"Starting Gold transformation",
            aggregate=args.aggregate
        )
        
        transformer = SilverToGoldTransformer()
        
        if args.aggregate == "all":
            results = transformer.transform_all()
        elif args.aggregate == "daily_revenue":
            results = {"daily_revenue_summary": transformer.create_daily_revenue_summary()}
        elif args.aggregate == "customer_clv":
            results = {"customer_lifetime_value": transformer.create_customer_lifetime_value()}
        elif args.aggregate == "product_metrics":
            results = {"product_sales_metrics": transformer.create_product_sales_metrics()}
        elif args.aggregate == "inventory":
            results = {"inventory_analytics": transformer.create_inventory_analytics()}
        
        logger.info("Gold transformation completed successfully", results=results)
    
    except Exception as e:
        logger.error(f"Gold transformation failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

