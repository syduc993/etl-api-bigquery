"""
Entry point cho Silver layer transformation.
File này chạy transformation từ Bronze sang Silver layer.
Có thể chạy độc lập hoặc được gọi từ orchestration script.
"""
import sys
import argparse
from src.transformers.bronze_to_silver import BronzeToSilverTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """
    Main entry point cho Silver transformation.
    
    Parse command line arguments và chạy transformation
    cho các entities được chỉ định.
    """
    parser = argparse.ArgumentParser(description="Bronze to Silver Transformation")
    parser.add_argument(
        "--entity",
        choices=["bills", "products", "customers", "all"],
        default="all",
        help="Entity to transform"
    )
    
    args = parser.parse_args()
    
    try:
        logger.info(
            f"Starting Silver transformation",
            entity=args.entity
        )
        
        transformer = BronzeToSilverTransformer()
        
        if args.entity == "all":
            results = transformer.transform_all()
        elif args.entity == "bills":
            results = {"bills": transformer.transform_bills()}
        elif args.entity == "products":
            results = {"products": transformer.transform_products()}
        elif args.entity == "customers":
            results = {"customers": transformer.transform_customers()}
        
        logger.info("Silver transformation completed successfully", results=results)
    
    except Exception as e:
        logger.error(f"Silver transformation failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

