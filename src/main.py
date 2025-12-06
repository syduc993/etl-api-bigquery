"""
Main entry point cho Cloud Run Jobs.
File này là điểm khởi đầu của ETL pipeline, xử lý extraction và loading
của các entities từ nhiều platforms vào Bronze layer.
Sử dụng registry pattern để dễ dàng mở rộng cho platforms và entities mới.
"""
import sys
import argparse
from datetime import datetime
from typing import List, Optional
from src.config import settings
from src.extractors import registry, list_available_platforms, list_available_entities
from src.loaders.gcs_loader import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.utils.logging import get_logger

logger = get_logger(__name__)


def extract_entity(
    platform: str,
    entity: str,
    loader: GCSLoader,
    watermark_tracker: WatermarkTracker,
    incremental: bool = True,
    **extractor_kwargs
):
    """
    Extract và load một entity từ một platform vào Bronze layer.
    
    Args:
        platform: Tên platform (ví dụ: 'nhanh', 'facebook')
        entity: Tên entity (ví dụ: 'bills', 'orders')
        loader: GCSLoader instance
        watermark_tracker: WatermarkTracker instance
        incremental: Có dùng incremental extraction hay không
        **extractor_kwargs: Các tham số cụ thể cho extractor
    """
    logger.info(
        f"Starting extraction",
        platform=platform,
        entity=entity,
        incremental=incremental
    )
    
    # Get extractor từ registry
    extractor_class = registry.get(platform, entity)
    if not extractor_class:
        logger.error(
            f"Extractor not found",
            platform=platform,
            entity=entity
        )
        raise ValueError(f"Extractor not found for {platform}.{entity}")
    
    # Create extractor instance
    extractor = registry.create_instance(platform, entity)
    if not extractor:
        logger.error(
            f"Failed to create extractor instance",
            platform=platform,
            entity=entity
        )
        raise ValueError(f"Failed to create extractor for {platform}.{entity}")
    
    # Get incremental range nếu cần (chỉ khi không có date range được chỉ định)
    if incremental and "from_date" not in extractor_kwargs and "to_date" not in extractor_kwargs:
        watermark_key = f"{platform}_{entity}"
        from_date, to_date = watermark_tracker.get_incremental_range(
            watermark_key,
            lookback_hours=1
        )
        extractor_kwargs["updated_at_from"] = from_date
        extractor_kwargs["updated_at_to"] = to_date
        extractor_kwargs["incremental"] = True
    elif "from_date" in extractor_kwargs and "to_date" in extractor_kwargs:
        # Use provided date range instead of incremental
        extractor_kwargs["incremental"] = False
    else:
        extractor_kwargs["incremental"] = False
    
    # Extract data
    try:
        data = extractor.extract(**extractor_kwargs)
    except Exception as e:
        logger.error(
            f"Error extracting {platform}.{entity}",
            error=str(e),
            platform=platform,
            entity=entity
        )
        raise
    
    if not data:
        logger.info(
            f"No data to upload",
            platform=platform,
            entity=entity
        )
        return
    
    # Upload to GCS với platform prefix
    entity_path = f"{platform}/{entity}"
    metadata = {
        "platform": platform,
        "entity": entity,
        "extraction_timestamp": datetime.utcnow().isoformat(),
        "record_count": len(data),
        "incremental": incremental
    }
    
    loader.upload_json(
        entity=entity_path,
        data=data,
        metadata=metadata
    )
    
    # Update watermark
    watermark_key = f"{platform}_{entity}"
    watermark_tracker.update_watermark(
        entity=watermark_key,
        extracted_at=datetime.utcnow(),
        records_count=len(data)
    )
    
    logger.info(
        f"Completed extraction",
        platform=platform,
        entity=entity,
        records=len(data)
    )


def main():
    """
    Main entry point của ETL pipeline.
    
    Hàm này parse command line arguments và chạy extraction
    cho các entities được chỉ định từ các platforms.
    """
    parser = argparse.ArgumentParser(description="Nhanh ETL Pipeline")
    parser.add_argument(
        "--platform",
        type=str,
        default="nhanh",
        help="Platform to extract from (nhanh, facebook, tiktok, oneoffices)"
    )
    parser.add_argument(
        "--entity",
        type=str,
        help="Entity to extract (bills, products, customers, orders, etc.). Use 'all' for all entities"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Use incremental extraction (default: True)"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Perform full sync (overrides --incremental)"
    )
    parser.add_argument(
        "--from-date",
        type=str,
        help="Start date for extraction (format: YYYY-MM-DD). When provided, uses date range instead of incremental"
    )
    parser.add_argument(
        "--to-date",
        type=str,
        help="End date for extraction (format: YYYY-MM-DD). When provided, uses date range instead of incremental"
    )
    
    args = parser.parse_args()
    
    # Parse date range if provided
    from_date = None
    to_date = None
    if args.from_date or args.to_date:
        if not args.from_date or not args.to_date:
            logger.error("Both --from-date and --to-date must be provided when using date range")
            sys.exit(1)
        try:
            from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
            to_date = datetime.strptime(args.to_date, "%Y-%m-%d")
            # Set end of day for to_date
            to_date = to_date.replace(hour=23, minute=59, second=59)
            logger.info(
                f"Using date range extraction",
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat()
            )
        except ValueError as e:
            logger.error(f"Invalid date format. Use YYYY-MM-DD format. Error: {e}")
            sys.exit(1)
    
    # If date range is provided, disable incremental
    incremental = not args.full_sync if args.full_sync else (args.incremental and not (from_date and to_date))
    
    try:
        logger.info(
            f"Starting ETL pipeline",
            platform=args.platform,
            entity=args.entity,
            incremental=incremental,
            project=settings.gcp_project
        )
        
        # Initialize components
        loader = GCSLoader(bucket_name=settings.bronze_bucket)
        watermark_tracker = WatermarkTracker()
        
        # Determine entities to process
        if args.entity == "all":
            # Get all entities for the platform
            entities = list_available_entities(args.platform)
            if not entities:
                logger.error(
                    f"No entities available for platform {args.platform}",
                    platform=args.platform,
                    available_platforms=list_available_platforms()
                )
                sys.exit(1)
        else:
            entities = [args.entity]
        
        logger.info(
            f"Processing entities",
            platform=args.platform,
            entities=entities,
            count=len(entities)
        )
        
        # Process each entity
        for entity in entities:
            try:
                logger.info(
                    f"Processing entity",
                    platform=args.platform,
                    entity=entity
                )
                
                # Prepare extractor kwargs
                extractor_kwargs = {}
                if from_date and to_date:
                    # Use date range for extraction
                    extractor_kwargs["from_date"] = from_date
                    extractor_kwargs["to_date"] = to_date
                
                extract_entity(
                    platform=args.platform,
                    entity=entity,
                    loader=loader,
                    watermark_tracker=watermark_tracker,
                    incremental=incremental,
                    **extractor_kwargs
                )
            
            except Exception as e:
                logger.error(
                    f"Error processing entity",
                    error=str(e),
                    platform=args.platform,
                    entity=entity
                )
                # Continue with next entity instead of failing completely
                continue
        
        logger.info("ETL pipeline completed successfully")
    
    except Exception as e:
        logger.error(f"ETL pipeline failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
