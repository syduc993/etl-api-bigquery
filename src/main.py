"""
Main entry point cho Cloud Run Jobs.
File này là điểm khởi đầu của ETL pipeline, xử lý extraction và loading
của các entities từ nhiều platforms vào Bronze layer.
Sử dụng registry pattern để dễ dàng mở rộng cho platforms và entities mới.
"""
import sys
import argparse
from datetime import datetime, timedelta
from typing import List, Optional
from src.config import settings
from src.extractors import registry, list_available_platforms, list_available_entities
from src.loaders.gcs_loader import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.loaders.bigquery_setup import BigQueryExternalTableSetup
from src.utils.logging import get_logger

logger = get_logger(__name__)


def extract_entity(
    platform: str,
    entity: str,
    loader: GCSLoader,
    watermark_tracker: WatermarkTracker,
    incremental: bool = True,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
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
    if incremental and not from_date and not to_date:
        watermark_key = f"{platform}_{entity}"
        incr_from, incr_to = watermark_tracker.get_incremental_range(
            watermark_key,
            lookback_hours=1
        )
        extractor_kwargs["updated_at_from"] = incr_from
        extractor_kwargs["updated_at_to"] = incr_to
    # Pass date range to extractor if provided
    elif from_date and to_date:
        extractor_kwargs["from_date"] = from_date
        extractor_kwargs["to_date"] = to_date
    
    # Special handling for bills: extract with products separation
    if platform == "nhanh" and entity == "bills" and hasattr(extractor, "extract_with_products"):
        # Nếu có date range, xử lý từng ngày riêng biệt
        if from_date and to_date:
            # Chia date range thành từng ngày
            from src.extractors.nhanh.base import NhanhApiClient
            date_splitter = NhanhApiClient()
            day_chunks = date_splitter.split_date_range_by_day(from_date, to_date)
            
            logger.info(
                f"Processing {len(day_chunks)} days separately",
                platform=platform,
                entity=entity,
                days=len(day_chunks)
            )
            
            total_bills = 0
            total_products = 0
            
            # Xử lý từng ngày
            for day_idx, (day_from, day_to) in enumerate(day_chunks, 1):
                day_date = day_from.date()
                logger.info(
                    f"Processing day {day_idx}/{len(day_chunks)}: {day_date}",
                    day=day_idx,
                    total_days=len(day_chunks),
                    date=day_date.isoformat()
                )
                
                # Extract data cho ngày này
                day_kwargs = extractor_kwargs.copy()
                day_kwargs["from_date"] = day_from
                day_kwargs["to_date"] = day_to
                day_kwargs["process_by_day"] = True
                
                try:
                    bills_data, products_data = extractor.extract_with_products(**day_kwargs)
                except Exception as e:
                    logger.error(
                        f"Error extracting {platform}.{entity} for day {day_date}",
                        error=str(e),
                        platform=platform,
                        entity=entity,
                        date=day_date.isoformat()
                    )
                    continue
                
                # Upload bills cho ngày này (parquet)
                if bills_data:
                    bills_path = f"{platform}/{entity}"
                    bills_metadata = {
                        "platform": platform,
                        "entity": entity,
                        "extraction_timestamp": datetime.utcnow().isoformat(),
                        "record_count": len(bills_data),
                        "incremental": incremental,
                        "date": day_date.isoformat()
                    }
                    
                    loader.upload_parquet_by_date(
                        entity=bills_path,
                        data=bills_data,
                        partition_date=day_date,
                        metadata=bills_metadata,
                        overwrite_partition=True
                    )
                    
                    total_bills += len(bills_data)
                    
                    logger.info(
                        f"Uploaded bills for day {day_date}",
                        date=day_date.isoformat(),
                        records=len(bills_data)
                    )
                
                # Upload products cho ngày này (parquet)
                if products_data:
                    products_path = f"{platform}/bill_products"
                    products_metadata = {
                        "platform": platform,
                        "entity": "bill_products",
                        "extraction_timestamp": datetime.utcnow().isoformat(),
                        "record_count": len(products_data),
                        "incremental": incremental,
                        "date": day_date.isoformat()
                    }
                    
                    loader.upload_parquet_by_date(
                        entity=products_path,
                        data=products_data,
                        partition_date=day_date,
                        metadata=products_metadata,
                        overwrite_partition=True
                    )
                    
                    total_products += len(products_data)
                    
                    logger.info(
                        f"Uploaded products for day {day_date}",
                        date=day_date.isoformat(),
                        records=len(products_data)
                    )
            
            # Update watermark sau khi xử lý tất cả các ngày
            if total_bills > 0:
                watermark_key = f"{platform}_{entity}"
                watermark_tracker.update_watermark(
                    entity=watermark_key,
                    extracted_at=datetime.utcnow(),
                    records_count=total_bills
                )
            
            if total_products > 0:
                watermark_key = f"{platform}_bill_products"
                watermark_tracker.update_watermark(
                    entity=watermark_key,
                    extracted_at=datetime.utcnow(),
                    records_count=total_products
                )
            
            logger.info(
                f"Completed extraction with products separation (by day)",
                platform=platform,
                entity=entity,
                total_bills=total_bills,
                total_products=total_products,
                days_processed=len(day_chunks)
            )
        else:
            # Không có date range, xử lý như cũ (incremental hoặc full sync)
            try:
                bills_data, products_data = extractor.extract_with_products(**extractor_kwargs)
            except Exception as e:
                logger.error(
                    f"Error extracting {platform}.{entity} with products",
                    error=str(e),
                    platform=platform,
                    entity=entity
                )
                raise
            
            if not bills_data and not products_data:
                logger.info(
                    f"No data to upload",
                    platform=platform,
                    entity=entity
                )
                return
            
            # Upload bills (parquet)
            if bills_data:
                bills_path = f"{platform}/{entity}"
                bills_metadata = {
                    "platform": platform,
                    "entity": entity,
                    "extraction_timestamp": datetime.utcnow().isoformat(),
                    "record_count": len(bills_data),
                    "incremental": incremental
                }
                
                partition_date = None
                if from_date:
                    partition_date = from_date.date()
                
                loader.upload_parquet_by_date(
                    entity=bills_path,
                    data=bills_data,
                    partition_date=partition_date,
                    metadata=bills_metadata,
                    overwrite_partition=True
                )
                
                # Update watermark for bills
                watermark_key = f"{platform}_{entity}"
                watermark_tracker.update_watermark(
                    entity=watermark_key,
                    extracted_at=datetime.utcnow(),
                    records_count=len(bills_data)
                )
            
            # Upload products (parquet)
            if products_data:
                products_path = f"{platform}/bill_products"
                products_metadata = {
                    "platform": platform,
                    "entity": "bill_products",
                    "extraction_timestamp": datetime.utcnow().isoformat(),
                    "record_count": len(products_data),
                    "incremental": incremental
                }
                
                partition_date = None
                if from_date:
                    partition_date = from_date.date()
                
                loader.upload_parquet_by_date(
                    entity=products_path,
                    data=products_data,
                    partition_date=partition_date,
                    metadata=products_metadata,
                    overwrite_partition=True
                )
                
                # Update watermark for bill_products
                watermark_key = f"{platform}_bill_products"
                watermark_tracker.update_watermark(
                    entity=watermark_key,
                    extracted_at=datetime.utcnow(),
                    records_count=len(products_data)
                )
            
            logger.info(
                f"Completed extraction with products separation",
                platform=platform,
                entity=entity,
                bills_count=len(bills_data) if bills_data else 0,
                products_count=len(products_data) if products_data else 0
            )
    else:
        # Standard extraction for other entities
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
        
        # Upload to GCS với platform prefix (parquet)
        entity_path = f"{platform}/{entity}"
        metadata = {
            "platform": platform,
            "entity": entity,
            "extraction_timestamp": datetime.utcnow().isoformat(),
            "record_count": len(data),
            "incremental": incremental
        }
        
        partition_date = None
        if from_date:
            partition_date = from_date.date()
        
        loader.upload_parquet_by_date(
            entity=entity_path,
            data=data,
            partition_date=partition_date,
            metadata=metadata,
            overwrite_partition=True
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
    parser.add_argument(
        "--batch-days",
        type=int,
        default=30,
        help="Number of days to process in each batch (default: 30). Use smaller batches to avoid timeout, larger to reduce job count"
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
                
                # Process entity (extractor will handle date range splitting internally if needed)
                extract_entity(
                    platform=args.platform,
                    entity=entity,
                    loader=loader,
                    watermark_tracker=watermark_tracker,
                    incremental=incremental,
                    from_date=from_date,
                    to_date=to_date
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
        
        # Auto-setup BigQuery External Tables
        logger.info("Setting up BigQuery External Tables...")
        try:
            bq_setup = BigQueryExternalTableSetup()
            bq_setup.setup_all_tables(platforms=[args.platform] if args.platform else None)
            logger.info("BigQuery External Tables setup completed")
        except Exception as e:
            logger.warning(f"Failed to setup BigQuery External Tables", error=str(e))
            # Don't fail the entire pipeline if table setup fails
    
    except Exception as e:
        logger.error(f"ETL pipeline failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
