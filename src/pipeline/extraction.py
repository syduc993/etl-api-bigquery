"""
Core extraction logic for ETL pipeline.
This module handles the extraction and loading of entities from various platforms to the Bronze layer.
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.shared.logging import get_logger
from src.shared.gcs import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.shared.nhanh import NhanhApiClient

# Feature imports - organized by platform
from src.features.nhanh.bills import BillExtractor

logger = get_logger(__name__)

# Feature registry - maps (platform, entity) to extractor class
FEATURE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "nhanh": {
        "bills": BillExtractor,
    },
    # Future platforms will be added here
}


def list_available_platforms() -> List[str]:
    """List all available platforms."""
    return list(FEATURE_REGISTRY.keys())


def list_available_entities(platform: str) -> List[str]:
    """List all available entities for a platform."""
    return list(FEATURE_REGISTRY.get(platform, {}).keys())


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
    Extract and load an entity from a platform into the Bronze layer.
    
    Args:
        platform: Platform name (e.g., 'nhanh', 'facebook')
        entity: Entity name (e.g., 'bills', 'orders')
        loader: GCSLoader instance
        watermark_tracker: WatermarkTracker instance
        incremental: Whether to use incremental extraction
        from_date: Start date (optional, overrides incremental)
        to_date: End date (optional, overrides incremental)
        **extractor_kwargs: Specific arguments for the extractor
    """
    logger.info(
        f"Starting extraction",
        platform=platform,
        entity=entity,
        incremental=incremental
    )
    
    # Get extractor from feature registry
    platform_extractors = FEATURE_REGISTRY.get(platform)
    if not platform_extractors:
        logger.error(f"Platform not found", platform=platform)
        raise ValueError(f"Platform not found: {platform}")
    
    extractor_class = platform_extractors.get(entity)
    if not extractor_class:
        logger.error(
            f"Extractor not found",
            platform=platform,
            entity=entity
        )
        raise ValueError(f"Extractor not found for {platform}.{entity}")
    
    # Create extractor instance
    extractor = extractor_class()
    
    # Handle incremental logic if dates are not provided
    if incremental and not from_date and not to_date:
        watermark_key = f"{platform}_{entity}"
        incr_from, incr_to = watermark_tracker.get_incremental_range(
            watermark_key,
            lookback_hours=1
        )
        extractor_kwargs["updated_at_from"] = incr_from
        extractor_kwargs["updated_at_to"] = incr_to
    elif from_date and to_date:
        extractor_kwargs["from_date"] = from_date
        extractor_kwargs["to_date"] = to_date
    
    # Special handling for Nhanh bills (fragmentation by day for large datasets)
    if platform == "nhanh" and entity == "bills" and hasattr(extractor, "extract_with_products"):
        # Create a copy of kwargs without date range to avoid "multiple values" error
        nhanh_kwargs = extractor_kwargs.copy()
        nhanh_kwargs.pop("from_date", None)
        nhanh_kwargs.pop("to_date", None)
        
        _extract_nhanh_bills(
            platform, entity, extractor, loader, watermark_tracker, 
            incremental, from_date, to_date, **nhanh_kwargs
        )
    else:
        _extract_standard(
            platform, entity, extractor, loader, watermark_tracker,
            incremental, from_date, **extractor_kwargs
        )


def _extract_nhanh_bills(
    platform, entity, extractor, loader, watermark_tracker,
    incremental, from_date, to_date, **extractor_kwargs
):
    """Helper to handle complex Nhanh bills extraction with product separation."""
    # If date range is provided, process day by day
    if from_date and to_date:
        date_splitter = NhanhApiClient()
        day_chunks = date_splitter.split_date_range_by_day(from_date, to_date)
        
        logger.info(
            f"Processing {len(day_chunks)} days separately",
            platform=platform, entity=entity, days=len(day_chunks)
        )
        
        total_bills = 0
        total_products = 0
        
        for day_idx, (day_from, day_to) in enumerate(day_chunks, 1):
            day_date = day_from.date()
            logger.info(f"Processing day {day_idx}/{len(day_chunks)}: {day_date}")
            
            day_kwargs = extractor_kwargs.copy()
            day_kwargs["from_date"] = day_from
            day_kwargs["to_date"] = day_to
            day_kwargs["process_by_day"] = True
            
            try:
                bills_data, products_data = extractor.extract_with_products(**day_kwargs)
            except Exception as e:
                logger.error(f"Error extracting day {day_date}: {e}")
                continue
            
            _upload_and_log(loader, platform, entity, bills_data, day_date, incremental)
            _upload_and_log(loader, platform, "bill_products", products_data, day_date, incremental)
            
            total_bills += len(bills_data)
            total_products += len(products_data)
        
        # Update watermark at the end
        if total_bills > 0:
            watermark_tracker.update_watermark(f"{platform}_{entity}", datetime.utcnow(), total_bills)
        if total_products > 0:
            watermark_tracker.update_watermark(f"{platform}_bill_products", datetime.utcnow(), total_products)
            
    else:
        # Standard extraction (incremental or full sync without split)
        bills_data, products_data = extractor.extract_with_products(**extractor_kwargs)
        
        partition_date = from_date.date() if from_date else None
        
        if bills_data:
            _upload_and_log(loader, platform, entity, bills_data, partition_date, incremental)
            watermark_tracker.update_watermark(f"{platform}_{entity}", datetime.utcnow(), len(bills_data))
            
        if products_data:
            _upload_and_log(loader, platform, "bill_products", products_data, partition_date, incremental)
            watermark_tracker.update_watermark(f"{platform}_bill_products", datetime.utcnow(), len(products_data))


def _extract_standard(
    platform, entity, extractor, loader, watermark_tracker,
    incremental, from_date, **extractor_kwargs
):
    """Standard extraction for normal entities."""
    data = extractor.extract(**extractor_kwargs)
    if not data:
        logger.info(f"No data to upload for {platform}.{entity}")
        return
        
    partition_date = from_date.date() if from_date else None
    _upload_and_log(loader, platform, entity, data, partition_date, incremental)
    
    watermark_tracker.update_watermark(
        f"{platform}_{entity}",
        datetime.utcnow(),
        len(data)
    )


def _upload_and_log(loader, platform, entity, data, partition_date, incremental):
    """Helper to upload parquet and log."""
    if not data:
        return

    path = f"{platform}/{entity}"
    metadata = {
        "platform": platform,
        "entity": entity,
        "extraction_timestamp": datetime.utcnow().isoformat(),
        "record_count": len(data),
        "incremental": incremental,
        "date": partition_date.isoformat() if partition_date else None
    }
    
    loader.upload_parquet_by_date(
        entity=path,
        data=data,
        partition_date=partition_date,
        metadata=metadata,
        overwrite_partition=True
    )
    
    logger.info(f"Uploaded {len(data)} records for {platform}.{entity}")
