"""
Orchestrator cho toàn bộ ETL pipeline.
File này điều phối việc chạy các phases:
1. Bronze extraction (Extract từ API → GCS) - hỗ trợ multi-platform
2. Silver transformation (GCS → BigQuery Silver)
3. Gold aggregation (Silver → BigQuery Gold)
"""
import sys
import argparse
from datetime import datetime
from typing import Dict, List
from src.config import settings
from src.extractors import registry, list_available_platforms, list_available_entities
from src.loaders.gcs_loader import GCSLoader
from src.loaders.watermark import WatermarkTracker
from src.transformers.bronze_to_silver import BronzeToSilverTransformer
from src.transformers.silver_to_gold import SilverToGoldTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_bronze_extraction(
    platforms: List[str] = None,
    entities: List[str] = None,
    incremental: bool = True
) -> dict:
    """
    Chạy Bronze layer extraction cho nhiều platforms và entities.
    
    Args:
        platforms: Danh sách platforms (None = tất cả)
        entities: Danh sách entities (None = tất cả cho mỗi platform)
        incremental: Có dùng incremental extraction hay không
        
    Returns:
        dict: Kết quả extraction theo platform và entity
    """
    logger.info("=" * 60)
    logger.info("PHASE 1: BRONZE EXTRACTION")
    logger.info("=" * 60)
    
    results = {}
    loader = GCSLoader(bucket_name=settings.bronze_bucket)
    watermark_tracker = WatermarkTracker()
    
    # Determine platforms to process
    if platforms is None:
        platforms = list_available_platforms()
    
    logger.info(
        f"Processing platforms",
        platforms=platforms,
        count=len(platforms)
    )
    
    # Process each platform
    for platform in platforms:
        results[platform] = {}
        
        # Determine entities for this platform
        if entities is None:
            platform_entities = list_available_entities(platform)
        else:
            platform_entities = entities
        
        if not platform_entities:
            logger.warning(
                f"No entities available for platform",
                platform=platform
            )
            continue
        
        logger.info(
            f"Processing platform",
            platform=platform,
            entities=platform_entities
        )
        
        # Process each entity
        for entity in platform_entities:
            try:
                logger.info(
                    f"Extracting entity",
                    platform=platform,
                    entity=entity
                )
                
                # Get extractor từ registry
                extractor_class = registry.get(platform, entity)
                if not extractor_class:
                    logger.warning(
                        f"Extractor not found, skipping",
                        platform=platform,
                        entity=entity
                    )
                    results[platform][entity] = {"status": "skipped", "reason": "extractor_not_found"}
                    continue
                
                # Create extractor instance
                extractor = registry.create_instance(platform, entity)
                if not extractor:
                    logger.error(
                        f"Failed to create extractor instance",
                        platform=platform,
                        entity=entity
                    )
                    results[platform][entity] = {"status": "error", "error": "failed_to_create_instance"}
                    continue
                
                # Get incremental range nếu cần
                extractor_kwargs = {}
                if incremental:
                    watermark_key = f"{platform}_{entity}"
                    from_date, to_date = watermark_tracker.get_incremental_range(
                        watermark_key,
                        lookback_hours=1
                    )
                    extractor_kwargs["updated_at_from"] = from_date
                    extractor_kwargs["updated_at_to"] = to_date
                    extractor_kwargs["incremental"] = True
                else:
                    extractor_kwargs["incremental"] = False
                
                # Extract data
                data = extractor.extract(**extractor_kwargs)
                
                if not data:
                    logger.info(
                        f"No data to upload",
                        platform=platform,
                        entity=entity
                    )
                    results[platform][entity] = {"count": 0, "status": "no_data"}
                    continue
                
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
                
                results[platform][entity] = {"count": len(data), "status": "success"}
                
            except Exception as e:
                logger.error(
                    f"Error extracting entity",
                    error=str(e),
                    platform=platform,
                    entity=entity
                )
                results[platform][entity] = {"status": "error", "error": str(e)}
                continue
    
    logger.info("Bronze extraction completed", results=results)
    return results


def run_silver_transformation() -> dict:
    """
    Chạy Silver layer transformation.
    
    Returns:
        dict: Kết quả transformation
    """
    logger.info("=" * 60)
    logger.info("PHASE 2: SILVER TRANSFORMATION")
    logger.info("=" * 60)
    
    transformer = BronzeToSilverTransformer()
    results = transformer.transform_all()
    
    logger.info("Silver transformation completed", results=results)
    return results


def run_gold_aggregation() -> dict:
    """
    Chạy Gold layer aggregation.
    
    Returns:
        dict: Kết quả aggregation
    """
    logger.info("=" * 60)
    logger.info("PHASE 3: GOLD AGGREGATION")
    logger.info("=" * 60)
    
    transformer = SilverToGoldTransformer()
    results = transformer.transform_all()
    
    logger.info("Gold aggregation completed", results=results)
    return results


def main():
    """
    Main entry point cho ETL orchestrator.
    
    Chạy toàn bộ pipeline từ Bronze → Silver → Gold
    hoặc chỉ chạy một phase cụ thể.
    Hỗ trợ multi-platform và multi-entity.
    """
    parser = argparse.ArgumentParser(description="ETL Pipeline Orchestrator")
    parser.add_argument(
        "--phase",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Phase to run"
    )
    parser.add_argument(
        "--platform",
        type=str,
        help="Platform to extract from (nhanh, facebook, etc.). Default: all platforms"
    )
    parser.add_argument(
        "--entity",
        type=str,
        help="Entity to extract (bills, products, etc.). Default: all entities"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Use incremental extraction for Bronze (default: True)"
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Perform full sync (overrides --incremental)"
    )
    
    args = parser.parse_args()
    
    incremental = not args.full_sync if args.full_sync else args.incremental
    
    try:
        start_time = datetime.utcnow()
        logger.info(
            "Starting ETL pipeline",
            phase=args.phase,
            platform=args.platform,
            entity=args.entity,
            incremental=incremental,
            project=settings.gcp_project
        )
        
        all_results = {}
        
        # Run Bronze
        if args.phase in ["bronze", "all"]:
            platforms = [args.platform] if args.platform else None
            entities = [args.entity] if args.entity else None
            all_results["bronze"] = run_bronze_extraction(
                platforms=platforms,
                entities=entities,
                incremental=incremental
            )
        
        # Run Silver
        if args.phase in ["silver", "all"]:
            all_results["silver"] = run_silver_transformation()
        
        # Run Gold
        if args.phase in ["gold", "all"]:
            all_results["gold"] = run_gold_aggregation()
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(
            "ETL pipeline completed successfully",
            duration_seconds=duration,
            results=all_results
        )
        
        # Print summary
        print("\n" + "=" * 60)
        print("ETL PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Results: {all_results}")
        print("=" * 60)
    
    except Exception as e:
        logger.error(f"ETL pipeline failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
