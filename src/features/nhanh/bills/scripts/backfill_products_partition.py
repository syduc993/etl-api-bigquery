"""
Backfill bill_date and extraction_timestamp for fact_sales_bills_product_v3_0
and migrate to new partition strategy (partition by bill_date instead of DATE(extraction_timestamp)).

Migration Strategy:
1. Add bill_date column to existing table (non-breaking)
2. Backfill bill_date and extraction_timestamp from bills table (JOIN)
3. Create new table with partition by bill_date
4. Copy data to new table
5. Verify data integrity
6. Swap tables (drop old, rename new)
"""
import sys
import os
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.config import settings
from src.shared.logging import get_logger

logger = get_logger(__name__)


def backfill_and_repartition(dry_run: bool = False):
    """
    Backfill bill_date and extraction_timestamp, then migrate to new partition strategy.
    
    Args:
        dry_run: If True, only show what would be done without executing
    """
    client = bigquery.Client(project=settings.gcp_project)
    project_id = settings.gcp_project
    dataset_id = settings.target_dataset
    table_id = f"{project_id}.{dataset_id}.fact_sales_bills_product_v3_0"
    bills_table_id = f"{project_id}.{dataset_id}.fact_sales_bills_v3_0"
    backup_table_id = f"{project_id}.{dataset_id}.fact_sales_bills_product_v3_0_backup"
    new_table_id = f"{project_id}.{dataset_id}.fact_sales_bills_product_v3_0_new"
    
    logger.info(f"Starting migration for {table_id}")
    logger.info(f"Dry run: {dry_run}")
    
    try:
        # Step 1: Check if table exists
        try:
            table = client.get_table(table_id)
            logger.info(f"Table exists: {table_id}")
            logger.info(f"Current row count: {table.num_rows:,}")
            logger.info(f"Current partition field: {table.time_partitioning.field if table.time_partitioning else 'None'}")
        except NotFound:
            logger.error(f"Table {table_id} does not exist. Cannot proceed with migration.")
            return False
        
        # Step 2: Check if bill_date column already exists
        existing_columns = [field.name for field in table.schema]
        has_bill_date = "bill_date" in existing_columns
        
        if not dry_run:
            # Step 3: Add bill_date column if it doesn't exist
            if not has_bill_date:
                logger.info("Adding bill_date column...")
                alter_sql = f"""
                ALTER TABLE `{table_id}`
                ADD COLUMN IF NOT EXISTS bill_date DATE
                """
                query_job = client.query(alter_sql)
                query_job.result()
                logger.info("✅ Added bill_date column")
            else:
                logger.info("bill_date column already exists")
            
            # Step 4: Backfill bill_date and extraction_timestamp from bills table
            logger.info("Backfilling bill_date and extraction_timestamp from bills table...")
            # Use MERGE with deduplicated bills (handle duplicates by taking most recent)
            backfill_sql = f"""
            MERGE `{table_id}` p
            USING (
                SELECT 
                    id,
                    date,
                    extraction_timestamp
                FROM (
                    SELECT 
                        id,
                        date,
                        extraction_timestamp,
                        ROW_NUMBER() OVER (PARTITION BY id ORDER BY extraction_timestamp DESC) as rn
                    FROM `{bills_table_id}`
                )
                WHERE rn = 1
            ) b
            ON p.bill_id = b.id
            WHEN MATCHED AND (p.bill_date IS NULL OR p.extraction_timestamp IS NULL) THEN
                UPDATE SET
                    bill_date = b.date,
                    extraction_timestamp = b.extraction_timestamp
            """
            query_job = client.query(backfill_sql)
            query_job.result()
            updated_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
            logger.info(f"✅ Backfilled {updated_rows:,} rows")
            
            # Step 5: Verify backfill
            verify_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(bill_date) as rows_with_bill_date,
                COUNT(extraction_timestamp) as rows_with_extraction_timestamp,
                COUNT(*) - COUNT(bill_date) as null_bill_date,
                COUNT(*) - COUNT(extraction_timestamp) as null_extraction_timestamp
            FROM `{table_id}`
            """
            verify_job = client.query(verify_sql)
            verify_results = list(verify_job.result())[0]
            logger.info(f"Verification after backfill:")
            logger.info(f"  Total rows: {verify_results.total_rows:,}")
            logger.info(f"  Rows with bill_date: {verify_results.rows_with_bill_date:,}")
            logger.info(f"  Rows with extraction_timestamp: {verify_results.rows_with_extraction_timestamp:,}")
            logger.info(f"  NULL bill_date: {verify_results.null_bill_date:,}")
            logger.info(f"  NULL extraction_timestamp: {verify_results.null_extraction_timestamp:,}")
            
            if verify_results.null_bill_date > 0 or verify_results.null_extraction_timestamp > 0:
                logger.warning(f"⚠️  Still have NULL values. Some products may not have matching bills.")
            
            # Step 6: Create backup table
            logger.info("Creating backup table...")
            backup_sql = f"""
            CREATE TABLE IF NOT EXISTS `{backup_table_id}`
            AS SELECT * FROM `{table_id}`
            """
            query_job = client.query(backup_sql)
            query_job.result()
            logger.info(f"✅ Created backup table: {backup_table_id}")
            
            # Step 7: Create new table with partition by bill_date
            logger.info("Creating new table with partition by bill_date...")
            create_new_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{new_table_id}`
            PARTITION BY bill_date
            CLUSTER BY bill_id, product_id
            AS 
            SELECT 
                bill_id,
                product_id,
                product_code,
                product_barcode,
                product_name,
                quantity,
                price,
                discount,
                vat_percent,
                vat_amount,
                amount,
                bill_date,
                extraction_timestamp
            FROM `{table_id}`
            WHERE bill_date IS NOT NULL
            """
            query_job = client.query(create_new_table_sql)
            query_job.result()
            logger.info(f"✅ Created new table: {new_table_id}")
            
            # Step 8: Verify new table
            new_table = client.get_table(new_table_id)
            logger.info(f"New table row count: {new_table.num_rows:,}")
            logger.info(f"New table partition field: {new_table.time_partitioning.field if new_table.time_partitioning else 'None'}")
            
            # Step 9: Verify row counts match
            count_old_sql = f"SELECT COUNT(*) as cnt FROM `{table_id}` WHERE bill_date IS NOT NULL"
            count_new_sql = f"SELECT COUNT(*) as cnt FROM `{new_table_id}`"
            
            old_count = list(client.query(count_old_sql).result())[0].cnt
            new_count = list(client.query(count_new_sql).result())[0].cnt
            
            logger.info(f"Old table (with bill_date): {old_count:,} rows")
            logger.info(f"New table: {new_count:,} rows")
            
            if old_count != new_count:
                logger.error(f"❌ Row count mismatch! Old: {old_count:,}, New: {new_count:,}")
                logger.error("Aborting migration. Please investigate.")
                return False
            
            # Step 10: Swap tables
            logger.info("Swapping tables...")
            
            # Create temporary name for old table
            temp_old_table_id = f"{project_id}.{dataset_id}.fact_sales_bills_product_v3_0_old"
            
            # Copy old table to temp name (as backup)
            logger.info(f"Copying old table to {temp_old_table_id}")
            copy_job = client.copy_table(table_id, temp_old_table_id)
            copy_job.result()
            logger.info("✅ Old table backed up to temp location")
            
            # Drop old table
            logger.info(f"Dropping old table: {table_id}")
            client.delete_table(table_id, not_found_ok=True)
            
            # Copy new table to original name
            logger.info(f"Copying {new_table_id} to {table_id}")
            copy_job = client.copy_table(new_table_id, table_id)
            copy_job.result()
            logger.info("✅ New table copied to original location")
            
            # Drop new table (we have it in original location now)
            logger.info(f"Dropping temporary new table: {new_table_id}")
            client.delete_table(new_table_id, not_found_ok=True)
            
            logger.info("✅ Migration completed successfully!")
            logger.info(f"Backup table available at: {backup_table_id}")
            logger.info(f"Old table (temp backup) available at: {temp_old_table_id}")
            
        else:
            # Dry run: just show what would be done
            logger.info("DRY RUN - Would execute:")
            logger.info("  1. Add bill_date column (if not exists)")
            logger.info("  2. Backfill bill_date and extraction_timestamp from bills table")
            logger.info("  3. Create backup table")
            logger.info("  4. Create new table with partition by bill_date")
            logger.info("  5. Verify data integrity")
            logger.info("  6. Swap tables")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return False


def main():
    """Main entry point for migration script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Backfill and repartition fact_sales_bills_product_v3_0")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing"
    )
    
    args = parser.parse_args()
    
    success = backfill_and_repartition(dry_run=args.dry_run)
    
    if success:
        logger.info("Migration script completed successfully")
        sys.exit(0)
    else:
        logger.error("Migration script failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

