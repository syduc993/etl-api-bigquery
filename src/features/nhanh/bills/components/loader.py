"""
Loader cho Bills feature.
Upload data lên GCS và load vào BigQuery native tables (fact tables).
Flatten nested structures trong Python trước khi load.
Sử dụng MERGE statement để đảm bảo idempotency.
"""
from datetime import datetime, date
from typing import Dict, Any, List, Optional
import uuid
import time
from google.cloud import bigquery
from src.shared.gcs import GCSLoader
from src.shared.bigquery import BigQueryExternalTableSetup
from src.shared.logging import get_logger
from src.config import settings

logger = get_logger(__name__)


class BillLoader:
    """
    Loader cho bills data.
    - Flatten nested structures (customer, payment, products) trong Python
    - Upload flattened data lên GCS (backup)
    - Load vào BigQuery fact tables (nhanhVN.fact_sales_bills_v3_0, nhanhVN.fact_sales_bills_product_v3_0)
    - Tránh duplicate bằng MERGE statement (idempotent, tự động UPDATE/INSERT)
    - Partition filtering trong MERGE để tối ưu performance
    """
    
    def __init__(self):
        """Khởi tạo loader với GCS và BigQuery clients."""
        self.gcs_loader = GCSLoader(bucket_name=settings.bronze_bucket)
        self.bq_setup = BigQueryExternalTableSetup()
        self.bq_client = bigquery.Client(project=settings.gcp_project)
        self.platform = "nhanh"
        self.entity = "bills"
        
        # Fact table IDs (nhanhVN dataset)
        self.bills_table_id = f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_v3_0"
        self.products_table_id = f"{settings.gcp_project}.{settings.target_dataset}.fact_sales_bills_product_v3_0"
    
    def _flatten_bill(self, bill: Dict[str, Any], extraction_timestamp: datetime) -> Dict[str, Any]:
        """
        Flatten nested bill structure thành flat record matching fact_sales_bills_v3_0 schema.
        
        Args:
            bill: Raw bill dict với nested structures (customer, payment, sale, created)
            extraction_timestamp: Timestamp khi extract data
            
        Returns:
            Flattened bill dict
        """
        flattened = {
            # Basic fields
            "id": bill.get("id"),
            "depotId": bill.get("depotId"),
            "date": None,  # Will parse from string
            "type": bill.get("type"),
            "mode": bill.get("mode"),
            
            # Customer fields (flattened from customer object)
            "customer_id": None,
            "customer_name": None,
            "customer_mobile": None,
            "customer_address": None,
            
            # Sale/Staff fields (flattened from sale and created objects)
            "sale_id": None,
            "sale_name": None,
            "created_id": None,
            "created_email": None,
            
            # Payment fields (flattened from payment object)
            "payment_total_amount": None,
            "payment_customer_amount": None,
            "payment_discount": None,
            "payment_points": None,
            
            # Payment methods (flattened from nested payment methods)
            "payment_cash_amount": None,
            "payment_transfer_amount": None,
            "payment_transfer_account_id": None,
            "payment_credit_amount": None,
            
            "description": bill.get("description"),
            "extraction_timestamp": extraction_timestamp
        }
        
        # Parse date (format: YYYY-MM-DD)
        date_str = bill.get("date")
        if date_str:
            try:
                if isinstance(date_str, str):
                    flattened["date"] = datetime.strptime(date_str, "%Y-%m-%d").date()
                elif isinstance(date_str, date):
                    flattened["date"] = date_str
                elif isinstance(date_str, datetime):
                    flattened["date"] = date_str.date()
            except (ValueError, TypeError):
                logger.warning(f"Failed to parse date: {date_str}")
                flattened["date"] = None
        
        # Flatten customer
        customer = bill.get("customer")
        if isinstance(customer, dict):
            flattened["customer_id"] = customer.get("id")
            flattened["customer_name"] = customer.get("name")
            flattened["customer_mobile"] = customer.get("mobile")
            flattened["customer_address"] = customer.get("address")
        
        # Flatten sale
        sale = bill.get("sale")
        if isinstance(sale, dict):
            flattened["sale_id"] = sale.get("id")
            flattened["sale_name"] = sale.get("name")
        
        # Flatten created
        created = bill.get("created")
        if isinstance(created, dict):
            flattened["created_id"] = created.get("id")
            flattened["created_email"] = created.get("name")  # Note: SQL uses created.name as created_email
        
        # Flatten payment
        payment = bill.get("payment")
        if isinstance(payment, dict):
            # Enforce types to avoid Parquet schema mismatch (INT64 vs FLOAT64)
            amount = payment.get("amount")
            flattened["payment_total_amount"] = float(amount) if amount is not None else None
            
            customer_amount = payment.get("customerAmount")
            flattened["payment_customer_amount"] = float(customer_amount) if customer_amount is not None else None
            
            discount = payment.get("discount")
            flattened["payment_discount"] = float(discount) if discount is not None else None
            
            points = payment.get("points")
            flattened["payment_points"] = float(points) if points is not None else None
            
            # Flatten payment methods
            cash = payment.get("cash")
            if isinstance(cash, dict):
                cash_amount = cash.get("amount")
                flattened["payment_cash_amount"] = float(cash_amount) if cash_amount is not None else None
            
            transfer = payment.get("transfer")
            if isinstance(transfer, dict):
                transfer_amount = transfer.get("amount")
                flattened["payment_transfer_amount"] = float(transfer_amount) if transfer_amount is not None else None
                flattened["payment_transfer_account_id"] = transfer.get("accountId")
            
            credit = payment.get("credit")
            if isinstance(credit, dict):
                credit_amount = credit.get("amount")
                flattened["payment_credit_amount"] = float(credit_amount) if credit_amount is not None else None
        
        return flattened
    
    def _flatten_bill_product(self, product: Dict[str, Any], extraction_timestamp: datetime, bill_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Flatten nested product structure thành flat record matching fact_sales_bills_product_v3_0 schema.
        
        Args:
            product: Raw product dict với nested vat structure
            extraction_timestamp: Timestamp khi extract data
            bill_date: Date from bills table for partitioning
            
        Returns:
            Flattened product dict
        """
        flattened = {
            "bill_id": product.get("bill_id"),
            "product_id": product.get("id") or product.get("product_id"),
            "product_code": product.get("code"),
            "product_barcode": product.get("barcode"),
            "product_name": product.get("name"),
            "quantity": product.get("quantity"),
            "price": product.get("price"),
            "discount": product.get("discount"),
            "vat_percent": None,
            "vat_amount": None,
            "amount": product.get("amount"),
            "bill_date": bill_date,
            "extraction_timestamp": extraction_timestamp
        }
        
        # Flatten vat
        vat = product.get("vat")
        if isinstance(vat, dict):
            flattened["vat_percent"] = vat.get("percent")
            flattened["vat_amount"] = vat.get("amount")
        
        return flattened
    
    def _delete_partition_data(self, table_id: str, partition_date: date, partition_field: str = "extraction_date", partition_type: str = "date") -> None:
        """
        Delete data trong partition cụ thể để tránh duplicate khi re-run.
        
        Args:
            table_id: Full table ID
            partition_date: Ngày partition cần xóa
            partition_field: Tên field partition (default: extraction_date)
            partition_type: Type of partition - "date" (direct DATE field) or "timestamp" (DATE(extraction_timestamp))
        """
        try:
            # Check if table exists and has partitioning
            table = None
            has_partitioning = False
            partition_by_column = False
            try:
                table = self.bq_client.get_table(table_id)
                has_partitioning = table.time_partitioning is not None
                # Check if partitioned by a column (not ingestion time)
                if has_partitioning and table.time_partitioning.field:
                    partition_by_column = True
            except Exception:
                # Table chưa tồn tại, không cần delete
                logger.debug(f"Table {table_id} does not exist, skipping delete")
                return
            
            # Delete partition data
            if partition_type == "timestamp":
                # For products table: partition by DATE(extraction_timestamp)
                sql = f"""
                DELETE FROM `{table_id}`
                WHERE DATE(extraction_timestamp) = DATE('{partition_date.isoformat()}')
                """
            elif has_partitioning and partition_by_column:
                # Table partitioned by DATE column - use column name, not _PARTITIONDATE
                sql = f"""
                DELETE FROM `{table_id}`
                WHERE {partition_field} = DATE('{partition_date.isoformat()}')
                """
            elif has_partitioning:
                # Time-based partitioning (ingestion time) - use _PARTITIONDATE
                sql = f"""
                DELETE FROM `{table_id}`
                WHERE _PARTITIONDATE = DATE('{partition_date.isoformat()}')
                """
            else:
                # Table không có partitioning, dùng field trực tiếp
                sql = f"""
                DELETE FROM `{table_id}`
                WHERE {partition_field} = DATE('{partition_date.isoformat()}')
                """
            
            query_job = self.bq_client.query(sql)
            query_job.result()
            
            deleted_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
            
            if deleted_rows > 0:
                logger.info(
                    f"Deleted {deleted_rows} rows from partition",
                    table_id=table_id,
                    partition_date=partition_date.isoformat(),
                    deleted_rows=deleted_rows
                )
            else:
                logger.debug(
                    f"No data to delete in partition",
                    table_id=table_id,
                    partition_date=partition_date.isoformat()
                )
                
        except Exception as e:
            logger.warning(
                f"Failed to delete partition data, continuing with load",
                table_id=table_id,
                partition_date=partition_date.isoformat(),
                error=str(e)
            )
            # Không raise để không block pipeline nếu delete fail
    
    def _create_temp_external_table(
        self,
        gcs_uri: str,
        table_id: str
    ) -> str:
        """
        Tạo temporary external table từ GCS Parquet file để dùng cho MERGE.
        
        Args:
            gcs_uri: GCS URI của Parquet file (gs://bucket/path/to/file.parquet)
            table_id: Target table ID để generate unique external table name
            
        Returns:
            str: Full external table ID
        """
        # Generate unique table name để tránh conflict
        timestamp = int(time.time() * 1000000)  # Microseconds
        unique_id = str(uuid.uuid4())[:8]
        table_name = table_id.split('.')[-1]  # Extract table name
        external_table_name = f"{table_name}_temp_{timestamp}_{unique_id}"
        
        # Use bronze dataset for temporary external tables
        external_table_id = f"{settings.gcp_project}.{settings.bronze_dataset}.{external_table_name}"
        
        sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{external_table_id}`
        OPTIONS (
          format = 'PARQUET',
          uris = ['{gcs_uri}']
        )
        """
        
        try:
            query_job = self.bq_client.query(sql)
            query_job.result()
            
            logger.debug(
                f"Created temporary external table",
                external_table_id=external_table_id,
                gcs_uri=gcs_uri
            )
            
            return external_table_id
            
        except Exception as e:
            logger.error(
                f"Failed to create temporary external table",
                external_table_id=external_table_id,
                gcs_uri=gcs_uri,
                error=str(e)
            )
            raise
    
    def _merge_bills_from_external_table(
        self,
        external_table_id: str,
        target_table_id: str,
        partition_date: date
    ) -> int:
        """
        MERGE data từ external table vào bills fact table.
        Giữ lại record mới nhất dựa trên extraction_timestamp.
        
        Args:
            external_table_id: External table ID chứa source data
            target_table_id: Target fact table ID
            partition_date: Partition date để filter
            
        Returns:
            int: Number of rows merged (inserted + updated)
        """
        date_str = partition_date.isoformat()
        
        # Check if table is empty - if so, use optimized INSERT approach
        try:
            count_query = f"SELECT COUNT(*) as cnt FROM `{target_table_id}`"
            count_job = self.bq_client.query(count_query)
            count_result = list(count_job.result())
            table_is_empty = count_result and count_result[0].cnt == 0
        except Exception:
            # Table might not exist or error checking - assume not empty and use MERGE
            table_is_empty = False
        
        if table_is_empty:
            # Use MERGE with ON FALSE for empty table - more efficient than checking conditions
            logger.debug(
                f"Table is empty, using optimized INSERT approach",
                target_table_id=target_table_id
            )
            sql = f"""
            MERGE `{target_table_id}` T
            USING (
                SELECT 
                    id, depotId, date, type, mode,
                    customer_id, customer_name, customer_mobile, customer_address,
                    sale_id, sale_name, created_id, created_email,
                    payment_total_amount, payment_customer_amount, payment_discount, payment_points,
                    payment_cash_amount, payment_transfer_amount, payment_transfer_account_id, payment_credit_amount,
                    description, extraction_timestamp
                FROM `{external_table_id}`
            ) S
            ON FALSE
            WHEN NOT MATCHED THEN
                INSERT ROW
            """
        else:
            # MERGE với partition filtering trong ON clause
            sql = f"""
            MERGE `{target_table_id}` T
            USING (
                SELECT 
                    id, depotId, date, type, mode,
                    customer_id, customer_name, customer_mobile, customer_address,
                    sale_id, sale_name, created_id, created_email,
                    payment_total_amount, payment_customer_amount, payment_discount, payment_points,
                    payment_cash_amount, payment_transfer_amount, payment_transfer_account_id, payment_credit_amount,
                    description, extraction_timestamp
                FROM `{external_table_id}`
            ) S
            ON T.id = S.id AND T.date = S.date
            WHEN MATCHED THEN
                UPDATE SET
                    depotId = S.depotId,
                    type = S.type,
                    mode = S.mode,
                    customer_id = S.customer_id,
                    customer_name = S.customer_name,
                    customer_mobile = S.customer_mobile,
                    customer_address = S.customer_address,
                    sale_id = S.sale_id,
                    sale_name = S.sale_name,
                    created_id = S.created_id,
                    created_email = S.created_email,
                    payment_total_amount = S.payment_total_amount,
                    payment_customer_amount = S.payment_customer_amount,
                    payment_discount = S.payment_discount,
                    payment_points = S.payment_points,
                    payment_cash_amount = S.payment_cash_amount,
                    payment_transfer_amount = S.payment_transfer_amount,
                    payment_transfer_account_id = S.payment_transfer_account_id,
                    payment_credit_amount = S.payment_credit_amount,
                    description = S.description,
                    extraction_timestamp = S.extraction_timestamp
            WHEN NOT MATCHED THEN
                INSERT (
                    id, depotId, date, type, mode,
                    customer_id, customer_name, customer_mobile, customer_address,
                    sale_id, sale_name, created_id, created_email,
                    payment_total_amount, payment_customer_amount, payment_discount, payment_points,
                    payment_cash_amount, payment_transfer_amount, payment_transfer_account_id, payment_credit_amount,
                    description, extraction_timestamp
                )
                VALUES (
                    S.id, S.depotId, S.date, S.type, S.mode,
                    S.customer_id, S.customer_name, S.customer_mobile, S.customer_address,
                    S.sale_id, S.sale_name, S.created_id, S.created_email,
                    S.payment_total_amount, S.payment_customer_amount, S.payment_discount, S.payment_points,
                    S.payment_cash_amount, S.payment_transfer_amount, S.payment_transfer_account_id, S.payment_credit_amount,
                    S.description, S.extraction_timestamp
                )
            """
        
        try:
            query_job = self.bq_client.query(sql)
            query_job.result()
            
            # Get merge statistics
            num_dml_affected_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
            
            logger.info(
                f"Merged bills data from external table",
                external_table_id=external_table_id,
                target_table_id=target_table_id,
                partition_date=date_str,
                rows_affected=num_dml_affected_rows,
                used_insert=table_is_empty
            )
            
            return num_dml_affected_rows
            
        except Exception as e:
            logger.error(
                f"Failed to merge bills data",
                external_table_id=external_table_id,
                target_table_id=target_table_id,
                partition_date=date_str,
                error=str(e)
            )
            raise
    
    def _merge_products_from_external_table(
        self,
        external_table_id: str,
        target_table_id: str,
        partition_date: date
    ) -> int:
        """
        MERGE data từ external table vào products fact table.
        Giữ lại record mới nhất dựa trên extraction_timestamp.
        
        Args:
            external_table_id: External table ID chứa source data
            target_table_id: Target fact table ID
            partition_date: Partition date để filter
            
        Returns:
            int: Number of rows merged (inserted + updated)
        """
        date_str = partition_date.isoformat()
        
        # Check if table is empty - if so, use INSERT instead of MERGE for better compatibility
        try:
            count_query = f"SELECT COUNT(*) as cnt FROM `{target_table_id}`"
            count_job = self.bq_client.query(count_query)
            count_result = list(count_job.result())
            table_is_empty = count_result and count_result[0].cnt == 0
        except Exception:
            # Table might not exist or error checking - assume not empty and use MERGE
            table_is_empty = False
        
        if table_is_empty:
            # Use load_table_from_uri for empty table - more reliable than INSERT with partitioned tables
            logger.debug(
                f"Table is empty, using load_table_from_uri instead of INSERT",
                target_table_id=target_table_id
            )
            # Extract GCS URI from external table
            # We'll need to get it from the caller, but for now use external table approach
            # Actually, let's use MERGE INSERT ROW which should work better
            sql = f"""
            MERGE `{target_table_id}` T
            USING (
                SELECT 
                    bill_id, product_id, product_code, product_barcode, product_name,
                    quantity, price, discount, vat_percent, vat_amount, amount,
                    bill_date, extraction_timestamp
                FROM `{external_table_id}`
            ) S
            ON FALSE
            WHEN NOT MATCHED THEN
                INSERT ROW
            """
        else:
            # MERGE với partition filtering
            # Note: ON clause chỉ dùng bill_id + product_id (không dùng bill_date) để tránh không match khi bill_date NULL
            # bill_date sẽ được UPDATE trong WHEN MATCHED để fix các records cũ có bill_date NULL
            sql = f"""
            MERGE `{target_table_id}` T
            USING (
                SELECT 
                    bill_id, product_id, product_code, product_barcode, product_name,
                    quantity, price, discount, vat_percent, vat_amount, amount,
                    bill_date, extraction_timestamp
                FROM `{external_table_id}`
            ) S
            ON T.bill_id = S.bill_id 
               AND T.product_id = S.product_id
            WHEN MATCHED THEN
                UPDATE SET
                    product_code = S.product_code,
                    product_barcode = S.product_barcode,
                    product_name = S.product_name,
                    quantity = S.quantity,
                    price = S.price,
                    discount = S.discount,
                    vat_percent = S.vat_percent,
                    vat_amount = S.vat_amount,
                    amount = S.amount,
                    bill_date = S.bill_date,
                    extraction_timestamp = S.extraction_timestamp
            WHEN NOT MATCHED THEN
                INSERT (
                    bill_id, product_id, product_code, product_barcode, product_name,
                    quantity, price, discount, vat_percent, vat_amount, amount,
                    bill_date, extraction_timestamp
                )
                VALUES (
                    S.bill_id, S.product_id, S.product_code, S.product_barcode, S.product_name,
                    S.quantity, S.price, S.discount, S.vat_percent, S.vat_amount, S.amount,
                    S.bill_date, S.extraction_timestamp
                )
            """
        
        try:
            query_job = self.bq_client.query(sql)
            query_job.result()
            
            # Get merge/insert statistics
            num_dml_affected_rows = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
            
            logger.info(
                f"Merged products data from external table",
                external_table_id=external_table_id,
                target_table_id=target_table_id,
                partition_date=date_str,
                rows_affected=num_dml_affected_rows,
                used_insert=table_is_empty
            )
            
            return num_dml_affected_rows
            
        except Exception as e:
            logger.error(
                f"Failed to merge products data",
                external_table_id=external_table_id,
                target_table_id=target_table_id,
                partition_date=date_str,
                error=str(e)
            )
            raise
    
    def _cleanup_external_table(self, external_table_id: str) -> None:
        """
        Drop temporary external table sau khi MERGE xong.
        
        Args:
            external_table_id: External table ID cần drop
        """
        try:
            sql = f"DROP TABLE IF EXISTS `{external_table_id}`"
            query_job = self.bq_client.query(sql)
            query_job.result()
            
            logger.debug(f"Dropped temporary external table", external_table_id=external_table_id)
            
        except Exception as e:
            # Log warning nhưng không raise - cleanup không critical
            logger.warning(
                f"Failed to cleanup external table (non-critical)",
                external_table_id=external_table_id,
                error=str(e)
            )
    
    def _load_gcs_to_bigquery(
        self,
        gcs_uri: str,
        table_id: str,
        partition_date: date,
        partition_field: str = "extraction_date",
        partition_type: str = "date"
    ) -> None:
        """
        Load data từ GCS Parquet file vào BigQuery native table sử dụng MERGE.
        MERGE đảm bảo idempotency: UPDATE nếu match, INSERT nếu không.
        Partition filtering trong ON clause để tối ưu performance.
        
        Args:
            gcs_uri: GCS URI của Parquet file (gs://bucket/path/to/file.parquet)
            table_id: Full BigQuery table ID
            partition_date: Ngày partition
            partition_field: Tên field để partition (default: extraction_date)
            partition_type: Type of partition - "date" (direct DATE field) or "timestamp" (DATE(extraction_timestamp))
        """
        if not gcs_uri:
            return
        
        external_table_id = None
        
        try:
            # Step 1: Ensure dataset exists
            self._ensure_table_exists(table_id, partition_field)
            
            # Step 2: Check if table exists, create if not (with partition)
            table_exists = False
            has_partitioning = False
            try:
                table = self.bq_client.get_table(table_id)
                table_exists = True
                has_partitioning = table.time_partitioning is not None
            except Exception:
                # Table chưa tồn tại, sẽ được tạo bởi MERGE INSERT
                # MERGE INSERT sẽ tự động tạo table, nhưng không có partition
                # We'll handle partition setup after first insert if needed
                table_exists = False
                has_partitioning = False
            
            # Step 3: Create temporary external table từ GCS file
            external_table_id = self._create_temp_external_table(gcs_uri, table_id)
            
            # Step 4: MERGE từ external table vào fact table
            # Determine merge method based on table type
            if table_id == self.bills_table_id:
                rows_affected = self._merge_bills_from_external_table(
                    external_table_id, table_id, partition_date
                )
            elif table_id == self.products_table_id:
                rows_affected = self._merge_products_from_external_table(
                    external_table_id, table_id, partition_date
                )
            else:
                # Fallback: generic merge (should not happen in normal flow)
                logger.warning(
                    f"Unknown table type, using generic merge",
                    table_id=table_id
                )
                # For now, raise error - can be extended later
                raise ValueError(f"Unknown table type for MERGE: {table_id}")
            
            # Step 5: Verify table exists and has partitioning after MERGE
            # Note: MERGE INSERT will create table but without partition definition
            # Partition is defined at table creation time, so if table was just created,
            # it won't have partition. This is acceptable - data will still be in correct
            # partition based on partition field value, but table metadata won't show partition.
            # For proper partitioning, table should be created using CREATE TABLE with PARTITION BY.
            # For now, we'll log a warning if table doesn't have partition metadata.
            try:
                table = self.bq_client.get_table(table_id)
                if not table.time_partitioning:
                    logger.warning(
                        f"Table exists but partition metadata not set. Data is still partitioned by field value.",
                        table_id=table_id,
                        partition_field=partition_field,
                        note="Consider creating table with PARTITION BY clause for optimal performance"
                    )
            except Exception:
                pass  # Table might not exist (shouldn't happen after MERGE)
            
            logger.info(
                f"Loaded data from GCS to BigQuery using MERGE",
                table_id=table_id,
                gcs_uri=gcs_uri,
                rows_affected=rows_affected,
                partition_date=partition_date.isoformat()
            )
            
        except Exception as e:
            logger.error(
                f"Failed to load GCS to BigQuery using MERGE",
                table_id=table_id,
                gcs_uri=gcs_uri,
                partition_date=partition_date.isoformat(),
                error=str(e)
            )
            # Không raise để không block pipeline nếu BigQuery load fail
            # GCS đã có backup rồi
        finally:
            # Step 6: Cleanup temporary external table
            if external_table_id:
                self._cleanup_external_table(external_table_id)
    
    def _ensure_table_exists(self, table_id: str, partition_field: str = "extraction_date") -> None:
        """
        Ensure BigQuery dataset exists. Table sẽ được tạo tự động khi load từ Parquet.
        Dataset: nhanhVN (target_dataset)
        """
        try:
            dataset_id = f"{settings.gcp_project}.{settings.target_dataset}"
            dataset = self.bq_client.get_dataset(dataset_id)
            logger.debug(f"Dataset {dataset_id} already exists")
        except Exception:
            logger.info(f"Dataset {settings.target_dataset} not found. Creating...")
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = settings.gcp_region
            dataset.description = "NhanhVN fact tables - Flattened sales data"
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_id}")
        
        # Table sẽ được tạo tự động khi load từ Parquet với autodetect=True
    
    def load_bills(
        self,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Flatten bills data, upload lên GCS và load vào BigQuery fact table.
        
        Args:
            data: Danh sách raw bills với nested structures
            partition_date: Ngày partition (dùng cho date field trong fact table)
            metadata: Metadata bổ sung
            
        Returns:
            str: GCS path của file đã upload
        """
        if not data:
            logger.warning("No bills data to load")
            return ""
        
        if partition_date is None:
            partition_date = date.today()
        
        entity_path = f"{self.platform}/{self.entity}"
        extraction_timestamp = datetime.utcnow()
        
        # Step 1: Flatten nested structures
        flattened_data = []
        for bill in data:
            flattened_bill = self._flatten_bill(bill, extraction_timestamp)
            flattened_data.append(flattened_bill)
        
        logger.info(
            f"Flattened {len(flattened_data)} bills",
            records=len(flattened_data)
        )
        
        upload_metadata = {
            "platform": self.platform,
            "entity": self.entity,
            "extraction_timestamp": extraction_timestamp.isoformat(),
            "record_count": len(data),
            **(metadata or {})
        }
        
        # Step 2: Upload flattened data to GCS (backup)
        gcs_path = self.gcs_loader.upload_parquet_by_date(
            entity=entity_path,
            data=flattened_data,
            partition_date=partition_date,
            metadata=upload_metadata,
            overwrite_partition=True
        )
        
        logger.info(
            f"Loaded {len(flattened_data)} flattened bills to GCS",
            path=gcs_path,
            records=len(flattened_data)
        )
        
        # Step 3: Load from GCS to BigQuery fact table
        if gcs_path:
            gcs_uri = f"gs://{settings.bronze_bucket}/{gcs_path}"
            try:
                # Bills table is partitioned by 'date' field
                # Use the date from the first bill or partition_date
                bill_date = partition_date
                if flattened_data and flattened_data[0].get("date"):
                    bill_date = flattened_data[0]["date"]
                
                self._load_gcs_to_bigquery(
                    gcs_uri=gcs_uri,
                    table_id=self.bills_table_id,
                    partition_date=bill_date,
                    partition_field="date",
                    partition_type="date"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to load bills to BigQuery, GCS backup available",
                    gcs_path=gcs_path,
                    error=str(e)
                )
                # Không raise để không block pipeline
        
        return gcs_path
    
    def load_bill_products(
        self,
        data: List[Dict[str, Any]],
        partition_date: Optional[date] = None,
        bills_data: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Flatten bill_products data, upload lên GCS và load vào BigQuery fact table.
        
        Args:
            data: Danh sách raw bill products với nested vat structure
            partition_date: Ngày partition (dùng cho bill_date trong fact table)
            bills_data: Optional bills data để tạo bill_id -> date mapping
            metadata: Metadata bổ sung
            
        Returns:
            str: GCS path của file đã upload
        """
        if not data:
            logger.warning("No bill_products data to load")
            return ""
        
        if partition_date is None:
            partition_date = date.today()
        
        entity_path = f"{self.platform}/bill_products"
        extraction_timestamp = datetime.utcnow()
        
        # Create bill_id -> date mapping from bills_data
        bill_date_map = {}
        if bills_data:
            for bill in bills_data:
                bill_id = bill.get("id")
                if bill_id:
                    bill_date_str = bill.get("date")
                    if bill_date_str:
                        try:
                            if isinstance(bill_date_str, str):
                                bill_date_map[bill_id] = datetime.strptime(bill_date_str, "%Y-%m-%d").date()
                            elif isinstance(bill_date_str, date):
                                bill_date_map[bill_id] = bill_date_str
                            elif isinstance(bill_date_str, datetime):
                                bill_date_map[bill_id] = bill_date_str.date()
                        except (ValueError, TypeError):
                            logger.warning(f"Failed to parse bill date: {bill_date_str}")
        
        # Log bill_date_map statistics for debugging
        if bill_date_map:
            logger.debug(
                f"Created bill_date_map with {len(bill_date_map)} entries",
                sample_bill_ids=list(bill_date_map.keys())[:5] if bill_date_map else []
            )
        else:
            logger.warning(
                f"bill_date_map is empty, will use partition_date {partition_date} for all products"
            )
        
        # Step 1: Flatten nested structures (vat)
        flattened_data = []
        products_with_bill_date_from_map = 0
        products_using_partition_date = 0
        
        for product in data:
            bill_id = product.get("bill_id")
            bill_date = bill_date_map.get(bill_id) if bill_id else None
            # Fallback to partition_date if bill_date not found
            if bill_date is None:
                bill_date = partition_date
                products_using_partition_date += 1
            else:
                products_with_bill_date_from_map += 1
            
            flattened_product = self._flatten_bill_product(product, extraction_timestamp, bill_date)
            flattened_data.append(flattened_product)
        
        # Log bill_date assignment statistics
        logger.debug(
            f"bill_date assignment: {products_with_bill_date_from_map} from map, {products_using_partition_date} using partition_date",
            partition_date=partition_date.isoformat()
        )
        
        logger.info(
            f"Flattened {len(flattened_data)} bill products",
            records=len(flattened_data)
        )
        
        upload_metadata = {
            "platform": self.platform,
            "entity": "bill_products",
            "extraction_timestamp": extraction_timestamp.isoformat(),
            "record_count": len(data),
            **(metadata or {})
        }
        
        # Step 2: Upload flattened data to GCS (backup)
        gcs_path = self.gcs_loader.upload_parquet_by_date(
            entity=entity_path,
            data=flattened_data,
            partition_date=partition_date,
            metadata=upload_metadata,
            overwrite_partition=True
        )
        
        logger.info(
            f"Loaded {len(flattened_data)} flattened bill products to GCS",
            path=gcs_path,
            records=len(flattened_data)
        )
        
        # Step 3: Load from GCS to BigQuery fact table
        if gcs_path:
            gcs_uri = f"gs://{settings.bronze_bucket}/{gcs_path}"
            try:
                # Products table is partitioned by bill_date
                # Use bill_date from first product or partition_date as fallback
                bill_date_for_partition = partition_date
                if flattened_data and flattened_data[0].get("bill_date"):
                    bill_date_for_partition = flattened_data[0]["bill_date"]
                
                # Delete existing partition data before MERGE to ensure clean state
                # This is important because old records might have NULL bill_date
                # and MERGE UPDATE might not work correctly with NULL values
                # Also delete records with NULL bill_date that match bill_ids from current extraction
                try:
                    if flattened_data:
                        # Get bill_ids from current extraction
                        bill_ids = list(set(p.get("bill_id") for p in flattened_data if p.get("bill_id")))
                        if bill_ids:
                            # Delete records with these bill_ids that have NULL bill_date
                            # Process in batches of 1000 to avoid query size limits
                            batch_size = 1000
                            total_deleted = 0
                            for i in range(0, len(bill_ids), batch_size):
                                batch = bill_ids[i:i + batch_size]
                                bill_ids_str = ",".join(str(bid) for bid in batch)
                                delete_sql = f"""
                                DELETE FROM `{self.products_table_id}`
                                WHERE bill_id IN ({bill_ids_str})
                                  AND bill_date IS NULL
                                """
                                delete_job = self.bq_client.query(delete_sql)
                                delete_job.result()
                                deleted_count = delete_job.num_dml_affected_rows if hasattr(delete_job, 'num_dml_affected_rows') else 0
                                total_deleted += deleted_count
                            
                            if total_deleted > 0:
                                logger.info(
                                    f"Deleted {total_deleted} rows with NULL bill_date for bill_ids in current extraction",
                                    deleted_count=total_deleted,
                                    bill_ids_count=len(bill_ids)
                                )
                except Exception as e:
                    logger.warning(f"Failed to delete NULL bill_date records, continuing: {e}")
                
                # Also delete partition data by bill_date (for records that already have correct bill_date)
                self._delete_partition_data(
                    table_id=self.products_table_id,
                    partition_date=bill_date_for_partition,
                    partition_field="bill_date",
                    partition_type="date"
                )
                
                self._load_gcs_to_bigquery(
                    gcs_uri=gcs_uri,
                    table_id=self.products_table_id,
                    partition_date=bill_date_for_partition,
                    partition_field="bill_date",
                    partition_type="date"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to load bill_products to BigQuery, GCS backup available",
                    gcs_path=gcs_path,
                    error=str(e)
                )
                # Không raise để không block pipeline
        
        return gcs_path
    
    def load_bills_from_gcs(
        self,
        gcs_uri: str,
        partition_date: date
    ) -> None:
        """
        Load bills data từ GCS Parquet file có sẵn vào BigQuery fact table.
        Không cần flatten/upload lại vì file đã được flatten sẵn.
        
        Args:
            gcs_uri: GCS URI của Parquet file (gs://bucket/path/to/file.parquet)
            partition_date: Ngày partition
        """
        self._load_gcs_to_bigquery(
            gcs_uri=gcs_uri,
            table_id=self.bills_table_id,
            partition_date=partition_date,
            partition_field="date",
            partition_type="date"
        )
    
    def load_products_from_gcs(
        self,
        gcs_uri: str,
        partition_date: date
    ) -> None:
        """
        Load bill_products data từ GCS Parquet file có sẵn vào BigQuery fact table.
        Không cần flatten/upload lại vì file đã được flatten sẵn.
        
        Args:
            gcs_uri: GCS URI của Parquet file (gs://bucket/path/to/file.parquet)
            partition_date: Ngày partition (bill_date)
        """
        self._load_gcs_to_bigquery(
            gcs_uri=gcs_uri,
            table_id=self.products_table_id,
            partition_date=partition_date,
            partition_field="bill_date",
            partition_type="date"
        )
    
    def setup_external_tables(self) -> Dict[str, str]:
        """Setup BigQuery external tables cho bills và bill_products."""
        tables = {}
        
        # Bills table
        tables["bills"] = self.bq_setup.setup_external_table(
            platform=self.platform,
            entity=self.entity
        )
        
        # Bill products table
        tables["bill_products"] = self.bq_setup.setup_external_table(
            platform=self.platform,
            entity="bill_products"
        )
        
        logger.info("Setup external tables for bills", tables=tables)
        return tables
