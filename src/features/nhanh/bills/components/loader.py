"""
Loader cho Bills feature.
Upload data lên GCS và load vào BigQuery native tables (fact tables).
Flatten nested structures trong Python trước khi load.
"""
from datetime import datetime, date
from typing import Dict, Any, List, Optional
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
    - Tránh duplicate bằng DELETE partition + WRITE_APPEND
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
    
    def _flatten_bill_product(self, product: Dict[str, Any], extraction_timestamp: datetime) -> Dict[str, Any]:
        """
        Flatten nested product structure thành flat record matching fact_sales_bills_product_v3_0 schema.
        
        Args:
            product: Raw product dict với nested vat structure
            extraction_timestamp: Timestamp khi extract data
            
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
            try:
                table = self.bq_client.get_table(table_id)
                has_partitioning = table.time_partitioning is not None
            except Exception:
                # Table chưa tồn tại, không cần delete
                logger.debug(f"Table {table_id} does not exist, skipping delete")
                return
            
            # Delete partition data
            # Sử dụng _PARTITIONDATE chỉ nếu table có time partitioning
            if partition_type == "timestamp":
                # For products table: partition by DATE(extraction_timestamp)
                sql = f"""
                DELETE FROM `{table_id}`
                WHERE DATE(extraction_timestamp) = DATE('{partition_date.isoformat()}')
                """
            elif has_partitioning:
                # For bills table: partition by date field directly, use _PARTITIONDATE if table has partitioning
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
    
    def _load_gcs_to_bigquery(
        self,
        gcs_uri: str,
        table_id: str,
        partition_date: date,
        partition_field: str = "extraction_date",
        partition_type: str = "date"
    ) -> None:
        """
        Load data từ GCS Parquet file vào BigQuery native table.
        Sử dụng DELETE partition + WRITE_APPEND để tránh duplicate.
        
        Args:
            gcs_uri: GCS URI của Parquet file (gs://bucket/path/to/file.parquet)
            table_id: Full BigQuery table ID
            partition_date: Ngày partition
            partition_field: Tên field để partition (default: extraction_date)
            partition_type: Type of partition - "date" (direct DATE field) or "timestamp" (DATE(extraction_timestamp))
        """
        if not gcs_uri:
            return
        
        try:
            # Step 1: Ensure dataset exists
            self._ensure_table_exists(table_id, partition_field)
            
            # Step 2: Check if table exists and has partitioning
            table_exists = False
            has_partitioning = False
            try:
                table = self.bq_client.get_table(table_id)
                table_exists = True
                has_partitioning = table.time_partitioning is not None
            except Exception:
                # Table chưa tồn tại, sẽ được tạo với partition
                table_exists = False
                has_partitioning = False
            
            # Step 3: Delete partition cũ để tránh duplicate (chỉ nếu table có partition)
            if table_exists and has_partitioning:
                self._delete_partition_data(table_id, partition_date, partition_field, partition_type)
            
            # Step 4: Load từ GCS vào BigQuery
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # Auto-detect schema từ Parquet file
                autodetect=True,
                # Allow schema updates để handle type changes (INTEGER -> FLOAT64)
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
                ]
            )
            
            # Nếu table đã tồn tại, sử dụng schema từ table để tránh mismatch
            if table_exists:
                table = self.bq_client.get_table(table_id)
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                # Không cần set schema vì BigQuery sẽ tự convert types nếu có schema_update_options
            
            # Chỉ set time partitioning nếu table chưa tồn tại hoặc chưa có partition
            if not table_exists or not has_partitioning:
                job_config.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
            
            load_job = self.bq_client.load_table_from_uri(
                gcs_uri,
                table_id,
                job_config=job_config
            )
            
            load_job.result()  # Wait for completion
            
            logger.info(
                f"Loaded data from GCS to BigQuery",
                table_id=table_id,
                gcs_uri=gcs_uri,
                rows=load_job.output_rows,
                partition_date=partition_date.isoformat()
            )
            
        except Exception as e:
            logger.error(
                f"Failed to load GCS to BigQuery",
                table_id=table_id,
                gcs_uri=gcs_uri,
                partition_date=partition_date.isoformat(),
                error=str(e)
            )
            # Không raise để không block pipeline nếu BigQuery load fail
            # GCS đã có backup rồi
    
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
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Flatten bill_products data, upload lên GCS và load vào BigQuery fact table.
        
        Args:
            data: Danh sách raw bill products với nested vat structure
            partition_date: Ngày partition (dùng cho DATE(extraction_timestamp) trong fact table)
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
        
        # Step 1: Flatten nested structures (vat)
        flattened_data = []
        for product in data:
            flattened_product = self._flatten_bill_product(product, extraction_timestamp)
            flattened_data.append(flattened_product)
        
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
                # Products table is partitioned by DATE(extraction_timestamp)
                self._load_gcs_to_bigquery(
                    gcs_uri=gcs_uri,
                    table_id=self.products_table_id,
                    partition_date=partition_date,
                    partition_field="extraction_timestamp",
                    partition_type="timestamp"
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
            partition_date: Ngày partition
        """
        self._load_gcs_to_bigquery(
            gcs_uri=gcs_uri,
            table_id=self.products_table_id,
            partition_date=partition_date,
            partition_field="extraction_timestamp",
            partition_type="timestamp"
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
