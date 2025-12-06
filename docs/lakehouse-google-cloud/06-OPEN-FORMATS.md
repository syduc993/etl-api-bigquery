# Open Formats trong Google Cloud Lakehouse

## Tổng quan

Google Cloud hỗ trợ các open table formats cho phép xây dựng lakehouse với tính linh hoạt cao và tránh vendor lock-in.

## Các Open Formats được hỗ trợ

### 1. Apache Iceberg

#### Giới thiệu

Apache Iceberg là một open table format cho large analytic tables, cung cấp:
- ACID transactions
- Time travel queries
- Schema evolution
- Hidden partitioning
- Metadata optimization

#### Tính năng chính

**ACID Transactions**:
- Atomic writes
- Consistent reads
- Isolation between concurrent operations
- Durability guarantees

**Time Travel**:
```sql
-- Query data at specific point in time
SELECT * FROM orders
FOR SYSTEM_TIME AS OF '2024-12-06 10:00:00';
```

**Schema Evolution**:
- Add/remove columns
- Rename columns
- Change column types
- Safe schema changes

**Hidden Partitioning**:
- Partitioning logic trong metadata
- Users không cần biết partition scheme
- Automatic partition pruning

#### BigQuery Integration

```sql
-- Create Iceberg table
CREATE TABLE `my-project.silver.orders_iceberg`
WITH (
  table_format = 'ICEBERG',
  catalog = 'iceberg_catalog',
  base_location = 'gs://my-lakehouse-silver/orders_iceberg'
);

-- Query Iceberg table
SELECT * FROM `my-project.silver.orders_iceberg`
WHERE order_date = '2024-12-06';
```

#### Use Cases

- Large analytical workloads
- Need for time travel
- Schema evolution requirements
- Multi-engine access (Spark, Flink, Trino)

### 2. Delta Lake

#### Giới thiệu

Delta Lake là một storage layer cho data lakes, cung cấp:
- ACID transactions
- Time travel
- Upserts và deletes
- Schema enforcement
- Audit history

#### Tính năng chính

**ACID Transactions**:
- Serializable isolation
- Optimistic concurrency control
- Transaction log

**Time Travel**:
```sql
-- Query previous version
SELECT * FROM delta.`gs://path/to/delta-table` VERSION AS OF 10;

-- Query at timestamp
SELECT * FROM delta.`gs://path/to/delta-table` TIMESTAMP AS OF '2024-12-06';
```

**Upserts**:
```sql
-- Merge operation
MERGE INTO delta_table target
USING source_table source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**Schema Evolution**:
- Automatic schema evolution
- Schema enforcement
- Merge schema option

#### BigQuery Integration

```sql
-- Create BigLake table pointing to Delta Lake
CREATE OR REPLACE EXTERNAL TABLE `my-project.silver.orders_delta`
WITH CONNECTION `my-project.us.biglake-connection`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://my-lakehouse-silver/orders_delta']
);
```

#### Use Cases

- Frequent updates/deletes
- Need for merge operations
- Streaming data ingestion
- Databricks integration

### 3. Apache Hudi

#### Giới thiệu

Apache Hudi (Hadoop Upserts Deletes and Incrementals) cung cấp:
- Upsert và delete operations
- Incremental processing
- Time travel queries
- Indexing for fast lookups

#### Tính năng chính

**Upserts và Deletes**:
- Efficient upsert operations
- Delete support
- Insert overwrite

**Incremental Processing**:
```java
// Read incremental changes
Dataset<Row> incrementalDF = spark
  .read()
  .format("hudi")
  .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), 
          DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
  .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), 
          "2024-12-06")
  .load(tablePath);
```

**Table Types**:
- **Copy-on-Write (COW)**: Update entire files
- **Merge-on-Read (MOR)**: Separate storage for updates

**Indexing**:
- Bloom filters
- Index metadata
- Fast lookups

#### Use Cases

- Frequent upserts
- Incremental ETL
- CDC scenarios
- Real-time data processing

## So sánh các Formats

| Feature | Iceberg | Delta Lake | Hudi |
|---------|---------|------------|------|
| ACID Transactions | ✅ | ✅ | ✅ |
| Time Travel | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ |
| Upserts/Deletes | ✅ | ✅ | ✅ |
| Multi-Engine | ✅ | ⚠️ | ⚠️ |
| Metadata Store | File-based | Transaction Log | File-based |
| Best For | Large Analytics | Frequent Updates | Incremental ETL |

## Choosing the Right Format

### Use Iceberg if:
- Large analytical workloads
- Need multi-engine support
- Want hidden partitioning
- Focus on read performance

### Use Delta Lake if:
- Frequent updates/deletes
- Using Databricks
- Need merge operations
- Streaming workloads

### Use Hudi if:
- Incremental processing
- CDC scenarios
- Need indexing
- Frequent upserts

## Implementation với BigLake

### Setup BigLake Connection

```bash
# Create BigLake connection
gcloud bigquery connections create biglake-connection \
  --location=us \
  --service-account-path=/path/to/service-account.json
```

### Create BigLake Tables

```sql
-- Iceberg table
CREATE TABLE `my-project.silver.orders_iceberg`
WITH (
  table_format = 'ICEBERG',
  catalog = 'iceberg_catalog',
  base_location = 'gs://my-lakehouse-silver/orders_iceberg'
);

-- Delta Lake table
CREATE OR REPLACE EXTERNAL TABLE `my-project.silver.orders_delta`
WITH CONNECTION `my-project.us.biglake-connection`
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://my-lakehouse-silver/orders_delta']
);

-- Hudi table
CREATE OR REPLACE EXTERNAL TABLE `my-project.silver.orders_hudi`
WITH CONNECTION `my-project.us.biglake-connection`
OPTIONS (
  format = 'HUDI',
  uris = ['gs://my-lakehouse-silver/orders_hudi']
);
```

## Best Practices

### 1. File Size Optimization

- Target file size: 256MB - 1GB
- Compact small files định kỳ
- Monitor file count

### 2. Partitioning

- Partition by high-cardinality columns
- Avoid over-partitioning
- Use time-based partitions khi có thể

### 3. Metadata Management

- Regular metadata cleanup
- Monitor metadata size
- Optimize metadata queries

### 4. Schema Management

- Version schemas
- Document schema changes
- Test schema evolution
- Plan for backward compatibility

## Migration Strategy

### From Legacy Formats

1. **Assessment**:
   - Analyze current format
   - Identify migration requirements
   - Plan downtime windows

2. **Migration**:
   - Parallel write strategy
   - Data validation
   - Gradual cutover

3. **Validation**:
   - Compare results
   - Performance testing
   - User acceptance

### Between Open Formats

- Similar migration approach
- Use format conversion tools
- Validate data integrity
- Update application code

## Performance Considerations

### Read Performance

- Partition pruning
- Column projection
- Predicate pushdown
- Metadata caching

### Write Performance

- Batch writes
- Parallel writes
- Optimize commit frequency
- Monitor compaction

## Monitoring

### Key Metrics

- Table size và growth
- File count
- Metadata size
- Query performance
- Write throughput

### Tools

- BigQuery monitoring
- Cloud Monitoring
- Custom dashboards
- Alerts for anomalies

## Tài liệu tham khảo

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Delta Lake Documentation](https://delta.io/)
- [Apache Hudi Documentation](https://hudi.apache.org/)
- [BigLake Documentation](https://cloud.google.com/bigquery/docs/biglake-intro)
- [Open Formats trên Google Cloud](https://cloud.google.com/blog/products/data-analytics/open-source-table-formats-for-data-lakes)

