# Best Practices cho Lakehouse trên Google Cloud

## 1. Storage Best Practices

### 1.1. Bucket Organization

**✅ DO**:
```
gs://project-data-lake/
├── bronze/
│   ├── source1/
│   │   ├── year=2024/month=12/day=06/
│   │   └── _schemas/
│   └── source2/
├── silver/
│   └── domain1/
└── gold/
    └── analytics/
```

**❌ DON'T**:
- Flat structure không có partition
- Mixed data types trong cùng folder
- Không có versioning cho production

### 1.2. File Format Recommendations

- **Use Parquet**: Columnar format, compression tốt
- **Optimal file size**: 256MB - 1GB per file
- **Avoid small files**: Consolidate nếu có nhiều files nhỏ

### 1.3. Partitioning Strategy

**Time-based Partitioning**:
```sql
PARTITION BY DATE(order_date)
CLUSTER BY customer_id
```

**Best Practices**:
- Partition by most common filter column
- Avoid over-partitioning (>1000 partitions)
- Use clustering for high-cardinality columns

### 1.4. Lifecycle Management

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 90}
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
        "condition": {"age": 365}
      }
    ]
  }
}
```

## 2. BigQuery Best Practices

### 2.1. Table Design

**Native Tables**:
- Use cho frequently queried data
- Optimize với partitioning và clustering
- Materialized views cho common aggregations

**External Tables**:
- Use cho infrequently queried data
- Historical data
- Multi-format support

**BigLake Tables**:
- Open formats (Iceberg, Delta)
- Multi-cloud scenarios
- Fine-grained access control

### 2.2. Query Optimization

**✅ DO**:
```sql
-- Use partitioning trong WHERE
SELECT * FROM orders
WHERE order_date = '2024-12-06';

-- Use clustering columns
SELECT * FROM orders
WHERE customer_id = 12345 AND order_date = '2024-12-06';

-- Limit columns selected
SELECT order_id, amount FROM orders;

-- Use APPROX functions khi có thể
SELECT APPROX_COUNT_DISTINCT(customer_id) FROM orders;
```

**❌ DON'T**:
```sql
-- Don't scan full table
SELECT * FROM orders;

-- Don't use functions on partition columns
SELECT * FROM orders WHERE DATE(order_timestamp) = '2024-12-06';

-- Avoid SELECT * trong production
SELECT * FROM large_table;
```

### 2.3. Cost Optimization

- **Use query result caching**: Re-run queries trong 24h
- **Use materialized views**: Pre-aggregate common queries
- **Right-size slots**: Monitor slot usage
- **Use approximate aggregations**: APPROX_COUNT_DISTINCT
- **Avoid duplicate queries**: Share cached results

### 2.4. Data Loading

**Batch Loading**:
- Use load jobs thay vì INSERT
- Load trong Parquet format
- Use write_disposition='WRITE_APPEND' cho incremental

**Streaming**:
- Batch inserts trong streaming
- Use streaming buffer
- Monitor streaming quota

## 3. Data Quality Best Practices

### 3.1. Schema Validation

```python
def validate_schema(record, schema):
    """Validate record against schema"""
    try:
        # Type checking
        # Required fields
        # Format validation
        return True
    except ValidationError as e:
        log_error(e)
        return False
```

### 3.2. Data Quality Rules

**Completeness**:
- Check for required fields
- Monitor null rates
- Set thresholds (e.g., <5% nulls)

**Accuracy**:
- Range checks
- Format validation
- Referential integrity

**Consistency**:
- Cross-table validation
- Business rule validation
- Temporal consistency

### 3.3. Quality Monitoring

- Automate quality checks
- Set up alerts for failures
- Track quality metrics over time
- Document quality issues

## 4. Security Best Practices

### 4.1. IAM Principles

- **Least Privilege**: Grant minimum required permissions
- **Service Accounts**: Use cho automated processes
- **Regular Reviews**: Audit permissions định kỳ

### 4.2. Data Classification

```sql
-- Classify sensitive data
ALTER TABLE customers
ALTER COLUMN ssn
SET OPTIONS (
  policy_tags = ['projects/.../policyTags/pii']
);
```

### 4.3. Encryption

- **At Rest**: Enable default encryption
- **In Transit**: Always use HTTPS/TLS
- **Customer-Managed Keys**: Cho sensitive data

### 4.4. Access Auditing

- Enable Cloud Audit Logs
- Monitor data access patterns
- Regular access reviews
- Alert on unusual access

## 5. Performance Best Practices

### 5.1. Pipeline Optimization

**Parallel Processing**:
- Use multiple workers
- Optimize shuffle operations
- Use appropriate windowing

**Incremental Processing**:
- Process only changed data
- Use watermark strategies
- Avoid full table scans

### 5.2. Caching Strategy

- Cache frequently accessed data
- Use materialized views
- Implement result caching
- Cache metadata queries

### 5.3. Monitoring Performance

- Track query performance
- Monitor slot usage
- Identify slow queries
- Optimize based on metrics

## 6. Cost Management Best Practices

### 6.1. Storage Costs

- Use appropriate storage classes
- Implement lifecycle policies
- Delete unused data
- Compress data effectively

### 6.2. Compute Costs

- Right-size resources
- Use preemptible VMs
- Optimize query performance
- Use reserved capacity nếu cần

### 6.3. Cost Monitoring

```bash
# Set budget alerts
gcloud billing budgets create \
  --billing-account=ACCOUNT_ID \
  --display-name="Lakehouse Budget" \
  --budget-amount=10000USD \
  --threshold-rule=percent=50 \
  --threshold-rule=percent=90
```

## 7. Data Governance Best Practices

### 7.1. Metadata Management

- Maintain data catalog
- Document data lineage
- Keep schemas updated
- Document business rules

### 7.2. Data Lineage

- Track data flow
- Document transformations
- Map dependencies
- Impact analysis tools

### 7.3. Compliance

- Understand regulations (GDPR, CCPA)
- Implement data retention policies
- Enable audit logging
- Regular compliance reviews

## 8. Development Best Practices

### 8.1. Code Organization

```
project/
├── pipelines/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/
│   ├── transformations/
│   └── queries/
├── tests/
└── configs/
```

### 8.2. Version Control

- Version all code
- Tag releases
- Document changes
- Review process

### 8.3. Testing

- Unit tests cho transformations
- Integration tests cho pipelines
- Data quality tests
- Performance tests

### 8.4. Documentation

- README files
- Data dictionaries
- Architecture diagrams
- Runbooks

## 9. Monitoring và Alerting

### 9.1. Key Metrics

**Pipeline Metrics**:
- Success/failure rates
- Processing time
- Data volume processed
- Error rates

**Quality Metrics**:
- Quality scores
- Validation failures
- Data freshness
- Completeness rates

**Cost Metrics**:
- Storage costs
- Compute costs
- Query costs
- Trends over time

### 9.2. Alerting Strategy

- Set meaningful thresholds
- Avoid alert fatigue
- Escalation procedures
- Runbook references

## 10. Disaster Recovery

### 10.1. Backup Strategy

- Regular backups
- Point-in-time recovery
- Cross-region replication
- Test restore procedures

### 10.2. High Availability

- Multi-region deployment
- Redundant pipelines
- Failover procedures
- Health checks

## Checklist: Before Production

### Infrastructure
- [ ] All APIs enabled
- [ ] IAM properly configured
- [ ] Networks configured
- [ ] Monitoring setup

### Data
- [ ] Schemas defined
- [ ] Quality rules implemented
- [ ] Partitioning strategy
- [ ] Backup procedures

### Pipelines
- [ ] Error handling
- [ ] Retry logic
- [ ] Monitoring
- [ ] Alerting

### Security
- [ ] Access controls
- [ ] Encryption enabled
- [ ] Audit logging
- [ ] Compliance checks

### Documentation
- [ ] Architecture documented
- [ ] Runbooks created
- [ ] Data dictionary
- [ ] Troubleshooting guides

## Tài liệu tham khảo

- [Google Cloud Architecture Framework](https://cloud.google.com/architecture/framework)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Data Engineering Best Practices](https://cloud.google.com/solutions/data-engineering-best-practices)

