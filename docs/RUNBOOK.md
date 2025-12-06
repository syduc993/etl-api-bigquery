# ETL Pipeline Runbook

Runbook cho operations và troubleshooting của ETL pipeline.

## Daily Operations

### Check Pipeline Status

```bash
# Check Cloud Run Jobs execution history
gcloud run jobs executions list \
  --job=nhanh-etl-job \
  --region=us-central1 \
  --limit=10

# Check logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=nhanh-etl-job" \
  --limit=50 \
  --format=json
```

### Verify Data Quality

```bash
# Check watermark table
bq query --use_legacy_sql=false "
  SELECT * FROM \`sync-nhanhvn-project.bronze.extraction_watermarks\`
  ORDER BY last_successful_run DESC
  LIMIT 10
"

# Check Silver layer data
bq query --use_legacy_sql=false "
  SELECT COUNT(*) as bills_count
  FROM \`sync-nhanhvn-project.silver.bills\`
  WHERE bill_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
"
```

## Common Issues & Solutions

### Issue: Job Failed

**Symptoms:**
- Job execution status = FAILED
- Error logs trong Cloud Logging

**Troubleshooting:**
1. Check logs để xem error message
2. Verify secrets trong Secret Manager
3. Check API credentials
4. Verify GCS và BigQuery permissions

**Solution:**
```bash
# Re-run job manually
gcloud run jobs execute nhanh-etl-job \
  --region=us-central1 \
  --wait
```

### Issue: Rate Limit Exceeded

**Symptoms:**
- ERR_429 errors trong logs
- Jobs bị delay

**Troubleshooting:**
1. Check rate limit logs
2. Verify token bucket algorithm hoạt động
3. Check job frequency

**Solution:**
- Pipeline tự động handle rate limits
- Nếu vẫn bị, giảm job frequency trong scheduler

### Issue: Data Missing

**Symptoms:**
- Watermark không update
- Không có data mới trong Silver/Gold

**Troubleshooting:**
1. Check watermark table
2. Verify extraction logs
3. Check GCS files
4. Verify transformation logs

**Solution:**
```bash
# Check last extraction
bq query --use_legacy_sql=false "
  SELECT * FROM \`sync-nhanhvn-project.bronze.extraction_watermarks\`
  WHERE entity = 'bills'
"

# Manual re-extraction
python src/main.py --entity bills --incremental
```

### Issue: Transformation Failed

**Symptoms:**
- Silver/Gold tables không update
- SQL errors trong logs

**Troubleshooting:**
1. Check BigQuery job logs
2. Verify SQL syntax
3. Check data quality issues
4. Verify table schemas

**Solution:**
```bash
# Re-run transformation
python src/transform_silver.py --entity all

# Check BigQuery errors
bq ls -j --max_results=10
```

## Manual Operations

### Full Sync

```bash
# Run full sync (không incremental)
python src/orchestrator.py --phase all --full-sync
```

### Re-run Specific Phase

```bash
# Bronze only
python src/orchestrator.py --phase bronze

# Silver only
python src/orchestrator.py --phase silver

# Gold only
python src/orchestrator.py --phase gold
```

### Re-run Specific Entity

```bash
# Bills only
python src/main.py --entity bills --incremental

# Products only
python src/main.py --entity products --incremental

# Customers only
python src/main.py --entity customers --incremental
```

## Data Validation

### Check Data Quality

```bash
# Check completeness
bq query --use_legacy_sql=false "
  SELECT 
    COUNT(*) as total,
    COUNT(bill_id) as has_id,
    COUNT(customer_id) as has_customer,
    COUNT(total_amount) as has_amount
  FROM \`sync-nhanhvn-project.silver.bills\`
  WHERE bill_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
"

# Check for duplicates
bq query --use_legacy_sql=false "
  SELECT bill_id, COUNT(*) as count
  FROM \`sync-nhanhvn-project.silver.bills\`
  GROUP BY bill_id
  HAVING count > 1
  LIMIT 10
"
```

## Monitoring

### Check Metrics

```bash
# View monitoring dashboard
# https://console.cloud.google.com/monitoring/dashboards?project=sync-nhanhvn-project

# Check recent logs
gcloud logging read "resource.type=cloud_run_job" \
  --limit=100 \
  --format=json
```

### Check Costs

```bash
# View cost breakdown
# https://console.cloud.google.com/billing?project=sync-nhanhvn-project

# Check BigQuery costs
bq query --use_legacy_sql=false "
  SELECT 
    job_id,
    creation_time,
    total_bytes_processed,
    total_bytes_billed
  FROM \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  ORDER BY creation_time DESC
  LIMIT 10
"
```

## Emergency Procedures

### Stop All Jobs

```bash
# Pause Cloud Scheduler
gcloud scheduler jobs pause nhanh-etl-bronze-schedule --location=us-central1
gcloud scheduler jobs pause nhanh-etl-silver-schedule --location=us-central1
gcloud scheduler jobs pause nhanh-etl-gold-schedule --location=us-central1
```

### Resume Jobs

```bash
# Resume Cloud Scheduler
gcloud scheduler jobs resume nhanh-etl-bronze-schedule --location=us-central1
gcloud scheduler jobs resume nhanh-etl-silver-schedule --location=us-central1
gcloud scheduler jobs resume nhanh-etl-gold-schedule --location=us-central1
```

### Rollback

Nếu cần rollback data:
1. Check GCS versioning (nếu enabled)
2. Restore từ backup nếu có
3. Re-run transformations từ specific date

## Maintenance

### Regular Tasks

**Daily:**
- Check job execution status
- Review error logs
- Verify data quality

**Weekly:**
- Review costs
- Check performance metrics
- Review alerts

**Monthly:**
- Security audit review
- Cost optimization review
- Update dependencies
- Review và update documentation

## Support Contacts

- **Technical Issues**: [Your contact]
- **Data Issues**: [Your contact]
- **Infrastructure**: [Your contact]

## Escalation

1. **Level 1**: Check logs và runbook
2. **Level 2**: Contact technical team
3. **Level 3**: Escalate to management

