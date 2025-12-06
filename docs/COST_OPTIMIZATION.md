# Cost Optimization Review

Review và recommendations cho cost optimization của ETL pipeline.

## Current Cost Structure

### GCS Storage Costs
- **Bronze bucket**: Raw JSON files (compressed)
- **Silver bucket**: Parquet files (nếu dùng)
- **Lifecycle policies**: Auto-transition to Nearline/Coldline

### BigQuery Costs
- **Storage**: Data stored trong datasets
- **Query**: SQL transformations và aggregations
- **Slots**: Compute resources cho queries

### Cloud Run Costs
- **CPU/Memory**: Job execution resources
- **Execution time**: Thời gian chạy jobs
- **Requests**: Số lần chạy jobs

## Cost Optimization Strategies

### ✅ Storage Optimization

#### GCS
- [x] Gzip compression cho JSON files
- [x] Lifecycle policies: Bronze → Nearline (90 days) → Coldline (365 days)
- [ ] Delete old Bronze data sau khi load vào Silver (nếu không cần replay)
- [ ] Use Parquet format cho Silver (better compression)

#### BigQuery
- [x] Partitioning tables by date
- [x] Clustering by business keys
- [ ] Set table expiration dates
- [ ] Use query result caching
- [ ] Materialized views cho common queries

### ✅ Query Optimization

#### BigQuery Queries
- [x] Partition pruning trong WHERE clauses
- [x] Clustering để reduce scanned data
- [ ] Use APPROX functions khi có thể
- [ ] Limit SELECT columns (không SELECT *)
- [ ] Use LIMIT cho test queries

#### Incremental Processing
- [x] Watermark tracking để chỉ process data mới
- [x] Date range filtering trong queries
- [ ] MERGE statements thay vì full table refresh

### ✅ Compute Optimization

#### Cloud Run Jobs
- [x] Right-size memory/CPU (2Gi/2CPU hiện tại)
- [ ] Monitor và adjust dựa trên actual usage
- [ ] Use preemptible instances nếu có thể
- [ ] Optimize job execution time

#### BigQuery Slots
- [ ] Monitor slot usage
- [ ] Right-size slots based on workload
- [ ] Use on-demand pricing nếu workload không đều

## Cost Monitoring

### Metrics to Track
1. **GCS Storage**: Total size, growth rate
2. **BigQuery Storage**: Table sizes, growth rate
3. **BigQuery Query**: Bytes scanned, query count
4. **Cloud Run**: Execution time, memory usage
5. **API Calls**: Rate limit usage, retry count

### Budget Alerts
- [ ] Setup budget alerts cho project
- [ ] Monitor monthly costs
- [ ] Alert khi cost vượt threshold

## Recommendations

### Immediate Actions
1. **Setup lifecycle policies**: Đã có, cần verify
2. **Monitor costs**: Setup cost monitoring dashboard
3. **Review query patterns**: Optimize expensive queries

### Short-term (1-3 months)
1. **Delete old Bronze data**: Sau khi verify Silver data
2. **Optimize BigQuery queries**: Review và optimize
3. **Right-size resources**: Adjust Cloud Run resources

### Long-term (3-6 months)
1. **Evaluate Parquet format**: Cho Silver layer
2. **Consider data archiving**: Move old data to Coldline
3. **Review partitioning strategy**: Upgrade to day-level nếu cần

## Cost Estimates

### Monthly Estimates (Example)
- **GCS Storage**: ~$50-100 (depends on data volume)
- **BigQuery Storage**: ~$20-50 (depends on data volume)
- **BigQuery Query**: ~$100-200 (depends on query frequency)
- **Cloud Run**: ~$10-30 (depends on execution frequency)
- **Total**: ~$180-380/month

*Note: Estimates dựa trên assumptions, cần monitor actual costs*

## Cost Optimization Checklist

- [x] Gzip compression enabled
- [x] Lifecycle policies configured
- [x] Partitioning và clustering
- [x] Incremental processing
- [ ] Cost monitoring dashboard
- [ ] Budget alerts setup
- [ ] Regular cost reviews
- [ ] Query optimization reviews

## Next Steps

1. **Monitor actual costs** trong 1-2 tháng
2. **Identify cost drivers** (storage, queries, compute)
3. **Optimize high-cost areas** dựa trên data
4. **Setup budget alerts** để prevent overruns
5. **Regular reviews** (monthly hoặc quarterly)

