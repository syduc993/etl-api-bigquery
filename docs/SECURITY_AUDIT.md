# Security Audit Checklist

Checklist cho security audit của ETL pipeline.

## Authentication & Authorization

### ✅ Secrets Management
- [x] API credentials lưu trong Secret Manager (không hardcode)
- [x] Service account có least privilege permissions
- [x] Secrets được rotate định kỳ
- [x] Access logs được enable

### ✅ IAM Roles
- [x] Service account chỉ có permissions cần thiết:
  - `roles/storage.objectAdmin` cho GCS
  - `roles/bigquery.dataEditor` cho BigQuery
  - `roles/secretmanager.secretAccessor` cho Secret Manager
- [x] Không có admin roles không cần thiết
- [x] Column-level security cho PII data (nếu có)

## Data Protection

### ✅ Encryption
- [x] GCS: Default encryption at rest (enabled by default)
- [x] BigQuery: Default encryption (enabled by default)
- [x] In transit: TLS/SSL cho tất cả connections

### ✅ Data Classification
- [ ] PII data được identify và classify
- [ ] Policy tags được apply cho sensitive columns
- [ ] Data retention policies được setup

## Network Security

### ✅ API Communication
- [x] HTTPS only cho Nhanh API calls
- [x] Certificate validation enabled
- [x] Timeout settings configured

### ✅ GCP Network
- [x] VPC firewall rules (nếu dùng VPC)
- [x] Private IP cho Cloud Run (nếu cần)
- [x] No public endpoints exposed

## Access Control

### ✅ Logging & Monitoring
- [x] Cloud Logging enabled
- [x] Audit logs enabled
- [x] Access logs được review định kỳ

### ✅ Error Handling
- [x] Không expose sensitive info trong error messages
- [x] Error logs không chứa credentials
- [x] Stack traces được sanitize

## Code Security

### ✅ Dependencies
- [x] Dependencies được update thường xuyên
- [x] Security vulnerabilities được scan
- [x] No known CVEs trong dependencies

### ✅ Input Validation
- [x] API responses được validate
- [x] SQL injection prevention (parameterized queries)
- [x] Data type validation

## Compliance

### ✅ Data Retention
- [ ] Data retention policies được define
- [ ] Lifecycle policies cho GCS buckets
- [ ] BigQuery table expiration được setup

### ✅ Audit Trail
- [x] All operations được log
- [x] Watermark tracking cho audit
- [ ] Data lineage được document

## Recommendations

### High Priority
1. **Setup data retention policies**: Định nghĩa retention cho từng layer
2. **Enable column-level security**: Cho PII data nếu có
3. **Regular security scans**: Scan dependencies và infrastructure

### Medium Priority
1. **Data classification**: Classify data theo sensitivity
2. **Access reviews**: Review IAM permissions định kỳ
3. **Backup strategy**: Define backup và recovery procedures

### Low Priority
1. **VPC setup**: Nếu cần network isolation
2. **Private endpoints**: Nếu cần thêm security
3. **Multi-region**: Nếu cần disaster recovery

## Action Items

- [ ] Review và update IAM roles
- [ ] Setup data retention policies
- [ ] Enable column-level security cho PII
- [ ] Schedule regular security audits
- [ ] Document data classification
- [ ] Setup backup và recovery procedures

