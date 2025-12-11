# Setup GitHub Secrets cho GCP_SA_KEY

Hướng dẫn setup Service Account key cho GitHub Actions workflow.

## Bước 1: Kiểm tra Service Account đã có

Service Account đã được tạo:
- **Email**: `github-actions@sync-nhanhvn-project.iam.gserviceaccount.com`
- **Display Name**: GitHub Actions Service Account

## Bước 2: Đảm bảo Service Account có đủ permissions

Service Account cần các permissions sau:

**Cách 1: Sử dụng script tự động (khuyến nghị)**

```bash
cd infrastructure/scripts
chmod +x grant-github-actions-permissions.sh
./grant-github-actions-permissions.sh
```

**Cách 2: Chạy thủ công**

```bash
PROJECT_ID="sync-nhanhvn-project"
SA_EMAIL="github-actions@sync-nhanhvn-project.iam.gserviceaccount.com"

# Storage Admin - Cần để list, create, và manage GCS buckets
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.admin"

# BigQuery Data Owner - Cần để create datasets và manage tables
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataOwner"

# BigQuery Job User - Cần để execute queries
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"
```

**Lưu ý**: Các permissions có thể mất vài phút để propagate. Nếu gặp lỗi permission ngay sau khi grant, đợi 2-3 phút rồi thử lại.

## Bước 3: Tạo JSON Key cho Service Account

```bash
PROJECT_ID="sync-nhanhvn-project"
SA_EMAIL="github-actions@sync-nhanhvn-project.iam.gserviceaccount.com"
KEY_FILE="github-actions-key.json"

# Tạo key và download về file
gcloud iam service-accounts keys create ${KEY_FILE} \
  --iam-account=${SA_EMAIL} \
  --project=${PROJECT_ID}

# Xem nội dung key (để copy vào GitHub Secrets)
cat ${KEY_FILE}
```

**Lưu ý**: File `github-actions-key.json` chứa credentials nhạy cảm. **KHÔNG commit file này vào Git!**

## Bước 4: Thêm Key vào GitHub Secrets

1. **Vào GitHub Repository**:
   - Vào repo: `https://github.com/syduc993/etl-api-bigquery`
   - Click **Settings** → **Secrets and variables** → **Actions**

2. **Tạo Secret mới**:
   - Click **New repository secret**
   - **Name**: `GCP_SA_KEY`
   - **Secret**: Paste toàn bộ nội dung file JSON (từ `{` đến `}`)
   - Click **Add secret**

3. **Xác nhận**:
   - Secret `GCP_SA_KEY` sẽ xuất hiện trong danh sách
   - Workflow sẽ tự động sử dụng secret này

## Bước 5: Xóa file key local (bảo mật)

```bash
# Xóa file key sau khi đã thêm vào GitHub Secrets
rm github-actions-key.json

# Hoặc nếu đã commit nhầm, xóa khỏi Git history
git rm --cached github-actions-key.json
git commit -m "Remove service account key"
```

## Bước 6: Test Workflow

1. Vào **Actions** tab trên GitHub
2. Chọn workflow **"Daily Bills Sync - 1AM VN Time"**
3. Click **"Run workflow"** → **"Run workflow"**
4. Kiểm tra logs để đảm bảo authentication thành công

## Troubleshooting

### Lỗi: "Your default credentials were not found"
- Kiểm tra secret `GCP_SA_KEY` đã được tạo trong GitHub Secrets
- Đảm bảo JSON key format đúng (có `{` và `}`)

### Lỗi: "Permission denied"
- Kiểm tra Service Account có đủ permissions (BigQuery, Storage)
- Chạy lại các lệnh gán permissions ở Bước 2

### Lỗi: "Service account key expired"
- Tạo key mới và update lại GitHub Secret
- Keys cũ sẽ tự động expire sau một thời gian

## Security Best Practices

1. ✅ **Không commit key vào Git** - Key đã được thêm vào `.gitignore`
2. ✅ **Rotate keys định kỳ** - Tạo key mới mỗi 90 ngày
3. ✅ **Least privilege** - Chỉ gán permissions cần thiết
4. ✅ **Monitor usage** - Kiểm tra Cloud Logging để theo dõi access

