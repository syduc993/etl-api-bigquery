# Setup secrets in Secret Manager for Windows PowerShell

$PROJECT_ID = "sync-nhanhvn-project"

Write-Host "Setting up secrets in Secret Manager..." -ForegroundColor Green
Write-Host ""

Write-Host "Please provide the following values:" -ForegroundColor Yellow

$APP_ID = Read-Host "Nhanh App ID"
$BUSINESS_ID = Read-Host "Nhanh Business ID"
$ACCESS_TOKEN = Read-Host "Nhanh Access Token" -AsSecureString
$ACCESS_TOKEN_PLAIN = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($ACCESS_TOKEN)
)

# Create secrets
Write-Host "Creating secrets..." -ForegroundColor Green

# App ID
$APP_ID | gcloud secrets create nhanh-app-id `
  --data-file=- `
  --project=$PROJECT_ID `
  2>$null
if ($LASTEXITCODE -ne 0) {
    $APP_ID | gcloud secrets versions add nhanh-app-id `
      --data-file=- `
      --project=$PROJECT_ID
}

# Business ID
$BUSINESS_ID | gcloud secrets create nhanh-business-id `
  --data-file=- `
  --project=$PROJECT_ID `
  2>$null
if ($LASTEXITCODE -ne 0) {
    $BUSINESS_ID | gcloud secrets versions add nhanh-business-id `
      --data-file=- `
      --project=$PROJECT_ID
}

# Access Token
$ACCESS_TOKEN_PLAIN | gcloud secrets create nhanh-access-token `
  --data-file=- `
  --project=$PROJECT_ID `
  2>$null
if ($LASTEXITCODE -ne 0) {
    $ACCESS_TOKEN_PLAIN | gcloud secrets versions add nhanh-access-token `
      --data-file=- `
      --project=$PROJECT_ID
}

Write-Host ""
Write-Host "Secrets created successfully!" -ForegroundColor Green
Write-Host ""
Write-Host "Granting access to service account..." -ForegroundColor Green

$SERVICE_ACCOUNT = "$PROJECT_ID@appspot.gserviceaccount.com"

gcloud secrets add-iam-policy-binding nhanh-app-id `
  --member="serviceAccount:$SERVICE_ACCOUNT" `
  --role="roles/secretmanager.secretAccessor" `
  --project=$PROJECT_ID

gcloud secrets add-iam-policy-binding nhanh-business-id `
  --member="serviceAccount:$SERVICE_ACCOUNT" `
  --role="roles/secretmanager.secretAccessor" `
  --project=$PROJECT_ID

gcloud secrets add-iam-policy-binding nhanh-access-token `
  --member="serviceAccount:$SERVICE_ACCOUNT" `
  --role="roles/secretmanager.secretAccessor" `
  --project=$PROJECT_ID

Write-Host ""
Write-Host "Setup completed!" -ForegroundColor Green

