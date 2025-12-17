"""
Quản lý cấu hình cho ETL pipeline.
File này chứa các settings và hàm lấy credentials từ Secret Manager.
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """
    Cấu hình ứng dụng được load từ environment variables.
    
    Bao gồm: GCP project settings, bucket names, dataset names, 
    Nhanh API configuration, và các settings khác.
    """
    
    # GCP Configuration
    gcp_project: str = Field(default="sync-nhanhvn-project", alias="GCP_PROJECT")
    gcp_region: str = Field(default="asia-southeast1", alias="GCP_REGION")
    
    # GCS Buckets
    bronze_bucket: str = Field(default="sync-nhanhvn-project", alias="BRONZE_BUCKET")
    silver_bucket: str = Field(default="sync-nhanhvn-project-silver", alias="SILVER_BUCKET")
    
    # BigQuery Datasets
    bronze_dataset: str = Field(default="bronze", alias="BRONZE_DATASET")
    silver_dataset: str = Field(default="silver", alias="SILVER_DATASET")
    gold_dataset: str = Field(default="gold", alias="GOLD_DATASET")
    target_dataset: str = Field(default="nhanhVN", alias="TARGET_DATASET")
    oneoffice_dataset: str = Field(default="oneoffice", alias="ONEOFFICE_DATASET")
    
    # Nhanh API Configuration
    nhanh_api_base_url: str = Field(
        default="https://pos.open.nhanh.vn",
        alias="NHANH_API_BASE_URL"
    )
    nhanh_rate_limit: int = Field(default=150, alias="NHANH_RATE_LIMIT")
    nhanh_rate_window: int = Field(default=30, alias="NHANH_RATE_WINDOW")
    nhanh_max_date_range_days: int = Field(default=31, alias="NHANH_MAX_DATE_RANGE_DAYS")
    
    # Partitioning Strategy (day or month)
    partition_strategy: str = Field(default="month", alias="PARTITION_STRATEGY")
    
    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # 1Office Configuration
    oneoffice_base_url: str = Field(
        default="https://minham.1office.vn/api",
        alias="ONEOFFICE_BASE_URL"
    )
    oneoffice_access_token: str = Field(default="", alias="ONEOFFICE_ACCESS_TOKEN")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from .env that are not defined in Settings


# Global settings instance
settings = Settings()


def get_nhanh_credentials() -> dict:
    """
    Lấy credentials của Nhanh API từ environment variables hoặc Secret Manager.
    
    Ưu tiên đọc từ environment variables (cho local testing):
    - NHANH_APP_ID: App ID của ứng dụng
    - NHANH_BUSINESS_ID: Business ID trên Nhanh.vn
    - NHANH_ACCESS_TOKEN: Access token để authenticate
    
    Nếu không có trong env, sẽ lấy từ Secret Manager (cho cloud):
    - nhanh-app-id
    - nhanh-business-id
    - nhanh-access-token
    
    Returns:
        dict: Dictionary chứa appId, businessId, và accessToken
        
    Raises:
        ValueError: Nếu không thể lấy được secret nào đó
    """
    credentials = {}
    
    # Ưu tiên đọc từ environment variables (cho local testing)
    app_id = os.getenv("NHANH_APP_ID")
    business_id = os.getenv("NHANH_BUSINESS_ID")
    access_token = os.getenv("NHANH_ACCESS_TOKEN")
    
    if app_id and business_id and access_token:
        # Có đủ credentials từ env
        credentials = {
            "appId": app_id.strip(),
            "businessId": business_id.strip(),
            "accessToken": access_token.strip()
        }
        return credentials
    
    # Nếu không có trong env, lấy từ Secret Manager (cho cloud)
    from google.cloud import secretmanager
    
    client = secretmanager.SecretManagerServiceClient()
    project_id = settings.gcp_project
    
    # Get secrets from Secret Manager
    secret_names = {
        "appId": f"projects/{project_id}/secrets/nhanh-app-id/versions/latest",
        "businessId": f"projects/{project_id}/secrets/nhanh-business-id/versions/latest",
        "accessToken": f"projects/{project_id}/secrets/nhanh-access-token/versions/latest",
    }
    
    for key, secret_name in secret_names.items():
        try:
            response = client.access_secret_version(request={"name": secret_name})
            # Strip whitespace and newlines from secret values
            credentials[key] = response.payload.data.decode("UTF-8").strip()
        except Exception as e:
            raise ValueError(f"Failed to retrieve secret {key}: {str(e)}")
    
    return credentials

