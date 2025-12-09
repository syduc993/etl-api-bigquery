"""
BigQuery Client - Shared utility for executing SQL queries.
"""
from google.cloud import bigquery
from src.config import settings


class BigQueryClient:
    """
    Client để thực thi SQL queries trên BigQuery.
    
    Sử dụng client này thay vì tạo client trực tiếp trong mỗi feature
    để đảm bảo consistency và dễ dàng mock khi testing.
    """
    
    def __init__(self, project_id: str = None, location: str = None):
        """
        Khởi tạo BigQuery client.
        
        Args:
            project_id: GCP Project ID (mặc định lấy từ settings)
            location: Default location cho jobs (mặc định lấy từ settings)
        """
        self.project_id = project_id or settings.gcp_project
        self.location = location or settings.gcp_region
        self.client = bigquery.Client(project=self.project_id, location=self.location)
    
    def execute_query(self, sql: str, **kwargs) -> bigquery.QueryJob:
        """
        Execute một SQL query.
        
        Args:
            sql: SQL query string
            **kwargs: Additional arguments cho QueryJobConfig
            
        Returns:
            QueryJob: BigQuery query job result
        """
        job_config = bigquery.QueryJobConfig(**kwargs) if kwargs else None
        return self.client.query(sql, job_config=job_config)
    
    def load_sql_file(self, file_path: str) -> str:
        """
        Load SQL content từ file.
        
        Args:
            file_path: Path đến SQL file
            
        Returns:
            str: SQL content
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def execute_sql_file(self, file_path: str, **kwargs) -> bigquery.QueryJob:
        """
        Load và execute SQL từ file.
        
        Args:
            file_path: Path đến SQL file
            **kwargs: Additional arguments
            
        Returns:
            QueryJob: BigQuery query job result
        """
        sql = self.load_sql_file(file_path)
        return self.execute_query(sql, **kwargs)
    
    def execute_script_from_file(self, script_path, params: dict = None) -> str:
        """
        Load SQL từ file, thay thế template parameters, và execute.
        
        Args:
            script_path: Path đến SQL file (string hoặc Path)
            params: Dict các template parameters để thay thế {key} trong SQL
            
        Returns:
            str: Job ID của query đã execute
        """
        # Load SQL content
        sql = self.load_sql_file(str(script_path))
        
        # Replace template parameters
        if params:
            for key, value in params.items():
                sql = sql.replace(f"{{{key}}}", str(value))
        
        # Execute query
        job = self.execute_query(sql)
        job.result()  # Wait for completion
        
        return job.job_id

