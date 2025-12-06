# Tài liệu Xây dựng Lakehouse trên Google Cloud

Thư mục này chứa các tài liệu tổng hợp về kiến trúc và triển khai Lakehouse trên Google Cloud Platform.

## Mục lục

1. [Tổng quan về Lakehouse trên GCP](./01-OVERVIEW.md)
2. [Các thành phần chính](./02-COMPONENTS.md)
3. [Kiến trúc và Patterns](./03-ARCHITECTURE.md)
4. [Implementation Guide](./04-IMPLEMENTATION.md)
5. [Best Practices](./05-BEST-PRACTICES.md)
6. [Open Formats hỗ trợ](./06-OPEN-FORMATS.md)
7. [Data Governance với Dataplex](./07-DATAPLEX.md)
8. [Cloud Run Jobs cho Pipeline](./08-CLOUD-RUN-JOBS.md)

## Tổng quan

Lakehouse là một kiến trúc dữ liệu hiện đại kết hợp các đặc điểm của Data Lake và Data Warehouse, cho phép:
- Lưu trữ dữ liệu có cấu trúc và phi cấu trúc
- Hỗ trợ các định dạng mở (Apache Iceberg, Delta Lake, Hudi)
- ACID transactions
- Query performance cao như data warehouse
- Cost-effective storage như data lake

## Các dịch vụ Google Cloud chính

- **BigQuery**: Serverless data warehouse với khả năng lakehouse
- **Cloud Storage**: Object storage cho data lake
- **BigLake**: Unified storage layer
- **Dataplex**: Data governance và management
- **Dataproc**: Managed Apache Spark/Hadoop
- **Dataflow**: Serverless data processing
- **Cloud Run Jobs**: Python scripts execution
- **Cloud Scheduler**: Cron-based job scheduling

## Tài liệu tham khảo

- [Google Cloud Official Documentation](https://cloud.google.com/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Dataplex Documentation](https://cloud.google.com/dataplex/docs)
- [Building a Data Lakehouse on GCP White Paper](https://services.google.com/fh/files/misc/building-a-data-lakehouse.pdf)

---

**Ngày tạo:** 2024-12-06  
**Cập nhật lần cuối:** 2024-12-06

