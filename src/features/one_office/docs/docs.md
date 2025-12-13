TÀI LIỆU KỸ THUẬT: TÍCH HỢP & XỬ LÝ DỮ LIỆU NHÂN SỰ 1OFFICE
1. Kiến trúc luồng dữ liệu (Data Pipeline)
Để giải quyết các bài toán cần so sánh lịch sử (như điều chuyển nhân sự) và báo cáo trạng thái hiện tại, chúng ta sẽ áp dụng mô hình Daily Snapshot (Chụp ảnh dữ liệu hàng ngày).
Quy trình tổng quan:
Extract (Lấy dữ liệu): Gọi API personnel/profile/gets hàng ngày vào một khung giờ cố định (ví dụ: 01:00 AM).
Load (Lưu trữ): Lưu toàn bộ dữ liệu lấy được vào Google BigQuery. Mỗi ngày là một Partition (phân vùng) hoặc gán nhãn thời gian snapshot_date.
Transform (Xử lý): Dùng SQL để query, so sánh dữ liệu ngày hôm nay (T) so với ngày hôm qua (T-1) để tìm ra sự thay đổi.
Cấu trúc bảng BigQuery đề xuất (hr_profile_daily_snapshot)
Column Name	Data Type	Mô tả
snapshot_date	DATE	Ngày lấy dữ liệu (VD: 2023-10-25)
code	STRING	Mã nhân sự (Key để định danh)
full_name	STRING	Họ tên (API không trả trực tiếp, mapping từ user_id hoặc desc nếu có)
department_id	STRING	ID Phòng ban
position_id	STRING	ID Vị trí
job_status	STRING	Trạng thái (WORKING, STOP_WORKING...)
user_id	STRING	Tài khoản đăng nhập hệ thống
raw_data	JSON	Lưu toàn bộ response gốc để dự phòng
...	...	Các trường chi tiết khác (ngày vào, ngày sinh...)
2. Chi tiết triển khai các bài toán
Phần 1: Các bài toán giải quyết ĐƯỢC NGAY (Logic trên Snapshot hiện tại)
Các bài toán này chỉ cần truy vấn dữ liệu của ngày mới nhất (snapshot_date = TODAY).
1.1. Báo cáo nhân sự mới tạo tài khoản
Mục đích: Giám sát việc cấp tài khoản mới.
Logic: Tìm nhân sự có ngày tạo tài khoản trùng với ngày hiện tại (hoặc ngày hôm qua nếu chạy job vào sáng sớm).
Trường dữ liệu: date_trigger_user (Ngày tạo TK 1Office).
SQL Query (Ví dụ):
code
SQL
SELECT code, user_id, date_trigger_user
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND date_trigger_user = CURRENT_DATE(); -- Hoặc DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
1.2. Nhắc lịch thử việc (Sắp hết hạn)
Mục đích: Cảnh báo trước X ngày để BLĐ đánh giá.
Logic: Tìm nhân sự có ngày hết hạn thử việc nằm trong khoảng 7 ngày tới.
Trường dữ liệu: job_date_end_review (Ngày hết hạn thử việc), job_status = 'WORKING'.
SQL Query:
code
SQL
SELECT code, job_date_end_review
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND job_date_end_review BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY);
1.3. Báo cáo chấm dứt hợp đồng (Nghỉ việc)
Mục đích: Danh sách nhân sự vừa nghỉ việc.
Logic: Nhân sự có trạng thái nghỉ việc và ngày nghỉ là hôm nay.
Trường dữ liệu: job_status = 'STOP_WORKING', job_date_out.
SQL Query:
code
SQL
SELECT code, job_out_reason, job_date_out
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'STOP_WORKING'
  AND job_date_out = CURRENT_DATE();
1.4. Cảnh báo không đủ hồ sơ
Mục đích: Đảm bảo Compliance hồ sơ nhân sự.
Logic: Nhân sự đang làm việc nhưng thiếu ảnh CMND/CCCD hoặc thiếu file đính kèm.
Trường dữ liệu:
job_status = 'WORKING'
image_private_front: 0 (Chưa có)
image_private_back: 0 (Chưa có)
num_file: 0 (Số lượng file = 0)
SQL Query:
code
SQL
SELECT code, department_id
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND (
      image_private_front = '0'
      OR image_private_back = '0'
      OR num_file = 0
      OR private_code_date IS NULL -- Ví dụ thêm: Thiếu ngày cấp CCCD
  );
1.5. Cảnh báo đã nghỉ việc nhưng không xoá tài khoản (Rủi ro bảo mật)
Mục đích: Thu hồi quyền truy cập hệ thống của nhân viên cũ.
Logic: Trạng thái là "Nghỉ việc" nhưng trường tài khoản hệ thống vẫn tồn tại dữ liệu.
Trường dữ liệu: job_status = 'STOP_WORKING', user_id (hoặc raw_user_id).
SQL Query:
code
SQL
SELECT code, user_id, job_date_out
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'STOP_WORKING'
  AND (user_id IS NOT NULL AND user_id != '');
Phần 2: Các bài toán giải quyết ĐƯỢC MỘT PHẦN (Cần logic so sánh T vs T-1)
Các bài toán này yêu cầu so sánh dữ liệu ngày hôm nay với dữ liệu ngày hôm qua trong BigQuery.
2.1. Báo cáo điều chuyển / Luân chuyển nhân sự
Mục đích: Phát hiện nhân sự chuyển phòng ban hoặc thay đổi vị trí/chức vụ.
Logic:
Lấy bản ghi của nhân sự ở ngày hôm nay (T).
Lấy bản ghi của nhân sự đó ở ngày hôm qua (T-1).
So sánh department_id hoặc position_id. Nếu khác nhau -> Có điều chuyển.
SQL Query (Sử dụng Self-Join hoặc Window Function):
code
SQL
WITH yesterday_data AS (
    SELECT code, department_id, position_id, job_title
    FROM `hr_profile_daily_snapshot`
    WHERE snapshot_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
today_data AS (
    SELECT code, department_id, position_id, job_title
    FROM `hr_profile_daily_snapshot`
    WHERE snapshot_date = CURRENT_DATE()
)
SELECT
    t.code,
    y.department_id AS old_dept,
    t.department_id AS new_dept,
    y.position_id AS old_pos,
    t.position_id AS new_pos
FROM today_data t
JOIN yesterday_data y ON t.code = y.code
WHERE
    t.department_id != y.department_id -- Có thay đổi phòng ban
    OR t.position_id != y.position_id; -- Hoặc thay đổi vị trí
2.2. Cảnh báo chưa ký/cập nhật hợp đồng
Mục đích: Phát hiện nhân sự đang làm việc chính thức nhưng trên hệ thống chưa ghi nhận loại hợp đồng.
Vấn đề: API không trả về "đã ký hay chưa" (boolean).
Logic xử lý:
Trạng thái là WORKING (Đang làm việc chính thức, không phải thử việc/thực tập).
Nhưng trường job_contract (Tên hợp đồng) lại Rỗng hoặc NULL.
SQL Query:
code
SQL
SELECT code, job_date_join
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND (job_contract IS NULL OR job_contract = '');
2.3. Tự nhắc lịch ký hợp đồng (Logic suy luận)
Mục đích: Nhắc nhở tái ký hợp đồng.
Hạn chế: API không có trường contract_end_date (Ngày hết hạn HĐ).
Logic suy luận (Workaround):
Trường hợp 1: Hết hạn thử việc. Dùng job_date_end_review (đã làm ở mục 1.2).
Trường hợp 2: Tái ký HĐ. Phải tự tính toán dựa trên job_reldate_join (Ngày ký HĐ chính thức).
Giả định hợp đồng 1 năm: Kiểm tra nếu TODAY = job_reldate_join + 1 Năm - 15 Ngày (Nhắc trước 15 ngày).
SQL Query (Ví dụ nhắc tái ký HĐ 1 năm):
code
SQL
SELECT code, job_reldate_join,
       DATE_ADD(job_reldate_join, INTERVAL 1 YEAR) AS estimated_expire_date
FROM `hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  -- Logic: Ngày hiện tại = (Ngày ký + 1 năm) - 30 ngày (Nhắc trước 1 tháng)
  AND CURRENT_DATE() = DATE_SUB(DATE_ADD(job_reldate_join, INTERVAL 1 YEAR), INTERVAL 30 DAY);
3. Lưu ý khi cấu hình Job Get Data
Pagination (Phân trang):
API giới hạn tối đa 100 bản ghi/lần (limit=100).
Cần viết vòng lặp (loop) tăng tham số page (1, 2, 3...) cho đến khi mảng dữ liệu trả về rỗng.
Access Token: Đảm bảo token 80391498468e8ae4c02f02819998254 còn hiệu lực và có cơ chế refresh token nếu API yêu cầu.
Xử lý dữ liệu Rỗng: Các trường ID phòng ban, vị trí có thể trả về chuỗi rỗng "" thay vì null. Cần chuẩn hóa khi đưa vào BigQuery (chuyển hết về NULL để dễ query).
4. Tổng kết file Script mẫu (Python pseudo-code)
code
Python
import requests
from google.cloud import bigquery
from datetime import date

def fetch_all_personnel():
    all_data = []
    page = 1
    while True:
        url = "https://minham.1office.vn/api/personnel/profile/gets"
        params = {
            "access_token": "YOUR_TOKEN",
            "limit": 100,
            "page": page
        }
        response = requests.get(url, params=params).json()
        
        # Kiểm tra cấu trúc trả về thực tế của 1Office để lấy list
        data_list = response.get('data', []) 
        
        if not data_list:
            break
            
        all_data.extend(data_list)
        page += 1
    return all_data

def load_to_bigquery(data):
    today = date.today().isoformat()
    rows_to_insert = []
    
    for item in data:
        # Chuẩn hóa dữ liệu trước khi insert
        row = {
            "snapshot_date": today,
            "code": item.get("code"),
            "department_id": item.get("department_id"),
            # ... map các trường khác tương ứng ...
            "raw_data": str(item) # Lưu raw để debug
        }
        rows_to_insert.append(row)
    
    client = bigquery.Client()
    table_id = "your_project.your_dataset.hr_profile_daily_snapshot"
    
    # Insert data (Nên dùng LoadJob cho dữ liệu lớn thay vì insert_rows_json)
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors: ", errors)

# Main execution
data = fetch_all_personnel()
load_to_bigquery(data)