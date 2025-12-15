# Chi tiết Logic từng Báo cáo HR

## 1. Báo cáo nhân sự mới tạo tài khoản

**Logic:**
- Query nhân sự có `date_trigger_user = CURRENT_DATE()` hoặc trong vòng 24h qua
- Filter: `date_trigger_user IS NOT NULL`

**SQL Pattern:**
```sql
SELECT code, full_name, user_id, date_trigger_user, department_id
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND date_trigger_user = CURRENT_DATE()
ORDER BY date_trigger_user DESC
```

## 2. Cảnh báo không đủ hồ sơ

**Logic:**
- Nhân sự đang làm việc (`job_status = 'WORKING'`)
- Thiếu ít nhất một trong: ảnh CMND mặt trước, mặt sau, hoặc không có file đính kèm
- Check: `image_private_front = '0'` OR `image_private_back = '0'` OR `num_file = 0` OR `private_code_date IS NULL`

**SQL Pattern:**
```sql
SELECT code, full_name, department_id,
       CASE 
         WHEN image_private_front = '0' THEN 'Thiếu ảnh CMND mặt trước'
         WHEN image_private_back = '0' THEN 'Thiếu ảnh CMND mặt sau'
         WHEN num_file = 0 THEN 'Không có file đính kèm'
         WHEN private_code_date IS NULL THEN 'Thiếu ngày cấp CCCD'
       END AS missing_item
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND (
    image_private_front = '0'
    OR image_private_back = '0'
    OR COALESCE(num_file, 0) = 0
    OR private_code_date IS NULL
  )
```

## 3. Cảnh báo chưa ký hợp đồng

**Logic:**
- Nhân sự đang làm việc (`job_status = 'WORKING'`)
- Nhưng `job_contract IS NULL` hoặc rỗng
- Có thể thêm điều kiện: `job_date_join < CURRENT_DATE()` (đã vào làm)

**SQL Pattern:**
```sql
SELECT code, full_name, department_id, job_date_join, job_reldate_join
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND (job_contract IS NULL OR job_contract = '')
  AND job_date_join < CURRENT_DATE()
```

## 4. Tự nhắc lịch ký hợp đồng

**Logic:**
- Nhân sự đang làm việc
- Có `job_reldate_join` (ngày ký HĐ chính thức)
- Tính ngày hết hạn dự kiến (giả định HĐ 1 năm hoặc 2 năm)
- Nhắc trước 30 ngày: `CURRENT_DATE() BETWEEN DATE_SUB(DATE_ADD(job_reldate_join, INTERVAL 1 YEAR), INTERVAL 30 DAY) AND DATE_ADD(job_reldate_join, INTERVAL 1 YEAR)`

**SQL Pattern:**
```sql
SELECT code, full_name, department_id, job_reldate_join,
       DATE_ADD(job_reldate_join, INTERVAL 1 YEAR) AS contract_expiry_date,
       DATE_DIFF(DATE_ADD(job_reldate_join, INTERVAL 1 YEAR), CURRENT_DATE(), DAY) AS days_until_expiry
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND job_reldate_join IS NOT NULL
  AND CURRENT_DATE() BETWEEN DATE_SUB(DATE_ADD(job_reldate_join, INTERVAL 1 YEAR), INTERVAL 30 DAY) 
                          AND DATE_ADD(job_reldate_join, INTERVAL 1 YEAR)
```

## 5. Nhắc lịch thử việc

**Logic:**
- Nhân sự đang làm việc
- Có `job_date_end_review` (ngày hết hạn thử việc)
- Nhắc trong vòng 7 ngày tới: `job_date_end_review BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)`

**SQL Pattern:**
```sql
SELECT code, full_name, department_id, job_date_end_review,
       DATE_DIFF(job_date_end_review, CURRENT_DATE(), DAY) AS days_until_end
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'WORKING'
  AND job_date_end_review IS NOT NULL
  AND job_date_end_review BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY job_date_end_review ASC
```

## 6. Báo cáo chấm dứt hợp đồng

**Logic:**
- Nhân sự có `job_status = 'STOP_WORKING'`
- `job_date_out = CURRENT_DATE()` hoặc trong vòng 7 ngày qua

**SQL Pattern:**
```sql
SELECT code, full_name, department_id, job_date_out, job_out_reason, user_id
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'STOP_WORKING'
  AND job_date_out >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY job_date_out DESC
```

## 7. Cảnh báo đã nghỉ việc nhưng chưa xóa tài khoản

**Logic:**
- Nhân sự có `job_status = 'STOP_WORKING'`
- Nhưng vẫn có `user_id IS NOT NULL` và `user_id != ''`
- Có thể thêm điều kiện: `job_date_out < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)` (đã nghỉ > 7 ngày)

**SQL Pattern:**
```sql
SELECT code, full_name, department_id, job_date_out, user_id,
       DATE_DIFF(CURRENT_DATE(), job_date_out, DAY) AS days_since_termination
FROM `{project}.{dataset}.hr_profile_daily_snapshot`
WHERE snapshot_date = CURRENT_DATE()
  AND job_status = 'STOP_WORKING'
  AND (user_id IS NOT NULL AND user_id != '')
  AND job_date_out < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY job_date_out DESC
```

## 8. Báo cáo điều chuyển/luân chuyển

**Logic:**
- So sánh snapshot hôm nay (T) với hôm qua (T-1)
- Phát hiện thay đổi: `department_id`, `position_id`, hoặc `job_title`
- Self-join hoặc window function để so sánh

**SQL Pattern:**
```sql
WITH yesterday_data AS (
  SELECT code, department_id, position_id, job_title, full_name
  FROM `{project}.{dataset}.hr_profile_daily_snapshot`
  WHERE snapshot_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
today_data AS (
  SELECT code, department_id, position_id, job_title, full_name
  FROM `{project}.{dataset}.hr_profile_daily_snapshot`
  WHERE snapshot_date = CURRENT_DATE()
)
SELECT
  t.code,
  t.full_name,
  y.department_id AS old_department,
  t.department_id AS new_department,
  y.position_id AS old_position,
  t.position_id AS new_position,
  y.job_title AS old_job_title,
  t.job_title AS new_job_title,
  CASE
    WHEN t.department_id != y.department_id THEN 'Điều chuyển phòng ban'
    WHEN t.position_id != y.position_id THEN 'Thay đổi vị trí'
    WHEN t.job_title != y.job_title THEN 'Thay đổi chức vụ'
  END AS change_type
FROM today_data t
INNER JOIN yesterday_data y ON t.code = y.code
WHERE
  (t.department_id != y.department_id)
  OR (t.position_id != y.position_id)
  OR (t.job_title != y.job_title)
```

