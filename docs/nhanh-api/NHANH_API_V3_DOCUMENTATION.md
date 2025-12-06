# Nhanh.vn Open API - Version 3.0 Documentation

**Nguồn:** https://apidocs.nhanh.vn/v3  
**Phiên bản:** 3.0  
**Cập nhật lần cuối:** 2024-12-05

---

## Mục lục

1. [Giới thiệu](#giới-thiệu)
2. [Khởi tạo ứng dụng](#khởi-tạo-ứng-dụng)
3. [Request Parameters](#request-parameters)
4. [Paginator](#paginator)
5. [Postman Samples](#postman-samples)
6. [Response Format](#response-format)
7. [Error Codes](#error-codes)
8. [API Rate Limit](#api-rate-limit)
9. [Danh sách API Endpoints](#danh-sách-api-endpoints)

---

## Giới thiệu

NhanhAPI version **3.0**

---

## Khởi tạo ứng dụng

- Xem cách khởi tạo ứng dụng tại: https://apidocs.nhanh.vn/app
- Xem cách đăng nhập cấp quyền và lấy accessToken tại: https://apidocs.nhanh.vn/app#lay-access-token

---

## Request Parameters

Sau khi lấy được accessToken, bạn có thể bắt đầu gọi các API. Mỗi service của Nhanh sẽ có các tên miền riêng:

- **POS**: `https://pos.open.nhanh.vn`
- **Vpage**: `https://vpage.open.nhanh.vn`

Version 3.0 sẽ dùng **POST** method, headers, body **raw** và các params bắt buộc như sau:

> **Chú ý:** Nếu param type có dấu * là bắt buộc (required), nếu không có là không bắt buộc (optional).

| Param | Type | Description |
|-------|------|-------------|
| appId | int * | ID app của bạn (Truyền lên link API) |
| businessId | int * | ID doanh nghiệp trên Nhanh.vn. Lúc bạn lấy accessToken có trả về businessId (Truyền lên link API) |
| Authorization | string * | Access token của bạn (Truyền lên Headers) |

---

## Paginator

### Request

Khi gọi API, bạn có thể truyền thêm paginator để lấy các dữ liệu phân trang. Paginator thường có 3 field chính:

- `size` (int): Số lượng bản ghi trên 1 trang. Mặc định tối đa không quá 100, nếu API hỗ trợ số lớn hơn, sẽ được mô tả rõ ở từng API.
- `sort` (array | object): Các tiêu chí sắp xếp, xem mô tả ở tài liệu của từng API.
- `next` (array | object): Dùng để gọi dữ liệu cho trang tiếp theo.
  - Khi gọi API cho trang đầu tiên, thì không cần truyền `next`, khi nhận được response có paginator.next, thì request tiếp theo bạn cần truyền `next` để lấy dữ liệu cho trang sau. Tùy từng API, hoặc cùng 1 API nhưng 1 số page cũng có thể có cấu trúc next trả về khác nhau.

### Lỗi thường hay gặp phải:

- Response trả về `"paginator":{"next":{"id":"xxx"}}`, thì request lấy trang tiếp theo cần truyền đúng `"paginator":{"next":{"id":"xxx"}}` chứ không phải `"paginator":{"next":"xxx"}`
- Response trả về `"paginator":{"next":{"totalRevenue":"xxx", "productId":"yyy"}}`, thì request lấy trang tiếp theo cần truyền đúng `"paginator":{"next":{"totalRevenue":"xxx", "productId":"yyy"}}` chứ không phải `"paginator":{"next": {"xxx", "yyy"}}`.
- Nếu response không trả về next, hoặc next bị NULL, nghĩa là đã hết dữ liệu, bạn không cần gọi API lấy trang tiếp theo.
- Nếu response trả về dữ liệu ít hơn số lượng `size` (VD có 1 số tình huống dữ liệu bị xóa hoặc đã bị filter ẩn đi), nhưng vẫn có `next`, nghĩa là vẫn còn dữ liệu ở trang tiếp theo.

### Response

- `next` (array | object): Giá trị phân trang cho trang tiếp theo.

---

## Postman Samples

### Lấy danh sách sản phẩm

```bash
curl --location --globoff 'https://pos.open.nhanh.vn/v3.0/product/list?appId={{appId}}&businessId={{businessId}}' \
--header 'Authorization: {{accessToken}}' \
--header 'Content-Type: application/json' \
--data '{
    "filters": {
        "name": "Áo sơ mi"
    },
    "paginator": {
        "size": 50,
        "sort": {"id": "desc"},
        "next": ""
    }
}'
```

### Thêm sản phẩm

```bash
curl --location --globoff 'https://pos.open.nhanh.vn/v3.0/product/add?appId={{appId}}&businessId={{businessId}}' \
--header 'Authorization: {{accessToken}}' \
--header 'Content-Type: application/json' \
--data '[
  {
    "appProductId": "string",
    "code": "Product Code 1",
    "barcode": "2000214262896",
    "name": "Product name 1",
    "price": 120000
  },
  {
    "appProductId": "string",
    "code": "Product Code 2",
    "barcode": "2000214262889",
    "name": "Product name 2",
    "price": 175000
  }
]'
```

### Postman Collection

Bạn có thể dùng [Postman Collection v3.0](https://www.postman.com/nhanh-vn/pos-open-nhanh-vn) tham khảo params cho 1 số API hay dùng.

**Để sử dụng:**
1. Click vào collection mình cần -> click **Fork**
2. Ở phần **Create Fork**: điền **Fork label** (Tên collection ở Workspace của bạn), **Workspace** (Chọn một không gian làm việc mà bạn muốn phân nhánh), xong click **Fork Collection**

> **Khuyến cáo:** Bạn nên tải Postman về, cài đặt và dùng ở localhost, Bản Postman chạy online trên web, có 1 số tình huống ghi nhận Response time bị sai.

### Code Sample

Bạn có thể dùng Postman điền các params và click vào Code Snippet trên Postman để xem cách tạo syntax cho các ngôn ngữ (Nodejs, PHP, C#, Go, Java...).

---

## Response Format

NhanhAPI sẽ phản hồi mã lỗi cho 2 tình huống:
- Successful: `"code": 1`
- Failed: `"code": 0`

### Successful Response

Tùy API sẽ có thể chỉ trả về `{"code": 1}`, có thể thêm `data` (object, array), `warning` (string, object, array).

**Ví dụ với data:**
```json
{
    "code": 1,
    "data": [
        {
            "appProductId": "X1aoCM",
            "id": 421509,
            "barcode": "2000214262919"
        }
    ]
}
```

**Ví dụ với warning:**
```json
{
  "code": 1,
  "warning": ["orderPrivateId is deprected, you should use appOrderId"]
}
```

### Failed Response

Khi gọi API bị lỗi, bạn sẽ nhận được phản hồi `"code": 0`, và có thể kèm theo các field khác như:

- `errorCode`: mã lỗi. Xem chi tiết [mã lỗi chung](#common-error-codes), mã lỗi riêng của POS hoặc Vpage.
- `messages`: có thể là 1 **string** (VD: `"messages": "content"`) hoặc **array** (VD: `"messages" : ["field_1": "message_1", "field_2": "message_2"]`.
- `data`: Dữ liệu lỗi kèm theo.

**Ví dụ:**
```json
{
  "code": 0,
  "errorCode": "ERR_INVALID_FORM_FIELDS",
  "messages": {
    "customer.mobile": "customer.mobile is required",
    "customer.email": "invalid customer.email"
  }
}
```

---

## Error Codes

### Common Error Codes

Bảng bên dưới mô tả mã lỗi chung cho cả POS và Vpage.

| errorCode | Description |
|-----------|-------------|
| ERR_INVALID_APP_ID | Invalid appId |
| ERR_INVALID_BUSINESS_ID | Invalid businessId |
| ERR_INVALID_ACCESS_TOKEN | Invalid access token |
| ERR_INVALID_VERSION | Invalid version. VD bạn lấy accessToken v2.0 gọi API v3.0 |
| ERR_EXCEEDED_RATE_LIMIT | App của bạn đã vượt quá API Rate Limit |
| ERR_BUSINESS_NOT_ENABLE_API | Doanh nghiệp chưa mở cài đặt cho phép dùng API. |
| ERR_INVALID_FORM_FIELDS | Trường dữ liệu không hợp lệ, khi gặp mã lỗi này thì thường `messages` sẽ trả về mảng `["field": "error" ]`, VD `["id": "id must be integer" ]` |
| ERR_INVALID_DATA | Invalid data. Lỗi này do data không phải là 1 chuỗi json string hợp lệ, thường do bạn không dùng các hàm json encode mà gõ thủ công gây thừa thiếu dấu hoặc không encode các kí tự đặc biệt. Bạn có thể kiểm tra chuỗi data json string bằng cách vào https://jsonformatter.org copy paste chuỗi của bạn vào textarea và click Validate. |
| ERR_403 | Access token không có quyền thao tác với dữ liệu, thường do user khi đăng nhập cấp quyền không chọn quyền này. VD để API lấy được danh sách đơn hàng, thì user phải có quyền truy cập trang danh sách đơn hàng trên Nhanh.vn, và phải chọn danh sách đơn hàng ở bước cấp quyền cho app. |
| ERR_429 | App exceeded the API Rate Limit |

### POS Common Error Codes

Bảng bên dưới là các mã lỗi chung của POS. Từng API có thể có thêm các mã lỗi riêng, bạn xem tại mô tả Response của từng API.

### Vpage Common Error Codes

| errorCode | Description |
|-----------|-------------|
| ERR_PAGE_EXPIRED | Page expired |
| ERR_INVALID_PAGE_ID | Invalid page id |

---

## API Rate Limit

### Tổng quan

Rate Limit là số lệnh gọi API mà ứng dụng của bạn có thể thực hiện trong khoảng thời gian nhất định. Nếu vượt quá giới hạn này thì ứng dụng có thể bị giới hạn tốc độ. Các lệnh gọi API do ứng dụng đang bị giới hạn tốc độ sẽ không thành công. Lỗi này thường gặp phải khi ứng dụng không lưu dữ liệu mà luôn gọi API (VD liên tục gọi API lấy danh sách sản phẩm, danh sách đơn hàng...).

> **Khuyến cáo:** Bạn cần lưu dữ liệu ở hệ thống của bên bạn, cập nhật dữ liệu mới từ webhooks, khi gọi API chỉ nên lấy các dữ liệu có thay đổi, không lấy toàn bộ dữ liệu cũ (các API có hỗ trợ lọc theo updatedAtFrom - updatedAtTo).

### Cơ chế Rate Limit

- Rate Limit sẽ được kết hợp từ appId + businessId + API URL, nên ứng dụng của bạn có thể sẽ bị giới hạn ở 1 URL này, nhưng vẫn có thể gọi URL khác nếu URL đó không bị giới hạn, hoặc nếu ứng dụng của bạn dùng cho nhiều doanh nghiệp, thì bạn có thể bị giới hạn với businessId 1, nhưng vẫn có thể dùng được với businessId 2.

> **Chú ý:** Nếu bạn cố tình dùng nhiều ứng dụng để gọi liên tục API, hệ thống sẽ khóa toàn bộ các ứng dụng. Các ứng dụng bị khóa, sẽ không thể gọi bất kì API nào nữa.

- Rate Limit sẽ có mức chung mặc định là: **150 requests / 30 giây**. Nếu 1 API có mức riêng, thì tài liệu của API đó sẽ có mô tả riêng.

### Xử lý khi vượt Rate Limit

Khi vượt quá Rate Limit, bạn sẽ nhận được errorCode = 429. Bạn cần tạm ngừng gọi API tới URL này cho tới khi quá thời gian `unlockedAt`. Nếu vẫn tiếp tục phát sinh gọi API khi đang bị Rate Limit, lockedSeconds và unlockedAt sẽ bị tăng lên.

**Ví dụ response khi vượt Rate Limit:**
```json
{
  "code": 0,
  "errorCode": "ERR_429",
  "message": "Your app exceeded the API Rate Limit",
  "data": {
    "lockedSeconds": 10,
    "unlockedAt": 1733387520
  }
}
```

| Key | Description |
|-----|-------------|
| lockedSeconds | Số giây bị khóa |
| unlockedAt | Thời gian được mở khóa theo Unix timestamp. VD: 1733387520 = 2024-12-05 15:32:00 GMT+07 |

---

## Danh sách API Endpoints

### Business APIs

- Danh sách kho hàng: `/v3/business/depot`
- Danh sách nhân viên: `/v3/business/user`
- Danh sách phòng ban: `/v3/business/department`
- Danh sách nhà cung cấp: `/v3/business/supplier`
- Thêm nhà cung cấp: `/v3/business/addsupplier`

### Product APIs

- Danh sách sản phẩm: `/v3/product/list`
- Thêm sản phẩm: `/v3/product/add`
- Chi tiết sản phẩm: `/v3/product/detail`
- Danh sách tồn kho: `/v3/product/inventory`
- Danh mục sản phẩm: `/v3/product/category`
- Danh mục nội bộ: `/v3/product/internalcategory`
- Danh sách thương hiệu: `/v3/product/brand`
- Danh sách đơn vị tính: `/v3/product/unit`
- Danh sách lô hàng: `/v3/product/batch`
- Hạn sử dụng sản phẩm: `/v3/product/expire`
- Quà tặng sản phẩm: `/v3/product/gift`
- Danh sách IMEI: `/v3/product/imei`
- Lịch sử IMEI: `/v3/product/imeihistory`
- Tra cứu IMEI bán ra theo ngày: `/v3/product/imeisold`
- Thêm ảnh sản phẩm: `/v3/product/externalimage`
- Danh sách nhãn sản phẩm: `/v3/product/tags`

### Order APIs

- Danh sách đơn hàng: `/v3/order/list`
- Thêm đơn hàng: `/v3/order/add`
- Sửa đơn hàng: `/v3/order/edit`
- Hủy đơn hàng: `/v3/order/cancelcarrier`
- Thêm khiếu nại đơn hàng: `/v3/order/addcomplain`
- Nguồn đơn hàng: `/v3/order/source`
- Lịch sử đơn hàng: `/v3/order/history`
- Danh sách nhãn đơn hàng: `/v3/order/tags`

### Shipping APIs

- Danh sách địa chỉ: `/v3/shipping/location`
- Tính phí vận chuyển: `/v3/shipping/fee`
- Danh sách hãng vận chuyển: `/v3/shipping/carrier`

### Bill APIs

- Danh sách hóa đơn: `/v3/bill/list`
- Danh sách hóa đơn bán lẻ: `/v3/bill/retail`
- Thêm hóa đơn bán lẻ: `/v3/bill/addretail`
- Sản phẩm xuất nhập kho: `/v3/bill/imexs`
- Danh sách xuất nhập kho nháp: `/v3/bill/draftlist`
- Sản phẩm xuất nhập kho nháp: `/v3/bill/draftproducts`

### Inventory APIs

- Thêm phiếu nhập nhà cung cấp: `/v3/inventory/importsupplier`
- Thêm phiếu trả nhà cung cấp: `/v3/inventory/exportsupplier`
- Thêm phiếu nhập khác: `/v3/inventory/importother`
- Thêm phiếu trả khác: `/v3/inventory/exportother`

### Customer APIs

- Danh sách khách hàng: `/v3/customer/list`
- Thêm khách hàng: `/v3/customer/add`
- Danh sách nhãn khách hàng: `/v3/customer/tags`

### Promotion APIs

- Danh sách chương trình coupon: `/v3/promotion/batch`
- Thêm chương trình coupon: `/v3/promotion/batchadd`
- Thêm mã coupon vào chương trình: `/v3/promotion/batchincreasecode`
- Danh sách mã coupon: `/v3/promotion/couponcode`
- Danh sách sản phẩm được áp dụng mã coupon: `/v3/promotion/couponproduct`
- Cập nhật trạng thái sử dụng coupon: `/v3/promotion/couponupdate`

### Accounting APIs

- Bút toán: `/v3/accounting/transaction`
- Công nợ khách hàng: `/v3/accounting/debts`
- Danh sách tài khoản kế toán: `/v3/accounting/account`

### Zalo APIs

- Gửi tin Zalo ZNS: `/v3/zalo/zns`

### Ecom APIs

- Danh sách gian hàng trên sàn: `/v3/ecom/shop`

### Vpage APIs

- Danh sách page: `/v3/vpage/page_list`
- Danh sách user: `/v3/vpage/user_list`
- Conversation: `/v3/vpage/conversation`
- Marketing: `/v3/vpage/marketing`
- Iframe khung tạo đơn: `/v3/vpage/order_iframe`

### Webhooks

- Cài đặt: `/v3/webhooks/webhooks`
- App: `/v3/webhooks/app`
- Đơn hàng: `/v3/webhooks/order`
- Sản phẩm: `/v3/webhooks/product`
- Tồn kho: `/v3/webhooks/inventory`

---

## Tài liệu tham khảo

- **Trang chủ tài liệu:** https://apidocs.nhanh.vn/v3
- **Khởi tạo ứng dụng:** https://apidocs.nhanh.vn/app
- **Postman Collection:** https://www.postman.com/nhanh-vn/pos-open-nhanh-vn
- **Change log:** https://apidocs.nhanh.vn/v3/changelog
- **Model Constant:** https://apidocs.nhanh.vn/v3/modelconstant

---

**Lưu ý:** Tài liệu này được thu thập tự động từ trang tài liệu chính thức của Nhanh.vn. Để có thông tin chi tiết nhất, vui lòng tham khảo trực tiếp tại https://apidocs.nhanh.vn/v3

