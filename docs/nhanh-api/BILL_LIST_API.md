# API: Danh sách hóa đơn

**Nguồn:** https://apidocs.nhanh.vn/v3/bill/list  
**Cập nhật lần cuối:** 2024-12-04

---

## Tổng quan

API này dùng để lấy danh sách phiếu xuất nhập kho: hóa đơn nhập nhà cung cấp, bán lẻ, bán sỉ, chuyển kho, kiểm kho.

Các loại hóa đơn khác bạn có thể xem 2 request params là `type` và `mode` ở bên dưới để làm tương tự.

### Chú ý quan trọng

- Hệ thống chỉ hỗ trợ lấy hóa đơn trong khoảng **31 ngày**.
- Nếu bạn không truyền param `createdAtFrom` mặc định sẽ lấy 31 ngày gần nhất. Nếu bạn muốn lấy dữ liệu các ngày cũ hơn, xin vui lòng truyền `createdAtFrom` và `createdAtTo` trong khoảng 31 ngày, VD: `2025-08-01` => `2025-08-31`.
- Khi lọc theo `id`, `customerId` hoặc `customerMobile` thì có thể bỏ qua khoảng ngày.

---

## Request

**Endpoint:** `https://pos.open.nhanh.vn/v3.0/bill/list`

**Method:** POST

**Headers:**
- `Authorization`: Access token
- `Content-Type`: application/json

**Query Parameters:**
- `appId`: ID app của bạn (required)
- `businessId`: ID doanh nghiệp trên Nhanh.vn (required)

**Body:**
```json
{
    "filters": {
        "modes": [2],
        "type": 2
    },
    "paginator": {
        "size": 50,
        "next": {"id": 100}
    },
    "dataOptions": {}
}
```

### Ví dụ cURL

```bash
curl --location 'https://pos.open.nhanh.vn/v3.0/bill/list?appId={{appId}}&businessId={{businessId}}' \
--header 'Authorization: {{accessToken}}' \
--header 'Content-Type: application/json' \
--data '{
    "filters": {
        "modes": [2],
        "type": 2
    },
     "paginator": {
        "size": 50,
        "next": {"id": 100}
    },
    "dataOptions": {}
}'
```

---

## Filters

Bảng các tham số lọc có thể sử dụng:

| Key | Type | Description |
|-----|------|-------------|
| `id` | int | ID phiếu xuất nhập kho |
| `type` | int | Loại xuất nhập kho: `1` = Nhập kho, `2` = Xuất kho |
| `modes` | array | Kiểu xuất nhập kho: <br>- `5` = Nhà cung cấp<br>- `3` = Chuyển kho<br>- `1` = Giao hàng<br>- `2` = Bán lẻ<br>- `6` = Bán buôn<br>- `8` = Bù trừ kiểm kho<br>- `13` = Bảo hành<br>- `15` = Sửa chữa<br>- `16` = Linh kiện bảo hành<br>- `19` = Combo<br>- `10` = Khác |
| `depotIds` | array | Mảng ID kho hàng |
| `orderIds` | array | Mảng ID đơn hàng (chỉ hỗ trợ lọc tối đa 100 ID đơn hàng) |
| `customerId` | int | ID khách hàng |
| `customerMobile` | string | Số điện thoại khách hàng |
| `fromDate` | date | Ngày xuất nhập kho. Format `yyyy-mm-dd` (ví dụ: `2015-07-16`) |
| `toDate` | date | Ngày xuất nhập kho. Format `yyyy-mm-dd` (ví dụ: `2015-08-16`) |

---

## Paginator

Xem cấu trúc chung tại: https://apidocs.nhanh.vn/v3#paginator

Các field chính:
- `size` (int): Số lượng bản ghi trên 1 trang
- `next` (object): Giá trị phân trang cho trang tiếp theo, VD: `{"id": 100}`

---

## DataOptions

Dữ liệu cần lấy thêm:

| Key | Type | Description |
|-----|------|-------------|
| `giftProducts` | int | Lấy thông tin quà tặng của sản phẩm trong xuất nhập kho |
| `tags` | int | Lấy nhãn được gắn cho xuất nhập kho |

---

## Response

Xem cấu trúc chung tại: https://apidocs.nhanh.vn/v3#response

### Failed Response

Xem các mã lỗi chung tại: https://apidocs.nhanh.vn/v3#failed-response

### Successful Response

Response sẽ trả về danh sách các hóa đơn với cấu trúc như sau:

```json
{
  "code": 1,
  "paginator": {
    "next": "Dùng để lấy dữ liệu trang tiếp theo"
  },
  "data": [
    {
      "id": "(int) ID phiếu XNK",
      "orderId": "(int) ID đơn hàng liên quan",
      "requirementBillId": "(int) ID phiếu nháp liên quan",
      "inventoryCheckId": "(int) ID phiếu kiểm kho liên quan",
      "warrantyBillId": "(int) ID phiếu bảo hành liên quan",
      "depotId": "(int) ID Kho hàng",
      "date": "(date) Ngày xuất nhập kho format Y-m-d",
      "type": "(int) Loại xuất nhập kho",
      "mode": "(int) Kiểu xuất nhập kho",
      "related": {
        "type": "(int) loại phiếu liên quan (VD: chuyển kho, quà tặng, xuất combo, trả lại bán lẻ, bán buôn...)",
        "parentId": "(int) ID phiếu trả hàng liên quan (dùng cho trường hợp đổi trả hàng và mua mới, hóa đơn mua mới sẽ có parentId = hóa đơn trả)",
        "billId": "(int) ID phiếu liên quan (VD: chuyển kho)",
        "businessId": "(int) ID doanh nghiệp của phiếu liên quan",
        "depotId": "(int) ID kho hàng của phiếu liên quan",
        "requirementBillId": "(int) ID phiếu nháp liên quan",
        "userId": "(int) ID user liên quan",
        "userName": "(string) Tên user liên quan"
      },
      "customer": {
        "id": "(int) ID khách hàng",
        "name": "(string) Tên khách hàng",
        "mobile": "(string) Số điện thoại khách hàng",
        "address": "(string) Địa chỉ khách hàng"
      },
      "created": {
        "id": "(int) ID người tạo XNK",
        "name": "(string) Tên người tạo XNK",
        "createdAt": "(Timestamp) Thời gian tạo XNK"
      },
      "sale": {
        "id": "(int) ID nhân viên bán hàng",
        "name": "(string) Tên nhân viên bán hàng",
        "saleBonus": "(double) Hoa hồng cho nhân viên bán hàng"
      },
      "technicalStaff": {
        "id": "(int) ID nhân viên kỹ thuật",
        "name": "(string) Tên nhân viên kỹ thuật"
      },
      "supplier": {
        "id": "(int) ID nhà cung cấp",
        "name": "(string) Tên nhà cung cấp"
      },
      "products": [
        {
          "imexId": "(int) ID sản phẩm XNK",
          "id": "(int) ID sản phẩm trên Nhanh",
          "code": "(string) Mã code sản phẩm trên Nhanh",
          "barcode": "(string) Mã vạch sản phẩm trên Nhanh",
          "name": "(string) Tên sản phẩm trên Nhanh",
          "imeiId": "(int) ID IMEI trên Nhanh",
          "imei": "(array) List IMEI sản phẩm trên Nhanh nếu là sản phẩm IMEI",
          "quantity": "(double) Số lượng sản phẩm XNK",
          "price": "(double) Giá bán sản phẩm XNK",
          "discount": "(double) Số tiền giảm giá",
          "extendedWarrantyAmount": "(double) Số tiền bảo hành mở rộng",
          "vat": {
            "percent": "(int) % vat",
            "amount": "(double) số tiền vat"
          },
          "amount": "(double) Thành tiền sản phẩm",
          "giftProducts": "(array) List sản phẩm quà tặng"
        }
      ],
      "payment": {
        "amount": "(double) Tổng số tiền hóa đơn",
        "customerAmount": "(double) Tiền khách đưa",
        "discount": "(double) Tổng tiền chiết khấu",
        "points": "(double) Số điểm được tích lũy",
        "usedPoints": {
          "points": "(double) Số điểm đã sử dụng",
          "amount": "(double) Số tiền quy đổi từ điểm sử dụng"
        },
        "transfer": {
          "accountId": "(int) ID tài khoản thanh toán chuyển khoản",
          "name": "(string) Tên tài khoản thanh toán chuyển khoản",
          "amount": "(double) Số tiền thanh toán chuyển khoản"
        },
        "cash": {
          "accountId": "(int) ID tài khoản thanh toán tiền mặt",
          "name": "(string) Tên tài khoản thanh toán tiền mặt",
          "amount": "(double) Số tiền thanh toán tiền mặt"
        },
        "installment": {
          "accountId": "(int) ID dịch vụ trả góp",
          "name": "(string) Tên dịch vụ trả góp",
          "amount": "(double) Số tiền trả góp"
        },
        "credit": {
          "accountId": "(int) ID tài khoản quẹt thẻ",
          "name": "(string) Tên tài khoản quẹt thẻ",
          "code": "(string) Mã giao dịch quẹt thẻ",
          "amount": "(double) Số tiền quẹt thẻ"
        },
        "coupon": {
          "code": "(string) Mã coupon",
          "value": "(double) Giá trị coupon",
          "amount": "(double) Giá trị coupon áp dụng cho XNK"
        },
        "returnFee": "(double) Phí trả hàng",
        "returnAmount": "(double) Tổng tiền trả hàng"
      },
      "description": "(string) Nội dung ghi chú cho XNK",
      "tags": "(array) List các nhãn gắn với XNK"
    }
  ]
}
```

### Model Constants

Để hiểu rõ hơn về các giá trị của các field, bạn có thể tham khảo:

- **Loại xuất nhập kho:** https://apidocs.nhanh.vn/v3/modelconstant#inventory-type
- **Kiểu xuất nhập kho:** https://apidocs.nhanh.vn/v3/modelconstant#inventory-mode
- **Loại xuất nhập kho liên quan:** https://apidocs.nhanh.vn/v3/modelconstant#inventory-related-type

---

## Ví dụ sử dụng

### Ví dụ 1: Lấy hóa đơn bán lẻ trong 31 ngày gần nhất

```json
{
    "filters": {
        "modes": [2],
        "type": 2
    },
    "paginator": {
        "size": 50
    },
    "dataOptions": {
        "tags": 1,
        "giftProducts": 1
    }
}
```

### Ví dụ 2: Lấy hóa đơn theo ID khách hàng

```json
{
    "filters": {
        "customerId": 12345,
        "type": 2
    },
    "paginator": {
        "size": 50
    }
}
```

### Ví dụ 3: Lấy hóa đơn trong khoảng thời gian cụ thể (tối đa 31 ngày)

```json
{
    "filters": {
        "fromDate": "2025-08-01",
        "toDate": "2025-08-31",
        "type": 2,
        "modes": [2, 6]
    },
    "paginator": {
        "size": 50
    }
}
```

---

## Tham khảo

- **Trang tài liệu chính thức:** https://apidocs.nhanh.vn/v3/bill/list
- **Tài liệu API v3.0 tổng quan:** [../NHANH_API_V3_DOCUMENTATION.md](../NHANH_API_V3_DOCUMENTATION.md)

