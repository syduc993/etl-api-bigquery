1	MINH ĐƯỢC GETDATA
1.1	Hồ sơ nhân sự
Bao gồm 2 API:
Danh sách hồ sơ nhân sự. GET: https://minham.1office.vn/api/personnel/profile/gets
Chi tiết hồ sơ nhân sự. GET: https://minham.1office.vn/api/personnel/profile/item
1.1.1	Danh sách hồ sơ nhân sự
Method: GET
Url: https://minham.1office.vn/api/personnel/profile/gets
Access Token: 80391498468e8ae4c02f02819998254
Params:
Field	Type	Required	Description
access_token*	string	true	Mã bảo mật
limit	integer	false	Số bản ghi trên trang (mặc định 50, tối đa 100)
page	integer	false	Số trang (mặc định 1)
sort_by	string	false	Tên trường cần sắp xếp
sort_type	string	false	Thứ tự sắp xếp của tên trường cần sắp xếp, giá trị là 'asc' hoặc 'desc'. trong đó 'asc' là sắp xếp tăng dần. 'desc' là sắp xếp giảm dần
filters	json	false	Giá trị cần lọc
Là JSON ENDCODE của mảng dữ liệu. Ví dụ: [{"key1":"value1", "key2":"value2"},{"key1":"value1", "key2":"value2"},... ]
Cấu trúc của field filters
s	string	false	Từ khóa tìm kiếm
ID	integer	false	ID Hồ sơ nhân sự
department_id	string	false	Phòng ban
Phòng ban cha con được nối với nhau bởi dấu || . Ví dụ 'Phòng kinh doanh || Nhóm 1' hoặc 'Cửa hàng A || Phòng điều hành || Ban giám đốc',....
department_business	string	false	Khối nghiệp vụ
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị3'Khối'
concurrent_position_ids	string comma	false	Vị trí kiêm nhiệm
Là 1 chuỗi giá trị phân cách bởi dấu phẩy (Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID). Ví dụ '1,2,3' hoặc 'a,b,c '...IDGiá trị215'Bussiness Analist'140'Chăm sóc khách hàng B2B'108'Chuyên viên C B'227'Chuyên viên Content Marketing'229'Chuyên viên Đào Tạo'190'Chuyên viên Đào tạo nội bộ   Quản lý hiệu suất'88'Chuyên viên Giám sát Cửa hàng'200'Chuyên viên Sáng tạo nội dung và Truyền thông'226'Chuyên viên Thiết kế Đồ hoạ'224'Chuyên viên Thiết kế Nội thất và Giám sát'228'Chuyên viên Tổ chức sự kiện'209'Chuyên viên Trade Marketing'196'Chuyên viên Truyền thông'223'Chuyên viên Tuyển dụng'219'Giám đốc Kinh doanh vải Ninh Hiệp'161'Giám đốc sàn TMĐT Tiktok'166'Giám đốc Sáng tạo phát triển sản phẩm'34'Giám đốc Vận hành'143'Giám sát  vận hành'145'Giám sát Camera'144'Giám sát cửa hàng'236'Giám sát Kinh doanh'238'Giám sát nội bộ'115'Giám sát nội bộ kiêm Trưởng nhóm Chăm sóc khách hàng Online'232'Graphic Design'153'Kế toán Công nợ   Giá thành'221'Kế toán Kho vải Ninh Hiệp'154'Kế toán Tài sản cố định, công cụ dụng cụ (TSCĐ, CCDC)'80'Kế toán Thanh toán'151'Kế toán Thanh toán   Hàng hoá'148'Kế toán Thuế'81'Kế toán Tổng hợp   Doanh thu'142'Kiểm toán'163'Lái xe'107'Lao công'168'Nghiên cứu sản phẩm (R D)'176'Nhân viên Affilate'83'Nhân viên bán buôn vải'85'Nhân viên Bán hàng'97'Nhân viên Bốc xếp Hàng hóa'179'Nhân viên Chăm sóc khách hàng TIiktok'175'Nhân viên chạy ADS'245'Nhân viên Chuyển đổi số'113'Nhân viên Data Analylist'201'Nhân viên Digital Marketing'183'Nhân viên Đồng bộ NPL'211'Nhân viên giám sát cửa hàng kiêm quản lý POSM cửa hàng'164'Nhân viên Hành chính - Nhân sự TMĐT Tiktok'24'Nhân viên Hành chính Kỹ Thuật'141'Nhân viên Hành chính Tổng hợp'76'Nhân viên Hình ảnh'146'Nhân viên IT'178'Nhân viên kế hoạch   điều phối hàng hoá'170'Nhân viên Kế hoạch sản xuất'43'Nhân viên Kế toán'36'Nhân viên Kế toán Kho'160'Nhân viên Kho Bán buôn quần áo'54'Nhân viên Kho Nguyên phụ liệu'126'Nhân viên Kho Shopee'121'Nhân viên Kho Tiktok'205'Nhân viên Kho Tổng cửa hàng'119'Nhân viên kho vải'199'Nhân viên kho vải Ninh Hiệp'137'Nhân viên kiểm kê'187'Nhân viên Kiểm soát chất lượng (QC)'194'Nhân viên Kinh doanh vải Ninh Hiệp'208'Nhân viên kinh doanh vải Tam Hiệp'186'Nhân viên Kỹ thuật'210'Nhân viên kỹ thuật chuyền may'118'Nhân viên kỹ thuật may'159'Nhân viên livesteam Full time'157'Nhân viên livesteam Part time'42'Nhân viên Marketing nội bộ'48'Nhân viên May mẫu'173'Nhân viên Media'182'Nhân viên mua hàng quốc tế'181'Nhân viên mua hàng trong nước'188'Nhân viên Mua sắm CCDC, TTB   sửa chữa CSVC'192'Nhân viên Pháp chế   xử lý quan hệ lao động'195'Nhân viên Phát triển thị trường'116'Nhân viên PR Marketing'47'Nhân viên Ra rập'207'Nhân viên Sale admin MT'40'Nhân viên sàn Thương mại điện tử'198'Nhân viên Stylist'222'Nhân viên Stylist kiêm Visual Merchandise'72'Nhân viên Tài liệu Kỹ Thuật'130'Nhân viên Tạp vụ'167'Nhân viên tạp vụ TMĐT tiktok'37'Nhân viên Telesale Fulltime'96'Nhân viên Theo dõi đơn hàng'29'Nhân viên Thiết kế Đồ họa'171'Nhân viên thiết kế thời trang'91'Nhân viên Thu ngân'191'Nhân viên thu ngân vải Ninh Hiệp'86'Nhân viên Tiếp đón'106'Nhân viên Tiktok'241'Nhân viên Tổ Cắt'135'Nhân viên Triển khai dự án kiêm Nhân viên KD vải Tam Hiệp'98'Nhân viên Tuyển dụng'217'Nhân viên Video Editer'65'Nhân viên VM'139'Nhân viên Xử lý đơn hàng online'169'Phát triển sản phẩm   nhà cung cấp'129'Phó Kho'197'Phó kho vải Ninh Hiệp'44'Phó Tổng Giám Đốc'230'Photographer'231'Photographer   Video Editor'225'PR   Marketing Intern'95'PTHT Nhà phân phối'112'QC xưởng'180'Quản lý Chăm sóc khách hàng Tiktok'55'Quản lý Cửa hàng'133'Quản lý cửa hàng kiêm Trợ lý đào tạo'184'Quản lý cửa hàng vải Ninh Hiệp'234'Quản lý Đơn hàng'104'Quản lý Kho'124'Quản lý kho Shopee'122'Quản lý Kho Tổng Cửa hàng'125'Quản lý kho vải   Nguyên phụ liệu'204'Quản lý Khu vực'155'Quản lý livestream'172'Quản lý Media'60'Quản lý Sản xuất'39'Quản lý Telesale'117'Quản lý tổ may'101'RSM'100'Sale Admin'138'Sale Online   Chăm sóc khách hàng'94'Sale Online   chăm sóc khách hàng (Part time)'246'Thiết kế nội thất'150'Thợ cắt'147'Thợ hoàn thiện'149'Thợ may'152'Thợ trải vải'134'Thủ kho Nguyên phụ liệu'111'Thực tập sinh Hành chính Nhân sự'243'Thực tập sinh Marketing'45'Tổng Giám đốc'239'Trade Marketing'213'Trợ lý Ban Giám đốc kiêm Phát triển sản phẩm'110'Trợ lý Ban Giám đốc kiêm Trưởng nhóm giám sát nội bộ'203'Trợ lý đào tạo cửa hàng'131'Trợ lý Marketing'177'Trợ lý phòng kế hoạch sản xuất'105'Trợ lý phòng Sản xuất'237'Trưng bày sản phẩm (Visual Merchandiser)'156'Trưởng bộ phận Kho bán hàng'114'Trưởng nhóm Đào tạo cửa hàng'235'Trưởng nhóm Hành chính'174'Trưởng nhóm Kế hoạch sản xuất'128'Trưởng nhóm Kho'162'Trưởng nhóm Kho Shopee'123'Trưởng nhóm kho Tiktok'158'Trưởng nhóm Kho Tổng cửa hàng'136'Trưởng nhóm Kiểm kê'185'Trưởng nhóm Kỹ thuật sản xuất'78'Trưởng nhóm Sàn Thương Mại Điện Tử'59'Trưởng nhóm Telesale'242'Trưởng nhóm Thương mại điện tử'189'Trưởng nhóm Tuyển dụng   Phát triển nguồn nhân lực'79'Trưởng nhóm Vận hành kiêm chuyên viên đào tạo Cửa hàng'244'Trưởng phòng Đào tạo'66'Trưởng phòng Hành chính Nhân sự'240'Trưởng phòng Kho vận'33'Trưởng phòng Kinh doanh'202'Trưởng phòng Kinh doanh bán lẻ'206'Trưởng phòng Kinh doanh online'102'Trưởng phòng Marketing'193'Trưởng phòng Phát triển thị trường'165'Trưởng phòng Sản phẩm'46'Trưởng phòng Tài chính Kế toán'233'Visual Merchandiser'
position_id	tree	false	Vị trí
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịIDGiá trị215'Bussiness Analist'140'Chăm sóc khách hàng B2B'108'Chuyên viên C B'227'Chuyên viên Content Marketing'229'Chuyên viên Đào Tạo'190'Chuyên viên Đào tạo nội bộ   Quản lý hiệu suất'88'Chuyên viên Giám sát Cửa hàng'200'Chuyên viên Sáng tạo nội dung và Truyền thông'226'Chuyên viên Thiết kế Đồ hoạ'224'Chuyên viên Thiết kế Nội thất và Giám sát'228'Chuyên viên Tổ chức sự kiện'209'Chuyên viên Trade Marketing'196'Chuyên viên Truyền thông'223'Chuyên viên Tuyển dụng'219'Giám đốc Kinh doanh vải Ninh Hiệp'161'Giám đốc sàn TMĐT Tiktok'166'Giám đốc Sáng tạo phát triển sản phẩm'34'Giám đốc Vận hành'143'Giám sát  vận hành'145'Giám sát Camera'144'Giám sát cửa hàng'236'Giám sát Kinh doanh'238'Giám sát nội bộ'115'Giám sát nội bộ kiêm Trưởng nhóm Chăm sóc khách hàng Online'232'Graphic Design'153'Kế toán Công nợ   Giá thành'221'Kế toán Kho vải Ninh Hiệp'154'Kế toán Tài sản cố định, công cụ dụng cụ (TSCĐ, CCDC)'80'Kế toán Thanh toán'151'Kế toán Thanh toán   Hàng hoá'148'Kế toán Thuế'81'Kế toán Tổng hợp   Doanh thu'142'Kiểm toán'163'Lái xe'107'Lao công'168'Nghiên cứu sản phẩm (R D)'176'Nhân viên Affilate'83'Nhân viên bán buôn vải'85'Nhân viên Bán hàng'97'Nhân viên Bốc xếp Hàng hóa'179'Nhân viên Chăm sóc khách hàng TIiktok'175'Nhân viên chạy ADS'245'Nhân viên Chuyển đổi số'113'Nhân viên Data Analylist'201'Nhân viên Digital Marketing'183'Nhân viên Đồng bộ NPL'211'Nhân viên giám sát cửa hàng kiêm quản lý POSM cửa hàng'164'Nhân viên Hành chính - Nhân sự TMĐT Tiktok'24'Nhân viên Hành chính Kỹ Thuật'141'Nhân viên Hành chính Tổng hợp'76'Nhân viên Hình ảnh'146'Nhân viên IT'178'Nhân viên kế hoạch   điều phối hàng hoá'170'Nhân viên Kế hoạch sản xuất'43'Nhân viên Kế toán'36'Nhân viên Kế toán Kho'160'Nhân viên Kho Bán buôn quần áo'54'Nhân viên Kho Nguyên phụ liệu'126'Nhân viên Kho Shopee'121'Nhân viên Kho Tiktok'205'Nhân viên Kho Tổng cửa hàng'119'Nhân viên kho vải'199'Nhân viên kho vải Ninh Hiệp'137'Nhân viên kiểm kê'187'Nhân viên Kiểm soát chất lượng (QC)'194'Nhân viên Kinh doanh vải Ninh Hiệp'208'Nhân viên kinh doanh vải Tam Hiệp'186'Nhân viên Kỹ thuật'210'Nhân viên kỹ thuật chuyền may'118'Nhân viên kỹ thuật may'159'Nhân viên livesteam Full time'157'Nhân viên livesteam Part time'42'Nhân viên Marketing nội bộ'48'Nhân viên May mẫu'173'Nhân viên Media'182'Nhân viên mua hàng quốc tế'181'Nhân viên mua hàng trong nước'188'Nhân viên Mua sắm CCDC, TTB   sửa chữa CSVC'192'Nhân viên Pháp chế   xử lý quan hệ lao động'195'Nhân viên Phát triển thị trường'116'Nhân viên PR Marketing'47'Nhân viên Ra rập'207'Nhân viên Sale admin MT'40'Nhân viên sàn Thương mại điện tử'198'Nhân viên Stylist'222'Nhân viên Stylist kiêm Visual Merchandise'72'Nhân viên Tài liệu Kỹ Thuật'130'Nhân viên Tạp vụ'167'Nhân viên tạp vụ TMĐT tiktok'37'Nhân viên Telesale Fulltime'96'Nhân viên Theo dõi đơn hàng'29'Nhân viên Thiết kế Đồ họa'171'Nhân viên thiết kế thời trang'91'Nhân viên Thu ngân'191'Nhân viên thu ngân vải Ninh Hiệp'86'Nhân viên Tiếp đón'106'Nhân viên Tiktok'241'Nhân viên Tổ Cắt'135'Nhân viên Triển khai dự án kiêm Nhân viên KD vải Tam Hiệp'98'Nhân viên Tuyển dụng'217'Nhân viên Video Editer'65'Nhân viên VM'139'Nhân viên Xử lý đơn hàng online'169'Phát triển sản phẩm   nhà cung cấp'129'Phó Kho'197'Phó kho vải Ninh Hiệp'44'Phó Tổng Giám Đốc'230'Photographer'231'Photographer   Video Editor'225'PR   Marketing Intern'95'PTHT Nhà phân phối'112'QC xưởng'180'Quản lý Chăm sóc khách hàng Tiktok'55'Quản lý Cửa hàng'133'Quản lý cửa hàng kiêm Trợ lý đào tạo'184'Quản lý cửa hàng vải Ninh Hiệp'234'Quản lý Đơn hàng'104'Quản lý Kho'124'Quản lý kho Shopee'122'Quản lý Kho Tổng Cửa hàng'125'Quản lý kho vải   Nguyên phụ liệu'204'Quản lý Khu vực'155'Quản lý livestream'172'Quản lý Media'60'Quản lý Sản xuất'39'Quản lý Telesale'117'Quản lý tổ may'101'RSM'100'Sale Admin'138'Sale Online   Chăm sóc khách hàng'94'Sale Online   chăm sóc khách hàng (Part time)'246'Thiết kế nội thất'150'Thợ cắt'147'Thợ hoàn thiện'149'Thợ may'152'Thợ trải vải'134'Thủ kho Nguyên phụ liệu'111'Thực tập sinh Hành chính Nhân sự'243'Thực tập sinh Marketing'45'Tổng Giám đốc'239'Trade Marketing'213'Trợ lý Ban Giám đốc kiêm Phát triển sản phẩm'110'Trợ lý Ban Giám đốc kiêm Trưởng nhóm giám sát nội bộ'203'Trợ lý đào tạo cửa hàng'131'Trợ lý Marketing'177'Trợ lý phòng kế hoạch sản xuất'105'Trợ lý phòng Sản xuất'237'Trưng bày sản phẩm (Visual Merchandiser)'156'Trưởng bộ phận Kho bán hàng'114'Trưởng nhóm Đào tạo cửa hàng'235'Trưởng nhóm Hành chính'174'Trưởng nhóm Kế hoạch sản xuất'128'Trưởng nhóm Kho'162'Trưởng nhóm Kho Shopee'123'Trưởng nhóm kho Tiktok'158'Trưởng nhóm Kho Tổng cửa hàng'136'Trưởng nhóm Kiểm kê'185'Trưởng nhóm Kỹ thuật sản xuất'78'Trưởng nhóm Sàn Thương Mại Điện Tử'59'Trưởng nhóm Telesale'242'Trưởng nhóm Thương mại điện tử'189'Trưởng nhóm Tuyển dụng   Phát triển nguồn nhân lực'79'Trưởng nhóm Vận hành kiêm chuyên viên đào tạo Cửa hàng'244'Trưởng phòng Đào tạo'66'Trưởng phòng Hành chính Nhân sự'240'Trưởng phòng Kho vận'33'Trưởng phòng Kinh doanh'202'Trưởng phòng Kinh doanh bán lẻ'206'Trưởng phòng Kinh doanh online'102'Trưởng phòng Marketing'193'Trưởng phòng Phát triển thị trường'165'Trưởng phòng Sản phẩm'46'Trưởng phòng Tài chính Kế toán'233'Visual Merchandiser'
job_title	string	false	Chức vụ
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịIDGiá trị54'Chuyên viên'38'Đôn đơn'49'Giám đốc Vận hành'62'Giám sát nội bộ kiêm Trưởng nhóm Chăm sóc khách hàng Online'48'Nhân viên'58'Nhân viên Thời vụ'64'Nhân viên Tổ may'50'Phó tổng Giám đốc'61'QC Xưởng'37'Quản lý'68'Quản lý Kho Shopee'67'Quản lý Kho Tiktok'66'Quản lý Kho Tổng Cửa hàng'65'Quản lý kho Vải và Nguyên Phụ liệu'63'Quản lý Tổ may'59'Sale Admin'60'Thực tập sinh'69'Thực tập sinh'36'Tổng Giám đốc'14'Trưởng nhóm'13'Trưởng phòng'
job_date_join*	date	true	Ngày vào
Định dạng dd/mm/YYYY
year_work_experience_now	integer	false	Số năm kinh nghiệm hiện tại
date_join_now	date	false	Ngày vào công ty hàng năm
Định dạng dd/mm/YYYY
year_join	integer	false	Thâm niên
birthday_current	integer	false	Số tuổi
job_reldate_join	date	false	Ngày ký HĐLĐ chính thức
Định dạng dd/mm/YYYY
date_trigger_user	date	false	Ngày tạo TK 1Office
Định dạng dd/mm/YYYY
job_status	string	false	Trạng thái
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịWAITTING'Chờ nhận việc'WORKING'Đang làm việc'Mặc địnhTEMPORARY'Nghỉ tạm thời'LEAVING'Nghỉ thai sản'LEAVE_UNPAID'Nghỉ không lương dài hạn'LEAVE_ARMY'Nghỉ đi nghĩa vụ quân sự'LEAVE_STUDY'Nghỉ đi học'LEAVE_SICK'Nghỉ ốm đau'STOP_WORKING'Nghỉ việc'
job_date_out	date	false	Ngày nghỉ việc
Định dạng dd/mm/YYYY
job_out_reason	string	false	Lý do nghỉ
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị9'Ca làm việc buổi tối muộn'8'Cá nhân'3'Chính sách công ty'2'Cơ chế lương'6'Công việc không đủ đáp ứng thời gian'5'Công việc quá tải'4'Gặp vấn đề với Quản lý / Ban điều hành'7'Không còn cơ hội phát triển'1'Văn hoá công ty'
birthday*	date	true	Ngày sinh
Định dạng dd/mm/YYYY
birthday_now	date	false	Ngày sinh nhật hiện tại
Định dạng dd/mm/YYYY
gender*	string	true	Giới tính
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị0'Nữ'1'Nam'2'Khác'
marital_status	string	false	Tình trạng hôn nhân
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị4'Độc thân'5'Kết hôn'6'Ly hôn'
military_service	string	false	Nghĩa vụ quân sự
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị0'Đã đi'1'Chưa đi'2'Không phải đi'Mặc định
people	string	false	Dân tộc
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịIDGiá trị1'Kinh'7'Ba Na'8'Bố Y'9'Brâu'10'Bru - Vân Kiều'63'Ca dong'13'Chăm'11'Chơ Ro'19'Chu ru'12'Chứt'14'Co'16'Cơ Ho'17'Cơ Lao'18'Cơ Tu'15'Cống'20'Dao'21'Ê Đê'22'Gia Rai'23'Giáy'24'Gié - Triêng'26'Hà Nhì'25'H'Mông'60'Hoa'27'Hrê'28'Kháng'29'Khơ me'30'Khơ Mú'32'La Chí'33'La Ha'34'La Hủ'35'Lào'36'Lô Lô'37'Lự'39'Mạ'40'Mảng'38'M'Nông'41'Mường'42'Ngái'43'Nùng'44'Ơ Đu'45'Pà Thẻn'46'Phù Lá'47'Pu Péo'48'Ra Glai'49'Rơ Măm'50'Sán Chay'51'Sán Dìu'52'Si La'53'Tà Ôi'54'Tày'55'Thái'56'Thổ'57'Xinh Mun'59'Xơ Đăng'58'Xtiêng'61'Pa Cô'62'Hán'
religious	string	false	Tôn giáo
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị6'Không'7'Cơ đốc giáo'8'Hồi giáo'9'Phật giáo'10'Thiên chúa giáo'12'Tin Lành'13'Phật giáo Hòa Hảo'14'Bà La Môn'15'Cao Đài'11'Khác'16'Phật giáo Lương'17'Công giáo La Mã'
private_code_date*	date	true	Ngày cấp CCCD
Định dạng dd/mm/YYYY
passport_date_from	date	false	Ngày cấp hộ chiếu
Định dạng dd/mm/YYYY
passport_date_expire	date	false	Ngày hết hạn
Định dạng dd/mm/YYYY
passport_code_place	string	false	Nơi cấp hộ chiếu
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịIDGiá trị12188'Bộ công an'12175'Cục cảnh sát ĐKQL cư trú và DLQG về dân cư'12177'Cục cảnh sát quản lý hành chính về trật tự xã hội'12198'Cục Quản lý XNC tại Hà Nội'12197'Cục Quản lý XNC tại TP Hồ Chí Minh'12176'Cục quản lý xuất nhập cảnh'11452'Phòng Quản lý XNC - Công an Thành phố Cần Thơ'7243'Phòng Quản lý XNC - Công an Thành phố Đà Nẵng'2'Phòng Quản lý XNC - Công an Thành phố Hà Nội - Cơ sở 1'12201'Phòng Quản lý XNC - Công an Thành phố Hà Nội - Cơ sở 2'4099'Phòng Quản lý XNC - Công an Thành phố Hải Phòng'9818'Phòng Quản lý XNC - Công an Thành phố Hồ Chí Minh'12192'Phòng Quản lý XNC - Công an Thành phố Huế'12196'Phòng Quản lý XNC - Công an Tỉnh An Giang - Cơ sở 1'12199'Phòng Quản lý XNC - Công an Tỉnh An Giang - Cơ sở 2'12200'Phòng Quản lý XNC - Công an Tỉnh An Giang - Cơ sở 3'9728'Phòng Quản lý XNC - Công an Tỉnh Bà Rịa - Vũng Tàu'3007'Phòng Quản lý XNC - Công an Tỉnh Bắc Giang'1037'Phòng Quản lý XNC - Công an Tỉnh Bắc Kạn'11753'Phòng Quản lý XNC - Công an Tỉnh Bạc Liêu'12190'Phòng Quản lý XNC - Công an Tỉnh Bắc Ninh'10558'Phòng Quản lý XNC - Công an Tỉnh Bến Tre'9444'Phòng Quản lý XNC - Công an Tỉnh Bình Dương'7769'Phòng Quản lý XNC - Công an Tỉnh Bình Định'9216'Phòng Quản lý XNC - Công an Tỉnh Bình Phước'8285'Phòng Quản lý XNC - Công an Tỉnh Bình Thuận'11825'Phòng Quản lý XNC - Công an Tỉnh Cà Mau'824'Phòng Quản lý XNC - Công an Tỉnh Cao Bằng'8776'Phòng Quản lý XNC - Công an Tỉnh Đắk Lắk'8976'Phòng Quản lý XNC - Công an Tỉnh Đắk Nông'1491'Phòng Quản lý XNC - Công an Tỉnh Điện Biên'9545'Phòng Quản lý XNC - Công an Tỉnh Đồng Nai'12195'Phòng Quản lý XNC - Công an Tỉnh Đồng Tháp'12193'Phòng Quản lý XNC - Công an Tỉnh Gia Lai'617'Phòng Quản lý XNC - Công an Tỉnh Hà Giang'4804'Phòng Quản lý XNC - Công an Tỉnh Hà Nam'6486'Phòng Quản lý XNC - Công an Tỉnh Hà Tĩnh'3821'Phòng Quản lý XNC - Công an Tỉnh Hải Dương'11547'Phòng Quản lý XNC - Công an Tỉnh Hậu Giang'2156'Phòng Quản lý XNC - Công an Tỉnh Hòa Bình'4337'Phòng Quản lý XNC - Công an Tỉnh Hưng Yên'8062'Phòng Quản lý XNC - Công an Tỉnh Khánh Hòa'11291'Phòng Quản lý XNC - Công an Tỉnh Kiên Giang - Cơ sở 1'12202'Phòng Quản lý XNC - Công an Tỉnh Kiên Giang - Cơ sở 2'12203'Phòng Quản lý XNC - Công an Tỉnh Kiên Giang - Cơ sở 3'8423'Phòng Quản lý XNC - Công an Tỉnh Kon Tum'1632'Phòng Quản lý XNC - Công an Tỉnh Lai Châu'9056'Phòng Quản lý XNC - Công an Tỉnh Lâm Đồng'2568'Phòng Quản lý XNC - Công an Tỉnh Lạng Sơn'12189'Phòng Quản lý XNC - Công an Tỉnh Lào Cai'10165'Phòng Quản lý XNC - Công an Tỉnh Long An'4927'Phòng Quản lý XNC - Công an Tỉnh Nam Định'5984'Phòng Quản lý XNC - Công an Tỉnh Nghệ An'5167'Phòng Quản lý XNC - Công an Tỉnh Ninh Bình'8212'Phòng Quản lý XNC - Công an Tỉnh Ninh Thuận'3248'Phòng Quản lý XNC - Công an Tỉnh Phú Thọ'7940'Phòng Quản lý XNC - Công an Tỉnh Phú Yên'6762'Phòng Quản lý XNC - Công an Tỉnh Quảng Bình'7307'Phòng Quản lý XNC - Công an Tỉnh Quảng Nam - Cơ sở 1'12204'Phòng Quản lý XNC - Công an Tỉnh Quảng Nam - Cơ sở 2'7570'Phòng Quản lý XNC - Công an Tỉnh Quảng Ngãi'2806'Phòng Quản lý XNC - Công an Tỉnh Quảng Ninh - Cơ sở 1'12205'Phòng Quản lý XNC - Công an Tỉnh Quảng Ninh - Cơ sở 2'12191'Phòng Quản lý XNC - Công an Tỉnh Quảng Trị'11632'Phòng Quản lý XNC - Công an Tỉnh Sóc Trăng'1749'Phòng Quản lý XNC - Công an Tỉnh Sơn La'12194'Phòng Quản lý XNC - Công an Tỉnh Tây Ninh'4509'Phòng Quản lý XNC - Công an Tỉnh Thái Bình'2378'Phòng Quản lý XNC - Công an Tỉnh Thái Nguyên'5321'Phòng Quản lý XNC - Công an Tỉnh Thanh Hóa'7081'Phòng Quản lý XNC - Công an Tỉnh Thừa Thiên Huế'10373'Phòng Quản lý XNC - Công an Tỉnh Tiền Giang'10732'Phòng Quản lý XNC - Công an Tỉnh Trà Vinh'1168'Phòng Quản lý XNC - Công an Tỉnh Tuyên Quang'10848'Phòng Quản lý XNC - Công an Tỉnh Vĩnh Long'3539'Phòng Quản lý XNC - Công an Tỉnh Vĩnh Phúc'1966'Phòng Quản lý XNC - Công an Tỉnh Yên Bái'
passport_type	string	false	Loại hộ chiếu
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịDIPLOMATIC'Hộ chiếu ngoại giao'OFFICIAL'Hộ chiếu công vụ'ORDINARY'Hộ chiếu phổ thông'
date_leave_from	date	false	Nghỉ việc từ ngày
Định dạng dd/mm/YYYY
date_leave_to	date	false	Nghỉ việc đến ngày
Định dạng dd/mm/YYYY
due_date	date	false	Ngày dự sinh
Định dạng dd/mm/YYYY
level_academic	string	false	Trình độ học vấn cao nhất
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị1'Sơ cấp'2'Trung cấp'3'Cao đẳng'5'Thạc sĩ'6'Tiến sĩ'7'Khác'8'Tiến sĩ khoa học'10'Kỹ sư'11'Đại học'12'Trung cấp nghề'14'Cao đẳng nghề'15'Trung học phổ thông'16'Trung cấp chuyên nghiệp'17'Sơ cấp nghề'18'Trung học cơ sở'19'Chứng chỉ hành nghề'20'Chuyên Khoa 1'21'Chuyên khoa 2'
level_school	string	false	Trình độ phổ thông
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị1'1/12'2'2/12'3'3/12'4'4/12'5'5/12'6'6/12'7'7/12'8'8/12'9'9/12'10'10/12'11'11/12'12'12/12'
desc	html	false	Ghi chú
photo	string	false	Ảnh đại diện
image_private_front	string	false	Ảnh CCCD/CMND mặt trước
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị1'Đã có'0'Chưa có'
image_private_back	string	false	Ảnh CCCD/CMND mặt sau
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị1'Đã có'0'Chưa có'
job_bank_id	string	false	Ngân hàng
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trịIDGiá trị13'Ngân hàng Phát triển Việt Nam'14'Ngân hàng Xây dựng'15'Ngân hàng Đại Dương'16'Ngân hàng Dầu Khí Toàn Cầu'17'Ngân hàng Nông nghiệp và Phát triển Nông thôn VN'18'Ngân hàng Á Châu'19'Ngân hàng Tiên Phong'20'Ngân hàng Đông Á'21'Ngân hàng Đông Nam Á'22'Ngân hàng An Bình'23'Ngân hàng Bắc Á'24'Ngân hàng Bản Việt'25'Ngân hàng TMCP Hàng Hải Việt Nam'26'Kỹ Thương Việt Nam'27'Ngân hàng TMCP Kiên Long'28'Ngân hàng Thương mại cổ phần Nam Á'29'Ngân hàng Thương mại Cổ phần Quốc Dân'30'Ngân hàng Việt Nam Thịnh Vượng'31'Ngân hàng thương mại cổ phần và phát triển thành phố Hồ Chí Minh'32'Ngân hàng TMCP Phương Đông'33'Ngân hàng quân đội'34'Ngân hàng TMCP Đại chúng Việt Nam'35'Ngân hàng Quốc tế'36'Ngân hàng Sài Gòn'37'Ngân hàng Sài Gòn Công Thương'38'Ngân hàng Sài Gòn-Hà Nội'39'Ngân hàng Sài Gòn Thương Tín'40'Ngân hàng Việt Á'41'Ngân hàng Bảo Việt'42'Ngân hàng Việt Nam Thương Tín'43'Ngân hàng Xăng dầu Petrolimex'44'Ngân hàng TMCP Xuất Nhập Khẩu Việt Nam'45'Ngân hàng Bưu điện Liên Việt'46'Ngân hàng Ngoại thương Việt Nam'47'Ngân hàng TMCP Công Thương Việt Nam'48'Ngân hàng Đầu tư và Phát triển Việt Nam'49'Ngân hàng TNHH một thành viên ANZ (Việt Nam)'50'Ngân hàng Deutsche Bank Việt Nam'51'Ngân hàng Citibank Việt Nam'52'Ngân hàng TNHH một thành viên HSBC (Việt Nam)'53'Ngân hàng Standard Chartered'54'Ngân hàng TNHH MTV Shinhan Việt Nam'55'Ngân hàng Hong Leong Việt Nam'56'Ngân hàng Đầu tư và Phát triển Campuchia'57'Ngân hàng Public Bank Việt Nam'58'Ngân hàng United Overseas Bank tại Việt Nam'59'Ngân hàng TNHH Indovina'60'Ngân hàng Việt - Nga'61'Ngân hàng Việt - Thái'62'Ví điện tử Payoneer'63'DBS BANK Ltd'64'Oversea Chinese Banking Corporation Limited'65'Ngân hàng Mizuho'66'Ngân hàng Sumitomo Mitsui'67'三菱UFJ銀行'68'りそな'69'新生銀行'70'ゆうちょ銀行'71'Ngân hàng Thương mại Cổ phần Sài Gòn'72'Ngân hàng số người Việt ViettelPay'73'Ngân hàng Woori Việt Nam'74'Ví điện tử Coin98'75'Ngân hàng KASIKORNBANK'77'Ngân hàng Kookmin'78'Ngân hàng Đại chúng TNHH Kasikornbank'80'Ngân hàng TMCP Nhà Hà Nội'81'Ngân hàng Thương mại Cổ phần Lộc Phát Việt Nam'82'Ngân hàng số Liobank by MSB'83'Ngân hàng số Cake by VPBank'84'Ngân hàng số Timo'85'JPMorgan Chase Bank N.A'86'Ngân hàng Trung Quốc'87'Ngân hàng Bangkok'88'Ngân hàng Kasikorn'89'Ngân hàng Krungthai'90'Ngân hàng Thương mại Siam'91'Ngân hàng Ayudhya (Krungsri)'92'Ngân hàng Số Vikki'93'Kho bạc Nhà nước'
job_contract	string	false	Tên hợp đồng
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị38'HĐKV khối Cửa hàng'39'HĐKV khối Cửa hàng - 28 công'37'HĐKV khối VP'34'HĐLĐ khối Cửa hàng'40'HĐLĐ khối Cửa hàng - 28 công'33'HĐLĐ khối VP'42'HĐLĐ Telesale'36'HĐTV khối Cửa hàng'41'HĐTV khối Cửa hàng - 28 công'35'HĐTV khối VP'
salary_real	money	false	Lương vị trí
date_updated	date	false	Ngày sửa hồ sơ
Định dạng dd/mm/YYYY
date_change_salary	date	false	Ngày tăng lương
Định dạng dd/mm/YYYY
level_id	string	false	Cấp bậc
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị5'Bậc 1'6'Bậc 2'7'Bậc 3'8'Bậc 4'
coefficient	money	false	Hệ số tay nghề
user_id	string	false	Tài khoản 1Office
Nhập mã nhân sự hoặc họ và tên của tài khoản người dùng đang hoạt động. Ví dụ: 'NV001' hoặc 'Nguyễn Văn A'...
raw_user_id	integer	false	ID người dùng
job_date_end_review	date	false	Ngày hết hạn thử việc
Định dạng dd/mm/YYYY
date_created	date	false	Ngày tạo
Định dạng dd/mm/YYYY
gps_location_ids	string comma	false	Địa điểm chấm công GPS
Thuộc trong các giá trị sau: 'Tất cả', '110 Nhổn', '116 Cầu Giấy', '122 Trần Hưng Đạo, Thái Bình', '1221 Giải Phóng', '128 Trần Hưng Đạo, Bắc Ninh', '1363 Phạm Văn Thuận, Biên Hoà, Đồng Nai', '154 Quang Trung', '156 Lương Ngọc Quyến, Thái Nguyên', '167 Nguyễn Văn Cừ, TP Vinh', '175 Chùa Bộc', '195 Sơn Tây', '206 Hoàng Văn Thụ, Bắc Giang', '208 Bạch Mai', '2145 Hùng Vương, Phú Thọ', '225-227 Võ Văn Ngân, Hồ Chí Minh', '236-238 Thanh Hoá', '242 Nguyễn Trãi, Hồ Chí Minh', '290 Nguyễn Trãi', '300 Lê Lợi, Hải Phòng', '34 Trần Phú', '378-380 đường Ninh Hiệp (kiot số 13-14 bưu điện)', '42 Bến Nghé, Huế', '436 Lê Duẩn, Đà Nẵng', '567 Quang Trung, Gò Vấp, Hồ Chí Minh', '57 Hàn Thuyên, Nam Định', '58 Trần Hưng Đạo, Hải Dương', '581 Lê Thánh Tông, Quảng Ninh', '61 Đinh Tiên Hoàng, Ninh Bình', '73 Nguyễn Việt Hồng, Cần Thơ', 'Công ty Khải Hưng Cụm CN đa nghề 1 Phường Đình Bảng,Từ Sơn, Bắc Ninh.', 'Cụm 5 Tam Hiệp', 'Liền kề 25-21, Khu đô thị Hinode', 'Liền kề 29-32, Khu đô thị Hinode', 'Số 10, Liền kề 21, Khu đô thị Hinode', 'Số 6-7 Lò Ô Phước, Ninh Hiệp', 'Trạm Trôi', 'Văn phòng Hàm Nghi', 'Xưởng Tân Hoàng Long', 'Yersin, Bình Dương'
labor_group	string	false	Loại lao động
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị
is_econtract	string	false	Ký số
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị0'Không'Mặc định1'Có'
live_manager_id	string comma	false	Người quản lý trực tiếp
Là 1 chuỗi mã hoặc tên nhân sự được phân cách bởi dấu phẩy. Ví dụ: 'NV06,NV08,Nguyễn Văn C'
department_type_id	string	false	Loại phòng ban
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị2'BỘ PHẬN KINH DOANH'1'Khối'
certificate_status	string	false	Trạng thái chữ ký số
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị0'Chưa kích hoạt'1'Kích hoạt'
date_created_certificate	date	false	Ngày cấp chữ ký số
Định dạng dd/mm/YYYY
certificate	string	false	Chữ ký số
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị0'Chưa tạo'1'Đã tạo'
salary_area_id	string	false	Vùng áp dụng lương tối thiểu
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị
salary_min_area	money	false	Lương tối thiểu vùng
created_by_id	string	false	Người tạo
Nhập mã nhân sự hoặc họ và tên của tài khoản người dùng đang hoạt động. Ví dụ: 'NV001' hoặc 'Nguyễn Văn A'...
updated_by_id	string	false	Người sửa.
Nhập mã nhân sự hoặc họ và tên của tài khoản người dùng đang hoạt động. Ví dụ: 'NV001' hoặc 'Nguyễn Văn A'...
type_contract	string	false	Loại hợp đồng
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị38'HĐKV khối Cửa hàng'39'HĐKV khối Cửa hàng - 28 công'37'HĐKV khối VP'34'HĐLĐ khối Cửa hàng'40'HĐLĐ khối Cửa hàng - 28 công'33'HĐLĐ khối VP'42'HĐLĐ Telesale'36'HĐTV khối Cửa hàng'41'HĐTV khối Cửa hàng - 28 công'35'HĐTV khối VP'
work_place	string	false	Nơi làm việc
Thuộc 1 trong các giá trị sau:(Sử dụng cột Giá trị, nếu cấu hình field_raws hãy sử dụng cột ID) IDGiá trị
num_file	integer	false	Số lượng file đính kèm
job_date_join_from	date	false	Từ ngày vào
Định dạng dd/mm/YYYY
job_date_join_to	date	false	Đến ngày vào
Định dạng dd/mm/YYYY
date_join_now_from	date	false	Từ ngày vào công ty hàng năm
Định dạng dd/mm/YYYY
date_join_now_to	date	false	Đến ngày vào công ty hàng năm
Định dạng dd/mm/YYYY
job_reldate_join_from	date	false	Từ ngày ký hđlđ chính thức
Định dạng dd/mm/YYYY
job_reldate_join_to	date	false	Đến ngày ký hđlđ chính thức
Định dạng dd/mm/YYYY
date_trigger_user_from	date	false	Từ ngày tạo tk 1office
Định dạng dd/mm/YYYY
date_trigger_user_to	date	false	Đến ngày tạo tk 1office
Định dạng dd/mm/YYYY
job_date_out_from	date	false	Từ ngày nghỉ việc
Định dạng dd/mm/YYYY
job_date_out_to	date	false	Đến ngày nghỉ việc
Định dạng dd/mm/YYYY
birthday_from	date	false	Từ ngày sinh
Định dạng dd/mm/YYYY
birthday_to	date	false	Đến ngày sinh
Định dạng dd/mm/YYYY
birthday_now_from	date	false	Từ ngày sinh nhật hiện tại
Định dạng dd/mm/YYYY
birthday_now_to	date	false	Đến ngày sinh nhật hiện tại
Định dạng dd/mm/YYYY
private_code_date_from	date	false	Từ ngày cấp cccd
Định dạng dd/mm/YYYY
private_code_date_to	date	false	Đến ngày cấp cccd
Định dạng dd/mm/YYYY
passport_date_from_from	date	false	Từ ngày cấp hộ chiếu
Định dạng dd/mm/YYYY
passport_date_from_to	date	false	Đến ngày cấp hộ chiếu
Định dạng dd/mm/YYYY
passport_date_expire_from	date	false	Từ ngày hết hạn
Định dạng dd/mm/YYYY
passport_date_expire_to	date	false	Đến ngày hết hạn
Định dạng dd/mm/YYYY
date_leave_from_from	date	false	Từ nghỉ việc từ ngày
Định dạng dd/mm/YYYY
date_leave_from_to	date	false	Đến nghỉ việc từ ngày
Định dạng dd/mm/YYYY
date_leave_to_from	date	false	Từ nghỉ việc đến ngày
Định dạng dd/mm/YYYY
date_leave_to_to	date	false	Đến nghỉ việc đến ngày
Định dạng dd/mm/YYYY
due_date_from	date	false	Từ ngày dự sinh
Định dạng dd/mm/YYYY
due_date_to	date	false	Đến ngày dự sinh
Định dạng dd/mm/YYYY
date_updated_from	date	false	Từ ngày sửa hồ sơ
Định dạng dd/mm/YYYY
date_updated_to	date	false	Đến ngày sửa hồ sơ
Định dạng dd/mm/YYYY
date_change_salary_from	date	false	Từ ngày tăng lương
Định dạng dd/mm/YYYY
date_change_salary_to	date	false	Đến ngày tăng lương
Định dạng dd/mm/YYYY
job_date_end_review_from	date	false	Từ ngày hết hạn thử việc
Định dạng dd/mm/YYYY
job_date_end_review_to	date	false	Đến ngày hết hạn thử việc
Định dạng dd/mm/YYYY
date_created_from	date	false	Từ ngày tạo
Định dạng dd/mm/YYYY
date_created_to	date	false	Đến ngày tạo
Định dạng dd/mm/YYYY
date_created_certificate_from	date	false	Từ ngày cấp chữ ký số
Định dạng dd/mm/YYYY
date_created_certificate_to	date	false	Đến ngày cấp chữ ký số
Định dạng dd/mm/YYYY
1.1.2	Chi tiết hồ sơ nhân sự
Method: GET
Url: https://minham.1office.vn/api/personnel/profile/item
Access Token: 80391498468e8ae4c02f02819998254
Params:
Field	Type	Required	Description
access_token*	string	true	Mã bảo mật
code*	string	true	Đối tượng mã cần lấy thông tin