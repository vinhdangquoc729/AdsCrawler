# Tài Liệu Kỹ Thuật: Hệ Thống Sinh Dữ Liệu Marketing Giả Lập (Mock Data)

Tài liệu này cung cấp cái nhìn chi tiết về công cụ **Mock Data Generator** được triển khai trong dự án `AdsCrawler`. Công cụ này không chỉ sinh số ngẫu nhiên mà còn mô phỏng các quy luật thực tế trong ngành Marketing.

---

## 1. Tổng Quan Hệ Thống

Bộ sinh dữ liệu được thiết kế để tạo ra dữ liệu đa tầng (Account > Campaign > Ad Set > Ad) có tính nhất quán cao, phục vụ cho việc kiểm thử các hệ thống Big Data (Spark, ClickHouse).

### Đặc điểm nổi bật:
- **Tính tái lập (Reproducibility)**: Cùng một Seed và Ngày sẽ luôn cho ra kết quả giống hệt nhau.
- **Tích hợp MinIO**: Dữ liệu đẩy trực tiếp vào Landing Zone dưới dạng JSON.
- **Độ phân giải cao**: Mô phỏng các biến số thị trường như lạm phát CPM, tỷ lệ giữ chân video, và hành vi người dùng theo nhân khẩu học.

---

## 2. Quy Trình Khởi Tạo Dữ Liệu

Quá trình sinh dữ liệu chia làm 2 giai đoạn chính:

### Giai đoạn A: Khởi tạo khung (Skeleton Setup)
Trước khi sinh số liệu hàng ngày, hệ thống xây dựng "bộ khung" thực thể:
1.  **Creative Pool**: Tạo ra một kho gồm 20 mẫu nội dung (Video/Banner) dùng chung.
2.  **Vòng đời thực thể**: Mỗi Ads/AdSet được gán ngày bắt đầu và kết thúc ngẫu nhiên.
3.  **Chỉ số chất lượng (Quality Score)**: Mỗi Ad có một hệ số chất lượng (0.6x - 2.2x) ảnh hưởng đến hiệu quả xuyên suốt vòng đời.
4.  **Phân tích mục tiêu (Targeting Bias)**: Hệ thống tự phân tích tên AdSet để xác định tệp khách hàng mục tiêu (ví dụ: "Phụ nữ", "CEO").

### Giai đoạn B: Vòng lặp Waterfall hàng ngày
1.  **Seed theo ngày**: Khởi tạo lại bộ sinh số ngẫu nhiên dựa trên `BaseSeed + Date`.
2.  **Lọc thực thể**: Chỉ sinh dữ liệu cho các Ads đang trong trạng thái "Active".
3.  **Tính toán chỉ số**: Áp dụng các quy luật Marketing phi tuyến tính.

---

## 3. Các Quy Luật Marketing Nâng Cao

### 1. Quy luật "Lợi nhuận giảm dần" (Diminishing Returns)
Khi ngân sách tăng, chi phí để tiếp cận thêm khách hàng mới sẽ đắt hơn.
> **Công thức**: `CPM = CPM_Gốc * Hệ_số_mùa_vụ * (1 + log10(Spend / 50.000)) / Hệ_số_chất_lượng`
- **Hiệu quả**: Các Ads có ngân sách càng lớn thì CPM sẽ càng cao.

### 2. Tính mùa vụ và Biến động cuối tuần
- **Cuối tuần (Thứ 6 - CN)**: CPM tăng **1.4 lần**.
- **Dịp lễ (Tết Nguyên Đán)**: CPM tăng mạnh **1.8 lần** do sự cạnh tranh thầu tăng cao.

### 3. Mô hình thác nước (Waterfall Funnel)
Đảm bảo tính logic tuyệt đối của phễu chuyển đổi:
- **Phễu tương tác (Click Funnel)**:
    - `Clicks`: Tổng hợp tất cả tương tác (like, share, xem thêm, tên fanpage).
    - `Link Clicks`: Chiếm **65-85%** của `Clicks` (chỉ những người thực sự bấm vào link về web).
    - `Landing Page Views`: Chiếm **70-90%** của `Link Clicks` (rơi rụng do rớt mạng, web load chậm).
- **Phễu hiển thị & Video**:
    - `Impressions` -> `v25` (8-15%) -> `v50` (30-50% của v25) -> `v100` (70-90% của v95).

### 4. Phân phối theo nhân khẩu học (Targeting Bias)
Tự động lái trọng số Weight dựa trên từ khóa trong tên AdSet:
- **Khớp mục tiêu**: Tệp khách hàng mục tiêu chiếm **99%** trọng số.
- **Nhiễu hệ thống**: Các tệp không liên quan chiếm **1%** để mô phỏng click nhầm hoặc sai tệp thực tế.

### 5. Tính nhất quán nội dung (Creative Consistency)
Nhiều Ads ở các tài khoản khác nhau có thể dùng chung 1 `creative_id`.
- **Giá trị**: Cho phép thực hiện bài toán **Creative Analytics** (Phân tích hiệu quả nội dung trên quy mô toàn hệ thống).

---

## 4. Danh Mục Bảng Dữ Liệu

| Tên Bảng | Phân vùng (Partition) | Các chỉ số chính |
| :--- | :--- | :--- |
| `fad_ad_daily_report` | `YYYY/MM/DD` | Spend, Impressions, Clicks, Video Funnel, Messages |
| `fad_age_gender_detailed` | `YYYY/MM/DD` | Chỉ số chia theo Độ tuổi và Giới tính |
| `fad_ad_performance_report` | (Toàn thời gian) | Tổng hợp trọn đời cho mỗi Ads |

---

## 5. Hướng Dẫn Sử Dụng

1.  **Chạy sinh dữ liệu**:
    ```powershell
    python kafka_ingestion/run_mock.py
    ```
2.  **Kiểm tra Excel**: Mở file `mock_data_report.xlsx` để xem số liệu trực quan.
3.  **Cấu hình**: Chỉnh sửa file `.env` hoặc `run_mock.py` để thay đổi `seed` hoặc `endpoint` MinIO.

> [!IMPORTANT]
> Để sinh ra bộ dữ liệu giống hệt lần cũ, hãy giữ nguyên giá trị `seed`, `start_date` và `end_date`.
