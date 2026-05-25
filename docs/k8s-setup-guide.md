# Hướng dẫn Deploy AdsCrawler lên Minikube (Windows & Linux)

## K8s là gì?

Các thành phần chính của **Kubernetes**:
*   **Pod (Thùng hàng):** Là đơn vị nhỏ nhất, chứa một hoặc vài container (ví dụ: container chạy Postgres, container chạy Spark).
*   **Service (Người chỉ đường):** Giúp các Pod có thể nói chuyện được với nhau qua một địa chỉ cố định, hoặc giúp chúng ta truy cập UI của hệ thống từ trình duyệt bên ngoài.
*   **Namespace (Phân khu):** Giống như một phòng ban riêng biệt trong dự án. Ở đây chúng ta gom tất cả tài nguyên vào khu vực tên là `marketing`.
*   **Minikube:** Là một công cụ tạo ra một "máy ảo" giả lập cụm Kubernetes chạy ngay trên máy tính cá nhân của bạn.

---

## Yêu cầu hệ thống tối thiểu

Vì hệ thống chạy rất nhiều dịch vụ nặng cùng lúc (Kafka, Spark, Airflow, ClickHouse, MinIO, Kafka Connect, v.v.), thiết bị cần đáp ứng:

| Tiêu chí | Cấu hình đề xuất |
| :--- | :--- |
| **RAM** | **12 GB trở lên** (Khuyến nghị 16 GB để chạy mượt mà) |
| **CPU** | **4 nhân trở lên** (Minikube cần ≥2 core cho hệ thống, Spark Worker cần thêm ≥2 core để speed-layer và batch ingest chạy song song không tranh nhau) |
| **Ổ cứng** | Trống tối thiểu **20 GB** |
| **Công cụ (Windows)** | **Docker Desktop** đã được cài đặt và đang chạy |
| **Công cụ (Linux)** | **Docker Engine** (`docker-ce`) đã được cài đặt và đang chạy |

---

## Quy trình triển khai

### Bước 1: Cài đặt Minikube và kubectl

#### 🪟 Windows

1.  Tải và cài đặt Minikube từ trang chủ: [Tải Minikube](https://minikube.sigs.k8s.io/docs/start/)
    *   *Chọn cấu hình: Windows → x86-64 → .exe installer*
2.  Sau khi cài xong, mở **PowerShell (quyền Admin)** và kiểm tra:
    ```powershell
    minikube version
    kubectl version --client
    ```

#### 🐧 Linux

1.  Cài Minikube bằng binary (hoạt động trên mọi distro):
    ```bash
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube
    rm minikube-linux-amd64
    ```
2.  Cài kubectl:
    ```bash
    # Ubuntu / Debian
    sudo apt update && sudo apt install -y kubectl

    # Hoặc cài bằng binary (hoạt động với mọi distro)
    curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    sudo install kubectl /usr/local/bin/kubectl && rm kubectl
    ```
3.  Thêm user hiện tại vào group `docker` (bắt buộc, tránh phải dùng `sudo` mỗi lần):
    ```bash
    sudo usermod -aG docker $USER
    newgrp docker
    ```
4.  Kiểm tra cài đặt:
    ```bash
    minikube version
    kubectl version --client
    ```

*Nếu màn hình hiển thị số phiên bản (version) tức là cài đặt thành công.*

---

### 📌 Bước 2: Khởi động máy ảo Minikube

Chạy lệnh sau để cấp phát tài nguyên cho Minikube:

```bash
minikube start --memory=8192 --cpus=4 --driver=docker
```

> [!NOTE]
> *   `--memory=8192`: Cấp 8GB RAM cho máy ảo chạy K8s.
> *   `--cpus=4`: Cấp 4 nhân CPU.
> *   `--driver=docker`: Sử dụng Docker làm môi trường nền tảng (Windows dùng Docker Desktop, Linux dùng Docker Engine).
> *   *Lưu ý: Lần đầu tiên chạy lệnh này sẽ mất từ 3 đến 5 phút để tải các gói cài đặt về máy.*

Kiểm tra trạng thái máy ảo bằng lệnh:
```bash
minikube status
```
Khi thấy dòng `host: Running` và `apiserver: Running` nghĩa là máy ảo đã sẵn sàng hoạt động!

---

### 📌 Bước 3: Đóng gói Docker Images của dự án

Dự án sử dụng các Docker image tùy biến (tự viết Dockerfile riêng chứ không dùng trực tiếp từ thư viện chung). Cần phải build chúng trước:

```bash
docker build -f Dockerfile.airflow -t mkt_airflow:latest .
docker build -f Dockerfile.superset -t mkt_superset:latest .
```

---

### 📌 Bước 4: Chuyển Docker Images vào máy ảo Minikube

Vì Minikube hoạt động trong một môi trường cô lập (máy ảo riêng), nó sẽ **không nhìn thấy** các Docker image bạn vừa build trên máy thật. Cần "gửi" chúng vào trong máy ảo bằng 2 lệnh sau:

```bash
minikube image load mkt_airflow:latest
minikube image load mkt_superset:latest
```
*(Quá trình tải ảnh vào máy ảo có thể mất 1-2 phút cho mỗi lệnh.)*

---

### 📌 Bước 5: Cầu nối thư mục (Mount dự án)

Để các dịch vụ bên trong K8s (như Spark để chạy code, Airflow để đọc DAGs, ClickHouse để nạp dữ liệu khởi tạo) có thể đọc được mã nguồn trên máy tính của bạn, chúng ta cần thực hiện một cầu nối dữ liệu:

1.  Mở một cửa sổ terminal mới độc lập (**Terminal 2**).
2.  Chạy lệnh mount từ thư mục gốc dự án:
    ```bash
    minikube mount .:/opt/spark/work-dir
    ```

> [!IMPORTANT]
> **Không được đóng terminal này!** Hãy giữ terminal này luôn mở trong suốt quá trình làm việc. Nếu đóng cửa sổ này, kết nối giữa máy tính và máy ảo Minikube sẽ bị ngắt, dẫn đến việc Airflow không thấy DAGs và Clickhouse/Spark không đọc được code.

---

### 📌 Bước 6: Khởi chạy toàn bộ hệ thống K8s

Quay lại cửa sổ terminal chính (**Terminal 1**), chạy lệnh duy nhất sau để tự động cấu hình và khởi động mọi Pod, Service và Job:

```bash
make k8s-up
```

> [!TIP]
> Lệnh `make k8s-up` sẽ tự động thực thi các file cấu hình YAML nằm trong thư mục `k8s/` để tạo các phân khu quản lý, phân bổ tài nguyên lưu trữ và kích hoạt toàn bộ hệ thống (Postgres, ClickHouse, Kafka, MinIO, Kafka Connect, Spark, Airflow, Superset và các Worker thu thập dữ liệu).

---

## 🔍 Kiểm tra trạng thái hoạt động của hệ thống

Để kiểm tra xem hệ thống đã hoạt động bình thường chưa, hãy chạy lệnh:

```bash
make k8s-status
# Hoặc:
kubectl get pods -n marketing -w
```
*(Ấn `Ctrl + C` nếu muốn thoát khỏi chế độ theo dõi thời gian thực).*

**Kết quả mong đợi:** Sau khoảng 2-3 phút khởi động, tất cả Pod phải chuyển sang trạng thái `Running` hoặc `Completed` như hình dưới:

```text
NAME                                  READY   STATUS      RESTARTS
postgres-xxx                          1/1     Running     0
minio-xxx                             1/1     Running     0
minio-init-xxx                        0/1     Completed   0
clickhouse-xxx                        1/1     Running     0
kafka-xxx                             1/1     Running     0
kafka-connect-xxx                     1/1     Running     0
kafka-connect-init-xxx                0/1     Completed   0
spark-master-xxx                      1/1     Running     0
spark-worker-xxx                      1/1     Running     0
airflow-init-xxx                      0/1     Completed   0
airflow-scheduler-xxx                 1/1     Running     0
airflow-webserver-xxx                 1/1     Running     0
batch-consumer-xxx                    1/1     Running     0
speed-layer-xxx                       1/1     Running     0
superset-xxx                          1/1     Running     0
```

> [!NOTE]
> *   Các Pod có chữ `-init` ở cuối như `minio-init`, `airflow-init`, `kafka-connect-init` có vai trò khởi tạo cài đặt ban đầu (tạo bucket, tạo bảng, tạo tài khoản). Khi làm xong nhiệm vụ, chúng sẽ dừng lại và hiển thị trạng thái `Completed` (Hoàn thành) là hoàn toàn chính xác.

---

## Đường dẫn truy cập các giao diện quản trị (UI)

```bash
make airflow-ui          # Mở giao diện lập lịch Airflow
make superset-ui         # Mở giao diện trực quan hóa dữ liệu Superset
make minio-ui            # Mở giao diện lưu trữ tệp tin MinIO Console
make spark-ui            # Mở giao diện giám sát Spark Master
make kafka-connect-ui    # Mở giao diện API kết nối Kafka Connect
```

**Tài khoản mặc định:**

| Dịch vụ | URL ví dụ từ Minikube | Tài khoản đăng nhập |
| :--- | :--- | :--- |
| **Airflow UI** | `http://<minikube-ip>:30082` | **admin** / **password123** |
| **Superset UI** | `http://<minikube-ip>:30088` | **admin** / **password123** |
| **MinIO Console** | `http://<minikube-ip>:30006` | **admin** / **password123** |
| **Spark Master** | `http://<minikube-ip>:30081` | *(Không cần đăng nhập)* |
| **ClickHouse HTTP** | `http://<minikube-ip>:30123` | **admin** / **password123** |

*(Để lấy `<minikube-ip>`, gõ lệnh `minikube ip` vào terminal).*

---

## 📥 Tạo Kafka Topic (Chỉ thực hiện một lần)

Sau khi kiểm tra thấy Kafka chuyển sang trạng thái `Running`, hãy chạy lệnh sau để tạo topic phục vụ việc truyền nhận dữ liệu thời gian thực:

```bash
# 1. Lấy tên chính xác của Pod Kafka đang chạy:
kubectl get pods -n marketing | grep kafka

# 2. Tạo topic nhận tin (Thay <tên-pod-kafka> bằng tên hiển thị ở lệnh trên)
kubectl exec -n marketing <tên-pod-kafka> -- kafka-topics --create --topic topic_fb_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## 🛑 Dừng và Dọn dẹp hệ thống khi làm việc xong

Khi muốn nghỉ ngơi hoặc giải phóng bộ nhớ cho máy tính của bạn:

```bash
# 1. Xóa bỏ tất cả các tài nguyên và Pod đang chạy trong cụm K8s
make k8s-down

# 2. Tắt máy ảo Minikube (Dữ liệu cũ vẫn được giữ lại cho lần sau)
minikube stop

# 3. (Tùy chọn) Xóa hoàn toàn máy ảo Minikube để giải phóng ổ cứng (Sẽ mất hết dữ liệu cấu hình)
minikube delete
```

---

## 🛠️ Xử lý sự cố (Troubleshooting)

### 1. Pod hiển thị trạng thái `Pending` mãi không chuyển sang `Running`
*   **Nguyên nhân:** Máy tính bị thiếu RAM hoặc CPU tự do nên cụm K8s không thể phân bổ tài nguyên.
*   **Cách xử lý:** Tắt bớt các ứng dụng nặng trên máy (Chrome, game, IDE khác) và thử gõ lệnh `minikube stop` rồi chạy lại `minikube start` với cấu hình RAM nhỏ hơn một chút, hoặc cân nhắc nâng cấp RAM máy tính.

### 2. Pod hiển thị trạng thái `ImagePullBackOff` hoặc `ErrImagePull`
*   **Nguyên nhân:** Máy ảo Minikube không tìm thấy Docker image tùy chỉnh trên máy bạn.
*   **Cách xử lý:** Đảm bảo đã chạy đúng 2 lệnh `minikube image load` ở **Bước 4**.

### 3. Airflow không hiển thị các file DAGs
*   **Nguyên nhân:** Bạn chưa thực hiện lệnh Mount hoặc đã lỡ đóng **Terminal 2** (Terminal chạy lệnh mount).
*   **Cách xử lý:** Mở lại terminal mới và chạy lại lệnh ở **Bước 5**: `minikube mount .:/opt/spark/work-dir`.

### 4. (Linux) Lỗi `permission denied` khi chạy lệnh docker
*   **Nguyên nhân:** User hiện tại chưa được thêm vào group `docker`.
*   **Cách xử lý:**
    ```bash
    sudo usermod -aG docker $USER
    newgrp docker
    ```
    Nếu vẫn lỗi sau lệnh trên, hãy **logout** rồi **login** lại để thay đổi group có hiệu lực.

### 5. (Linux) Minikube báo lỗi `The "docker" driver should not be used with root privileges`
*   **Nguyên nhân:** Bạn đang chạy lệnh với `sudo` hoặc đang đăng nhập bằng tài khoản `root`.
*   **Cách xử lý:** Chạy lệnh với tài khoản user thường (không phải root), và đảm bảo user đó đã được thêm vào group `docker` như hướng dẫn ở Bước 1.

### 6. Muốn xem nhật ký hoạt động (Logs) của một Pod để debug
```bash
# Xem log của Pod (Thay <tên-pod> bằng tên pod thực tế)
kubectl logs -n marketing <tên-pod>

# Xem log theo thời gian thực (giống tail -f)
kubectl logs -n marketing <tên-pod> -f
```
