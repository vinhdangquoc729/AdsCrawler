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
docker build -f Dockerfile.clickhouse -t mkt_clickhouse:latest .
```

---

### 📌 Bước 4: Chuyển Docker Images vào máy ảo Minikube

Vì Minikube hoạt động trong một môi trường cô lập (máy ảo riêng), nó sẽ **không nhìn thấy** các Docker image bạn vừa build trên máy thật. Cần "gửi" chúng vào trong máy ảo bằng 2 lệnh sau:

```bash
minikube image load mkt_airflow:latest
minikube image load mkt_superset:latest
minikube image load mkt_clickhouse:latest
```
*(Quá trình tải image vào máy ảo có thể mất 1-2 phút cho mỗi lệnh.)*

---

### 📌 Bước 5: Khởi chạy toàn bộ hệ thống K8s

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

## 📥 Tạo Kafka Topics và đăng ký Connectors (Bắt buộc sau mỗi lần fresh start)

Kafka không tự tạo topic khi có producer ghi vào. Nếu bỏ qua bước này, `speed-layer` sẽ bị lỗi `UnknownTopicOrPartitionException` và crash liên tục.

### Tạo 11 raw input topics

Sau khi Kafka chuyển sang trạng thái `Running`, chạy một lệnh duy nhất:

```bash
kubectl exec -n marketing deployment/kafka -- bash -c "
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic fad_ad_daily_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic fad_age_gender_detailed_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic TTA_ad_performance &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_campaign_daily_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_ad_group_daily_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_account_daily_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_keyword_performance_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_age_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_gender_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_ad_asset_daily_report &&
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic gad_click_type_report
"
```

Sau đó restart speed-layer để nhận diện topics mới:

```bash
kubectl rollout restart deployment/speed-layer -n marketing
```

### Đăng ký Kafka Connectors

Sau khi `kafka-connect` chuyển sang `Running`, đăng ký 2 connectors (S3 Sink → MinIO và JDBC Sink → ClickHouse):

```bash
# S3 Sink connector (Kafka → MinIO)
kubectl exec -n marketing deployment/kafka-connect -- \
  curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/spark/work-dir/kafka-connect/connect-s3-sink.json

# JDBC Sink connector (Kafka processed_* → ClickHouse rt_* tables)
kubectl exec -n marketing deployment/kafka-connect -- \
  curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/spark/work-dir/kafka-connect/connect-jdbc-sink.json
```

Kiểm tra connector đã đăng ký chưa:

```bash
kubectl exec -n marketing deployment/kafka-connect -- \
  curl -s http://localhost:8083/connectors
```

> [!NOTE]
> Kafka có PVC (`kafka-pvc`) để lưu trữ dữ liệu topics và connector offsets. Sau khi đã tạo topics và đăng ký connectors một lần, chúng sẽ **tự động phục hồi** sau khi Kafka restart mà không cần làm lại bước này — trừ khi xóa PVC hoặc chạy `minikube delete`.

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

## 📊 Phân phối tài nguyên (Resource Allocation)

Hệ thống sử dụng hai mức tài nguyên cho mỗi Pod:
- **`requests`**: Lượng tài nguyên K8s "đặt chỗ" khi lên lịch pod vào node. Tổng requests của tất cả pod phải nhỏ hơn capacity của node.
- **`limits`**: Giới hạn tối đa pod được phép sử dụng. Có thể vượt tổng capacity (overcommit) vì các pod không dùng tối đa cùng lúc. Pod bị OOMKill nếu vượt memory limit, bị throttle nếu vượt CPU limit.

### Cấu hình hiện tại (Minikube: 4 CPU · 12 GB RAM)

| Service | CPU Request → Limit | Memory Request → Limit | Vai trò |
| :--- | :--- | :--- | :--- |
| **airflow-scheduler** | 200m → 1000m | 512Mi → 2Gi | Điều phối và chạy DAG tasks |
| **airflow-webserver** | 200m → 1000m | 512Mi → 2Gi | Giao diện web Airflow |
| **kafka** | 200m → 1000m | 512Mi → 1Gi | Message broker |
| **kafka-connect** | 200m → 1000m | 512Mi → 3Gi | Sink dữ liệu từ Kafka → MinIO |
| **clickhouse** | 200m → 1000m | 512Mi → 2Gi | Data warehouse |
| **spark-master** | 200m → 1000m | 512Mi → 1Gi | Điều phối Spark cluster |
| **spark-worker** | 200m → 1000m | 512Mi → 2Gi | Thực thi Spark jobs |
| **speed-layer** | 200m → 1000m | 512Mi → 1536Mi | Xử lý stream realtime |
| **superset** | 200m → 1000m | 512Mi → 1Gi | Giao diện trực quan hóa |
| **minio** | 100m → 500m | 256Mi → 1Gi | Object storage (Data Lake) |
| **postgres** | 100m → 500m | 256Mi → 512Mi | Metadata database cho Airflow |
| **batch-consumer** | 100m → 500m | 256Mi → 512Mi | Consumer Kafka → MinIO |
| **Tổng requests** | **2100m (52%)** | **5.25 GB (40%)** | |
| **Tổng limits** | 10500m (262%) | ~18 GB (139%) |  |

> [!NOTE]
> **Tại sao `requests` thấp hơn `limits` nhiều?** Requests thấp giúp K8s có thể schedule tất cả pod lên node. Còn limits cao để các pod như kafka-connect (cần 3GB khi khởi động) hay airflow-scheduler (cần 2GB khi chạy Spark job) có đủ bộ nhớ thực tế. Các pod không bao giờ dùng hết tài nguyên cùng một lúc nên overcommit là an toàn.

### Cơ chế quản lý xung đột Spark

Spark Worker chỉ có **1 core** (giới hạn bởi `limits.cpu: 1000m`). Để tránh speed-layer và batch ingest tranh nhau core duy nhất đó, DAG tự động:
1. **Tạm dừng** speed-layer (`replicas=0`) trước khi chạy batch ingest
2. **Khôi phục** speed-layer (`replicas=1`) sau khi batch ingest hoàn tất (kể cả khi ingest thất bại)

---

## 🛠️ Xử lý sự cố (Troubleshooting)

### 1. Pod hiển thị trạng thái `Pending` mãi không chuyển sang `Running`
*   **Nguyên nhân:** Máy tính bị thiếu RAM hoặc CPU tự do nên cụm K8s không thể phân bổ tài nguyên.
*   **Cách xử lý:** Tắt bớt các ứng dụng nặng trên máy (Chrome, game, IDE khác) và thử gõ lệnh `minikube stop` rồi chạy lại `minikube start` với cấu hình RAM nhỏ hơn một chút, hoặc cân nhắc nâng cấp RAM máy tính.

### 2. Pod hiển thị trạng thái `ImagePullBackOff` hoặc `ErrImagePull`
*   **Nguyên nhân:** Máy ảo Minikube không tìm thấy Docker image tùy chỉnh trên máy bạn.
*   **Cách xử lý:** Đảm bảo đã chạy đúng 2 lệnh `minikube image load` ở **Bước 4**.

### 3. Airflow không hiển thị các file DAGs
*   **Nguyên nhân:** DAGs được bake vào Docker image lúc build. Nếu không thấy DAG, có thể image cũ chưa được rebuild.
*   **Cách xử lý:** Rebuild image và load lại vào Minikube:
    ```bash
    docker build -f Dockerfile.airflow -t mkt_airflow:latest .
    minikube image load mkt_airflow:latest
    kubectl rollout restart deployment/airflow-scheduler deployment/airflow-webserver -n marketing
    ```

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

### 6. `airflow-scheduler` / `airflow-webserver` CrashLoopBackOff với lỗi "You need to initialize the database"

*   **Nguyên nhân:** `airflow-init` job chưa hoàn thành khi scheduler/webserver start. Race condition khi fresh deploy.
*   **Cách xử lý:** Đợi `airflow-init` chuyển sang `Completed` (khoảng 3-4 phút), scheduler/webserver sẽ tự retry và phục hồi. Nếu không tự phục hồi, chạy thủ công:
    ```bash
    kubectl exec -n marketing deployment/airflow-scheduler -- airflow db init
    ```

### 7. `speed-layer` CrashLoopBackOff với lỗi `UnknownTopicOrPartitionException`

*   **Nguyên nhân:** Kafka topics chưa được tạo.
*   **Cách xử lý:** Chạy lệnh tạo 11 topics ở mục **📥 Tạo Kafka Topics** ở trên, sau đó restart speed-layer.

### 8. Muốn xem nhật ký hoạt động (Logs) của một Pod để debug
```bash
# Xem log của Pod (Thay <tên-pod> bằng tên pod thực tế)
kubectl logs -n marketing <tên-pod>

# Xem log theo thời gian thực (giống tail -f)
kubectl logs -n marketing <tên-pod> -f
```
