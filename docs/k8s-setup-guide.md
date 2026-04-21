# Hướng dẫn Deploy AdsCrawler lên Minikube (Windows)

## Yêu cầu hệ thống

|  | Yêu cầu tối thiểu |
|-----|-------------------|
| RAM | **12 GB trở lên** (stack nặng: Kafka + Spark + Airflow + ClickHouse + Superset) |
| CPU | 4 nhân trở lên |
| Dung lượng ổ cứng | **Tối thiểu 20 GB** trống |
| Docker Desktop | Đã cài, đang chạy |
| WSL2 | Đã bật (thường có sẵn khi cài Docker Desktop) |

---

## Bước 1: Cài Minikube

**Tải về và cài đặt:**

1. Tải file cài đặt từ trang chủ Minikube: https://minikube.sigs.k8s.io/docs/start/
2. Chọn: **Windows → x86-64 → .exe installer**
3. Chạy file `.exe` vừa tải về, nhấn Next cho đến khi xong

**Kiểm tra đã cài thành công:**
```bash
minikube version
kubectl version --client
```

---

## Bước 2: Khởi động Minikube

Mở terminal (PowerShell hoặc Git Bash) và chạy:

```bash
minikube start --memory=8192 --cpus=4 --driver=docker
```

> **Giải thích:**
> - `--memory=8192` → cấp 8GB RAM cho Minikube VM
> - `--cpus=4` → cấp 4 CPU core
> - `--driver=docker` → dùng Docker Desktop làm nền tảng
>
> Lần đầu chạy sẽ mất 3-5 phút để tải image về.

**Kiểm tra Minikube đã chạy:**
```bash
minikube status
```

Kết quả mong đợi:
```
minikube
type: Control Plane
host: Running
kubelet: Running
apiserver: Running
kubeconfig: Configured
```

---

## Bước 3: Build custom Docker images
Dự án dùng 2 image tự build (không có sẵn trên Docker Hub):

```bash
# Build từ thư mục gốc dự án
docker build -f Dockerfile.airflow -t mkt_airflow:latest .
docker build -f Dockerfile.superset -t mkt_superset:latest .
```

---

## Bước 4: Load images vào Minikube
Minikube chạy trong một VM riêng biệt, **không thấy** được các Docker image trên máy bạn. Phải "chuyển" image vào bên trong Minikube:

```bash
minikube image load mkt_airflow:latest
minikube image load mkt_superset:latest
```

> Mỗi lệnh mất 1-2 phút. Sau khi load xong, Minikube mới có thể dùng image này để chạy Pod.

---

## Bước 5: Mount thư mục DAGs (quan trọng!)

Airflow cần đọc các file DAG từ thư mục `./airflow/dags/` trên máy bạn. Bước này "kết nối" thư mục đó vào trong Minikube VM.

**Mở một terminal riêng (Terminal 2)** và chạy:

```bash
# Chạy từ thư mục gốc dự án
minikube mount ./airflow/dags:/opt/airflow/dags
```

> **Quan trọng:** Giữ terminal này **mở suốt** trong quá trình chạy hệ thống. Nếu đóng terminal này, Airflow sẽ không thấy DAG nữa.

Kết quả mong đợi:
```
📁  Mounting host path ./airflow/dags into VM as /opt/airflow/dags ...
...
🚀  Userspace file server: ufs starting
✅  Successfully mounted ./airflow/dags to /opt/airflow/dags
```

---

## Bước 6: Deploy toàn bộ hệ thống

Quay lại **Terminal 1**, chạy:

```bash
make k8s-up
```

Lệnh này tương đương với:
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps/
kubectl apply -f k8s/secrets/
kubectl apply -f k8s/pvc/
kubectl apply -f k8s/deployments/
kubectl apply -f k8s/services/
kubectl apply -f k8s/jobs/
```

---

## Bước 7: Chờ hệ thống khởi động

Chạy lệnh sau để xem trạng thái các Pod:

```bash
make k8s-status
# Hoặc:
kubectl get pods -n marketing -w
```

> Flag `-w` (watch) sẽ tự động cập nhật màn hình. Nhấn `Ctrl+C` để dừng.

**Trạng thái cần đạt được (tất cả `Running` hoặc `Completed`):**

```
NAME                                  READY   STATUS      RESTARTS
airflow-scheduler-xxx                 1/1     Running     0
airflow-webserver-xxx                 1/1     Running     0
clickhouse-xxx                        1/1     Running     0
kafka-xxx                             1/1     Running     0
minio-xxx                             1/1     Running     0
minio-init-xxx                        0/1     Completed   0
postgres-xxx                          1/1     Running     0
spark-master-xxx                      1/1     Running     0
spark-worker-xxx                      1/1     Running     0
superset-xxx                          1/1     Running     0
airflow-init-xxx                      0/1     Completed   0
```

> **Mẹo:** Airflow và Superset mất 2-3 phút để khởi động. Trạng thái `Init:0/1` hay `ContainerCreating` là bình thường, cứ chờ.

---

## Bước 8: Truy cập các giao diện UI

Dùng các lệnh trong Makefile để mở trực tiếp trên trình duyệt:

```bash
make airflow-ui    # Airflow: http://192.168.x.x:30082
make superset-ui   # Superset: http://192.168.x.x:30088
make minio-ui      # MinIO Console: http://192.168.x.x:30006
make spark-ui      # Spark Master: http://192.168.x.x:30081
```

**Hoặc** lấy IP của Minikube rồi truy cập thủ công:

```bash
minikube ip
# Ví dụ kết quả: 192.168.49.2
```

| Service | URL | Account |
|---------|-----|-----------|
| Airflow UI | `http://<minikube-ip>:30082` | admin / password123 |
| Superset UI | `http://<minikube-ip>:30088` | admin / password123 |
| MinIO Console | `http://<minikube-ip>:30006` | admin / password123 |
| Spark Master | `http://<minikube-ip>:30081` | (không cần đăng nhập) |
| ClickHouse HTTP | `http://<minikube-ip>:30123` | admin / password123 |

---

## Bước 9: Tạo Kafka Topic (bước một lần)

Sau khi Kafka đã `Running`, tạo topic cho pipeline:

```bash
# Lấy tên pod Kafka
kubectl get pods -n marketing | grep kafka

# Tạo topic (thay <kafka-pod-name> bằng tên thực)
kubectl exec -n marketing <kafka-pod-name> -- \
  kafka-topics --create \
  --topic topic_fb_raw \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

## Tắt hệ thống khi xong

```bash
# Xóa tất cả tài nguyên K8s
make k8s-down

# Tắt Minikube VM (giữ lại data)
minikube stop

# Hoặc xóa hoàn toàn Minikube VM (mất hết data)
minikube delete
```

---

## Xử lý lỗi thường gặp

### Pod bị `Pending` mãi không chạy

```bash
kubectl describe pod <tên-pod> -n marketing
```

Xem phần `Events` ở cuối output. Thường gặp: thiếu RAM → tăng `--memory` khi start Minikube.

### Pod bị `ImagePullBackOff`

Image chưa được load vào Minikube. Chạy lại:
```bash
minikube image load mkt_airflow:latest
minikube image load mkt_superset:latest
```

### Xem logs của một Pod

```bash
kubectl logs -n marketing <tên-pod>
# Xem real-time:
kubectl logs -n marketing <tên-pod> -f
```

### Airflow không thấy DAG

Kiểm tra terminal mount còn mở không. Nếu đã đóng, chạy lại:
```bash
minikube mount ./airflow/dags:/opt/airflow/dags
```
