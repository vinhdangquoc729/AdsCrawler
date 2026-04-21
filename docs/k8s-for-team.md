# Kubernetes là gì?
Kubernetes (thường gọi tắt là K8s) là một nền tảng mã nguồn mở để tự động hóa việc triển khai, mở rộng và quản lý các ứng dụng container. Nó giúp bạn chạy nhiều container trên nhiều máy chủ một cách dễ dàng và hiệu quả.

## 1. K8s là gì?

Hãy tưởng tượng bạn có một nhà hàng (ứng dụng của bạn) và bạn cần nhiều đầu bếp (container) để nấu ăn. Docker giúp bạn tạo ra những đầu bếp này, nhưng ai sẽ quản lý họ? Ai sẽ đảm bảo rằng luôn có đủ đầu bếp khi khách đông? Ai sẽ thay thế nếu một đầu bếp bị ốm? Kubernetes chính là người quản lý nhà hàng đó, đảm bảo mọi thứ hoạt động trơn tru.

| | Analogy | Trong thực tế |
|--|---------|---------------|
| **Docker** | Đầu bếp nấu một món | Chạy một container |
| **Docker Compose** | Bếp trưởng điều phối cả bếp trên **1 bàn làm việc** | Chạy nhiều container trên 1 máy |
| **Kubernetes** | **Chuỗi nhà hàng** tự động mở chi nhánh mới khi đông khách, tự đóng cửa chi nhánh hỏng | Tự động quản lý container trên nhiều máy |

---

## 2. So sánh Docker Compose vs Kubernetes

Mọi thứ trong `docker-compose.yml` đều có tương đương trong K8s:

| Docker Compose | Kubernetes | Ý nghĩa |
|----------------|------------|---------|
| `services:` | **Deployment** | Khai báo "tôi muốn chạy cái này" |
| Mỗi container đang chạy | **Pod** | Đơn vị nhỏ nhất đang thực sự chạy |
| `ports: "8080:8080"` | **Service** | Cho phép truy cập vào Pod từ bên ngoài |
| `volumes:` | **PersistentVolumeClaim (PVC)** | Lưu data không bị mất khi restart |
| `environment:` (giá trị thường) | **ConfigMap** | Lưu config không nhạy cảm |
| `environment:` (password, token) | **Secret** | Lưu thông tin nhạy cảm, được mã hóa |
| `networks:` | **Namespace** | Nhóm các tài nguyên lại với nhau |
| `depends_on:` + init container | **InitContainer** | Chờ service khác sẵn sàng trước |
| `docker compose up` (một lần) | **Job** | Tác vụ chạy một lần rồi kết thúc |

---

## 3. Cấu trúc thư mục `k8s/`

```
k8s/
├── namespace.yaml          ← Tạo "không gian làm việc" tên "marketing"
├── secrets/                ← Mật khẩu, connection strings
│   ├── airflow-secret.yaml
│   ├── clickhouse-secret.yaml
│   ├── minio-secret.yaml
│   └── postgres-secret.yaml
├── configmaps/             ← Config không nhạy cảm
│   ├── airflow-configmap.yaml
│   ├── clickhouse-configmap.yaml
│   ├── kafka-configmap.yaml
│   └── superset-configmap.yaml
├── pvc/                    ← Yêu cầu lưu trữ persistent
│   ├── clickhouse-pvc.yaml   (5GB)
│   ├── minio-pvc.yaml        (10GB)
│   └── superset-pvc.yaml     (1GB)
├── deployments/            ← Khai báo các service cần chạy
│   ├── postgres-deployment.yaml
│   ├── clickhouse-deployment.yaml
│   ├── kafka-deployment.yaml
│   ├── minio-deployment.yaml
│   ├── spark-master-deployment.yaml
│   ├── spark-worker-deployment.yaml
│   ├── airflow-scheduler-deployment.yaml
│   ├── airflow-webserver-deployment.yaml
│   └── superset-deployment.yaml
├── services/               ← Expose các deployment ra ngoài
│   ├── postgres-service.yaml
│   ├── clickhouse-service.yaml
│   ├── kafka-service.yaml
│   ├── minio-service.yaml
│   ├── spark-master-service.yaml
│   ├── airflow-service.yaml
│   └── superset-service.yaml
└── jobs/                   ← Tác vụ khởi tạo một lần
    ├── airflow-init-job.yaml   ← Tạo DB schema + admin user
    └── minio-init-job.yaml     ← Tạo bucket "marketing-datalake"
```

**Quy tắc apply theo thứ tự:** namespace → secrets → configmaps → pvc → deployments → services → jobs

---

## 4. Sơ đồ kiến trúc hệ thống

```
                    ┌────────────────────────────────────────┐
                    │           Namespace: marketing         │
                    │                                        │
  Raw JSON files    │   ┌──────────┐     ┌──────────────┐    │
  (mock data)  ─────┼──►│  Kafka   │────►│    Spark     │    │
                    │   │(broker)  │     │  (consumer)  │    │
                    │   └──────────┘     └──────┬───────┘    │
                    │                           │            │
                    │                    ┌──────▼───────┐    │
                    │                    │   ClickHouse │    │
                    │                    │  (warehouse) │    │
                    │                    └──────┬───────┘    │
                    │                           │            │
                    │   ┌──────────┐     ┌──────▼───────┐    │
                    │   │  MinIO   │     │   Superset   │    │
                    │   │(datalake)│     │(dashboard UI)│    │
                    │   └──────────┘     └──────────────┘    │
                    │                                        │
                    │   ┌──────────────────────────────────┐ │
                    │   │           Airflow                │ │
                    │   │  (điều phối toàn bộ pipeline)    │ │
                    │   │  Webserver (UI) + Scheduler      │ │
                    │   └──────────────────────────────────┘ │
                    │                                        │
                    │   ┌──────────┐   ┌──────────────────┐  │
                    │   │ Postgres │   │   Spark Master   │  │
                    │   │(metadata │   │   + Worker       │  │
                    │   │ Airflow) │   │                  │  │
                    │   └──────────┘   └──────────────────┘  │
                    └────────────────────────────────────────┘
```

---

## 5. Vai trò của từng service

| Service | Vai trò trong dự án |
|---------|---------------------|
| **Kafka** | "Đường ống dữ liệu" — nhận dữ liệu thô từ script mock, đưa vào topic để Spark xử lý |
| **Spark Master** | Não bộ của cụm Spark, điều phối công việc xử lý dữ liệu |
| **Spark Worker** | Tay chân của Spark, thực sự đọc từ Kafka, xử lý, và ghi vào ClickHouse + MinIO |
| **ClickHouse** | Database lưu dữ liệu đã xử lý, phục vụ query nhanh cho Superset |
| **MinIO** | Kho lưu trữ file dạng Parquet (như S3 local) — backup dữ liệu từ Spark |
| **Airflow Webserver** | Giao diện web để xem và quản lý các DAG (pipeline) |
| **Airflow Scheduler** | Chạy ngầm, trigger DAG đúng lịch |
| **Postgres** | Database lưu metadata của Airflow (lịch sử chạy, trạng thái task...) |
| **Superset** | Dashboard visualization — kết nối ClickHouse để vẽ biểu đồ |

---

## 6. Thứ tự khởi động (tại sao quan trọng)

K8s không giống Docker Compose ở chỗ: các Pod khởi động **gần như cùng lúc**, nhưng một số service phụ thuộc vào service khác.

```
Giai đoạn 1 (khởi động tự do, không phụ thuộc):
  Postgres ─┐
  Kafka     ├── Tất cả khởi động cùng lúc
  ClickHouse│
  MinIO     ┘
  Spark Master

Giai đoạn 2 (chờ Giai đoạn 1 xong):
  airflow-init-job  ← Chờ Postgres healthy → tạo DB, tạo user admin
  minio-init-job    ← Chờ MinIO healthy → tạo bucket

Giai đoạn 3 (chờ Giai đoạn 2 xong):
  Airflow Webserver  ─┐ Có InitContainer chờ Postgres
  Airflow Scheduler  ─┘
  Spark Worker         ← Tự kết nối Spark Master
  Superset             ← Khởi động độc lập
```

**InitContainer** là cơ chế của K8s để "chờ". Xem trong file `k8s/deployments/airflow-webserver-deployment.yaml`:

```yaml
initContainers:
  - name: wait-for-postgres
    image: postgres:13
    command: ['sh', '-c', 'until pg_isready -h postgres -U airflow; do sleep 2; done']
```

Dòng này nghĩa là: "Trước khi chạy Airflow, hãy thử kết nối Postgres mỗi 2 giây cho đến khi thành công."

---

## 7. Các lệnh kubectl hay dùng nhất

Tất cả lệnh đều thêm `-n marketing` vì dự án dùng namespace `marketing`.

```bash
# Xem tất cả Pod đang chạy
kubectl get pods -n marketing

# Xem real-time (tự cập nhật)
kubectl get pods -n marketing -w

# Xem logs của một Pod (thay <tên-pod> bằng tên thực từ lệnh trên)
kubectl logs -n marketing <tên-pod>

# Xem logs real-time
kubectl logs -n marketing <tên-pod> -f

# Khi Pod bị lỗi, xem chi tiết nguyên nhân
kubectl describe pod -n marketing <tên-pod>

# "SSH" vào trong container (như docker exec)
kubectl exec -it -n marketing <tên-pod> -- bash

# Xem tất cả services (ports đang expose)
kubectl get services -n marketing

# Xem tất cả mọi thứ cùng lúc
kubectl get all -n marketing
```

**Shortcut trong Makefile của dự án:**

```bash
make k8s-up       # Deploy toàn bộ
make k8s-down     # Xóa toàn bộ
make k8s-status   # = kubectl get all -n marketing
make k8s-logs app=airflow-webserver   # Xem log theo app label
```

---

## 8. Hiểu một file Deployment (ví dụ thực tế)

Đây là file `k8s/deployments/postgres-deployment.yaml` được giải thích từng dòng:

```yaml
apiVersion: apps/v1
kind: Deployment          # Loại tài nguyên: Deployment
metadata:
  name: postgres          # Tên deployment
  namespace: marketing    # Thuộc namespace nào
spec:
  replicas: 1             # Chạy 1 Pod (1 instance)
  selector:
    matchLabels:
      app: postgres       # Deployment này quản lý Pod nào (bằng label)
  template:               # Đây là "khuôn" để tạo Pod
    metadata:
      labels:
        app: postgres     # Label của Pod (phải khớp matchLabels trên)
    spec:
      containers:
        - name: postgres
          image: postgres:13       # Docker image (giống docker-compose)
          ports:
            - containerPort: 5432  # Port bên trong container
          envFrom:
            - secretRef:
                name: postgres-secret  # Đọc biến môi trường từ Secret
          resources:
            requests:              # "Tôi cần tối thiểu..."
              memory: "256Mi"
              cpu: "250m"
            limits:                # "Tôi không được dùng quá..."
              memory: "512Mi"
              cpu: "500m"
```

> **`250m` CPU nghĩa là gì?** `m` = milli-core. `1000m = 1 CPU core`. Nên `250m` = 1/4 core.

---

## 9. Secret và ConfigMap khác gì nhau?

Cả hai đều lưu "biến môi trường", nhưng:

| | ConfigMap | Secret |
|--|-----------|--------|
| **Dùng cho** | Config thông thường | Password, token, connection string |
| **Được mã hóa?** | Không | Có (base64 encode) |
| **Ví dụ trong dự án** | `KAFKA_BOOTSTRAP_SERVERS: kafka:29092` | `POSTGRES_PASSWORD: airflow` |

Trong file YAML, bạn sẽ thấy 2 cách khác nhau để đọc:
```yaml
# Đọc toàn bộ ConfigMap
envFrom:
  - configMapRef:
      name: airflow-config

# Đọc một key cụ thể từ Secret
env:
  - name: MY_PASSWORD
    valueFrom:
      secretKeyRef:
        name: postgres-secret
        key: POSTGRES_PASSWORD
```
