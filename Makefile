.PHONY: k8s-up k8s-down k8s-status k8s-logs compose-up compose-down speed-layer speed-layer-logs

# --- Kubernetes (minikube) ---

k8s-up:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmaps/
	kubectl apply -f k8s/secrets/
	kubectl apply -f k8s/pvc/
	kubectl apply -f k8s/deployments/
	kubectl apply -f k8s/services/
	kubectl apply -f k8s/jobs/

k8s-down:
	kubectl delete namespace marketing

k8s-status:
	kubectl get all -n marketing

k8s-logs:
	kubectl logs -n marketing -l app=$(app) --tail=100

airflow-ui:
	minikube service airflow -n marketing

superset-ui:
	minikube service superset -n marketing

minio-ui:
	minikube service minio -n marketing

spark-ui:
	minikube service spark-master -n marketing

kafka-connect-ui:
	minikube service kafka-connect -n marketing

clickhouse-ui:
	minikube service clickhouse -n marketing

# --- Speed Layer (Spark Structured Streaming) ---

speed-layer:
	kubectl rollout restart deployment/speed-layer -n marketing

speed-layer-logs:
	kubectl logs -n marketing -l app=speed-layer --tail=100 -f

# --- Docker Compose (local dev) ---

build:
	docker build -f Dockerfile.airflow -t mkt_airflow:latest .
	docker build -f Dockerfile.superset -t mkt_superset:latest .
	docker build -f Dockerfile.clickhouse -t mkt_clickhouse:latest .
	minikube image load mkt_airflow:latest
	minikube image load mkt_superset:latest
	minikube image load mkt_clickhouse:latest

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down -v

compose-logs:
	docker compose logs -f $(service)
