.PHONY: k8s-up k8s-down k8s-status k8s-logs compose-up compose-down

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

# --- Docker Compose (local dev) ---

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down -v

compose-logs:
	docker compose logs -f $(service)
