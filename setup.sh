eval $(minikube docker-env)
docker build -t geo-service:0.1 .
kubectl apply -f config-map.yaml
kubectl apply -f deployment.yaml
kubectl apply -f cluster-ip.yaml