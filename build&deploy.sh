cd ~/cloudcontinuum-orchestrator

# Build
make build

# Verifica che compili senza errori
# Output atteso:
# go fmt ./...
# go vet ./...
# go build -o bin/manager cmd/main.go

# Build Docker image
export DOCKER_USERNAME=tuousername
make docker-build IMG=$DOCKER_USERNAME/cloudcontinuum-orchestrator:v1.0.5

# Push
make docker-push IMG=$DOCKER_USERNAME/cloudcontinuum-orchestrator:v1.0.5

# Update deployment
kubectl --context=cloud_cluster set image deployment/cloudcontinuum-orchestrator-controller-manager \
  -n cloudcontinuum-orchestrator-system \
  manager=$DOCKER_USERNAME/cloudcontinuum-orchestrator:v1.0.5

# Wait for rollout
kubectl --context=cloud_cluster rollout status deployment/cloudcontinuum-orchestrator-controller-manager \
  -n cloudcontinuum-orchestrator-system


kubectl apply -f examples/pipelineplacementrequest-inline.yaml

# Watch i log
kubectl --context=cloud_cluster logs -f deployment/cloudcontinuum-orchestrator-controller-manager \
  -n cloudcontinuum-orchestrator-system

kubectl get pipelineplacementrequest simple-request -n kubeflow -o yaml | grep -A 10 status