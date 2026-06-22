# Deployment Guide

## Prerequisites

- AWS account with appropriate permissions
- Terraform 1.6+
- kubectl 1.27+
- Docker (for local testing)
- AWS CLI v2
- GitHub account (for CI/CD)

## Step 1: Initial Setup

```bash
# Clone repository
git clone https://github.com/niteshme21/Airflow.git
cd Airflow/airflow-enterprise

# Install dependencies
pip install -r requirements.txt

# Verify installation
python -m pytest tests/
```

## Step 2: Infrastructure Deployment with Terraform

```bash
cd terraform

# Initialize Terraform
terraform init

# Set variables
cat > terraform.tfvars << EOF
aws_region              = "ap-south-1"
environment             = "dev"
kubernetes_version      = "1.27"
node_instance_type      = "t3.xlarge"
node_desired_size       = 3
node_max_size           = 10
node_min_size           = 1
db_instance_class       = "db.t3.medium"
db_instance_count       = 2
EOF

# Preview changes
terraform plan

# Deploy infrastructure
terraform apply

# Get outputs
terraform output eks_cluster_name
terraform output rds_cluster_endpoint
```

## Step 3: Configure kubectl

```bash
# Get cluster name
CLUSTER_NAME=$(terraform output -raw eks_cluster_name)

# Update kubeconfig
aws eks update-kubeconfig \
  --name $CLUSTER_NAME \
  --region ap-south-1

# Verify connection
kubectl get nodes
```

## Step 4: Deploy Airflow to Kubernetes

```bash
# Create namespace
kubectl create namespace airflow-enterprise

# Create secrets
kubectl create secret generic airflow-secrets \
  --from-literal=AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql://airflow:password@rds-endpoint:5432/airflow' \
  -n airflow-enterprise

# Deploy Airflow
kubectl apply -f ../k8s/airflow-deployment.yaml

# Verify deployment
kubectl get pods -n airflow-enterprise
kubectl get svc -n airflow-enterprise
```

## Step 5: Initialize Airflow Database

```bash
# Get pod name
POD=$(kubectl get pods -n airflow-enterprise -l app=airflow-webserver -o jsonpath='{.items[0].metadata.name}')

# Initialize database
kubectl exec -it $POD -n airflow-enterprise -- airflow db init

# Create admin user
kubectl exec -it $POD -n airflow-enterprise -- airflow users create \
  --username admin \
  --password admin123 \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@airflow.com
```

## Step 6: Verify Deployment

```bash
# Port forward to access UI
kubectl port-forward svc/airflow-webserver 8080:80 -n airflow-enterprise

# Access UI
# http://localhost:8080
# Login: admin / admin123

# Check example DAGs are loaded
kubectl exec -it $POD -n airflow-enterprise -- airflow dags list
```

## Step 7: Test Cross-DAG Dependencies

```bash
# Trigger source DAG
kubectl exec -it $POD -n airflow-enterprise -- \
  airflow dags trigger etl_source_data_extraction

# Monitor execution
kubectl exec -it $POD -n airflow-enterprise -- \
  airflow dags list-runs

# Trigger dependent DAG
kubectl exec -it $POD -n airflow-enterprise -- \
  airflow dags trigger analytics_transformation_pipeline

# Monitor dependency checks
kubectl logs -f -n airflow-enterprise deployment/airflow-scheduler
```

## Monitoring Setup

### Prometheus Integration

```bash
# Install Prometheus Operator
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/kube-prometheus-stack \
  -n airflow-enterprise

# Port forward to Prometheus
kubectl port-forward -n airflow-enterprise svc/prometheus-kube-prometheus-prometheus 9090:9090
```

### Grafana Dashboards

```bash
# Access Grafana
kubectl port-forward -n airflow-enterprise svc/prometheus-grafana 3000:80

# Import dashboard JSON files
# Look for pre-built Airflow Enterprise dashboards
```

## CI/CD Setup

### GitHub Secrets Configuration

```bash
# Store AWS credentials
gh secret set AWS_ROLE_DEV --body "arn:aws:iam::ACCOUNT:role/GithubActionsRole"
gh secret set AWS_ROLE_PROD --body "arn:aws:iam::ACCOUNT:role/GithubActionsRole"

# Trigger workflows
gh workflow run ci-cd.yml
```

## Production Deployment Checklist

- [ ] Infrastructure created in prod region (multi-AZ)
- [ ] RDS automated backups enabled
- [ ] Kubernetes auto-scaling configured
- [ ] Monitoring and alerting set up
- [ ] SSL/TLS certificates configured
- [ ] Network security groups hardened
- [ ] RBAC policies applied
- [ ] Disaster recovery tested
- [ ] Load testing completed
- [ ] Documentation reviewed

## Environment-Specific Configuration

### Dev Environment

```bash
terraform apply -var-file=terraform.dev.tfvars
```

### Staging Environment

```bash
terraform apply -var-file=terraform.staging.tfvars
```

### Production Environment

```bash
# Always run plan first in prod
terraform plan -var-file=terraform.prod.tfvars

# Review carefully before apply
terraform apply -var-file=terraform.prod.tfvars
```

## Rollback Procedures

### Kubernetes Rollback

```bash
# View rollout history
kubectl rollout history deployment/airflow-webserver -n airflow-enterprise

# Rollback to previous version
kubectl rollout undo deployment/airflow-webserver -n airflow-enterprise

# Rollback to specific revision
kubectl rollout undo deployment/airflow-webserver --to-revision=2 -n airflow-enterprise
```

### Terraform Rollback

```bash
# Rollback to previous state
terraform state pull > backup-state.json
git checkout HEAD~1 terraform/

# Reapply with previous code
terraform apply -var-file=terraform.tfvars
```

## Troubleshooting

### Pod Not Starting

```bash
# Check pod status
kubectl describe pod POD_NAME -n airflow-enterprise

# Check logs
kubectl logs POD_NAME -n airflow-enterprise
```

### Database Connection Issues

```bash
# Test RDS connectivity
kubectl run -it --rm debug --image=ubuntu:latest --restart=Never -- bash
apt-get update && apt-get install -y postgresql-client
psql -h RDS_ENDPOINT -U airflow -d airflow
```

### Scheduler Not Processing DAGs

```bash
# Check scheduler logs
kubectl logs -f deployment/airflow-scheduler -n airflow-enterprise

# Check database for blocked tasks
kubectl exec POD -- airflow tasks clear
```

## Performance Tuning

### Database Connection Pooling

Update `k8s/airflow-deployment.yaml`:
```yaml
env:
  - name: AIRFLOW__DATABASE__SQL_POOL_SIZE
    value: "10"
  - name: AIRFLOW__DATABASE__SQL_MAX_OVERFLOW
    value: "20"
```

### Scheduler Configuration

```yaml
env:
  - name: AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL
    value: "300"
  - name: AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT
    value: "False"
```

## Cost Optimization

- Use spot instances for non-critical workloads
- Enable S3 bucket lifecycle policies for logs
- Configure RDS backup retention
- Use CloudWatch log groups efficiently
- Scale nodes based on demand

## Maintenance

### Regular Backups

```bash
# Backup RDS
aws rds create-db-cluster-snapshot \
  --db-cluster-identifier airflow-cluster \
  --db-cluster-snapshot-identifier airflow-backup-$(date +%Y%m%d)
```

### Update Airflow

```bash
# Update requirements
pip install --upgrade apache-airflow

# Rebuild Docker image
docker build -t airflow-enterprise:latest .

# Redeploy
kubectl set image deployment/airflow-webserver \
  webserver=airflow-enterprise:latest \
  -n airflow-enterprise
```

## Support

For issues or questions:
1. Check logs: `kubectl logs -f deployment/airflow-scheduler -n airflow-enterprise`
2. Review Prometheus metrics
3. Check AWS CloudTrail for infrastructure issues
4. Open GitHub issue with full context
