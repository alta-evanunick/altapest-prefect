# Azure Container Instance Push Work Pool Setup Guide

## Prerequisites
1. Azure subscription with Container Instances enabled
2. Azure Service Principal with required permissions
3. Docker image for your flows

## Step 1: Create Azure Service Principal

```bash
# Create service principal
az ad sp create-for-rbac \
  --name "prefect-aci-sp" \
  --role Contributor \
  --scopes /subscriptions/{subscription-id}
```

Save the output - you'll need:
- `appId` (Client ID)
- `password` (Client Secret)
- `tenant` (Tenant ID)

## Step 2: Create Prefect Work Pool

```bash
# Create push work pool
prefect work-pool create altapest-aci-pool \
  --type azure-container-instance:push \
  --set-as-default
```

## Step 3: Configure Work Pool

In Prefect UI:
1. Go to Work Pools → altapest-aci-pool → Edit
2. Configure:
   - **subscription_id**: Your Azure subscription ID
   - **resource_group**: Resource group for ACI (create if needed)
   - **tenant_id**: From service principal
   - **client_id**: From service principal  
   - **client_secret**: From service principal (use Secret block)
   - **region**: e.g., "westus2"
   - **cpu**: 2.0 (adjust based on needs)
   - **memory**: 4.0 (GB, adjust based on needs)

## Step 4: Create Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
RUN pip install --no-cache-dir -r prefect prefect-snowflake snowflake-connector-python snowflake-sqlalchemy requests pandas pyarrow pytz orjson tenacity

# Copy flow code
COPY flows/ ./flows/
COPY sql/ ./sql/

# Set Python path
ENV PYTHONPATH=/app

# Prefect will inject the flow run command
```

## Step 5: Build and Push Docker Image

```bash
# Build image
docker build -t altapest-etl:latest .

# Tag for Azure Container Registry (if using ACR)
docker tag altapest-etl:latest {registry}.azurecr.io/altapest-etl:latest

# Push to registry
docker push {registry}.azurecr.io/altapest-etl:latest
```

## Step 6: Update prefect.yaml

```yaml
# Add to each deployment:
work_pool:
  name: altapest-aci-pool
  work_queue_name: default
  job_variables:
    image: "{registry}.azurecr.io/altapest-etl:latest"
    # Optional: Override CPU/memory per deployment
    cpu: 2.0
    memory: 4.0
```

## Alternative: Local Worker (Simpler Start)

If Azure setup is too complex initially, run a local worker:

```bash
# Start local worker
prefect worker start --pool default-work-pool

# Keep it running (use screen/tmux or run as service)
```

This uses your local machine's resources but avoids Azure complexity.

## Troubleshooting Azure Issues

### Common Problems:
1. **Authentication failures**: Double-check service principal permissions
2. **Resource limits**: Check Azure subscription quotas
3. **Network issues**: Ensure ACI can reach Snowflake/FieldRoutes
4. **Image pull errors**: Verify registry credentials

### Debug Commands:
```bash
# Test Azure CLI auth
az account show

# List container instances
az container list --resource-group {rg-name}

# Check container logs
az container logs --name {container-name} --resource-group {rg-name}
```

## Cost Optimization

- ACI charges per second of execution
- Your workload (3 AM daily + 2-hour CDC) = ~25 hours/month
- Estimated cost: ~$30-50/month with 2 vCPU, 4GB RAM
- Compare to Prefect Cloud worker upgrade cost

## Recommendation Path

1. **Start with local worker** to avoid immediate Azure complexity
2. **Test flows work properly** in your environment  
3. **Then migrate to ACI** once stable
4. **Use Push work pool** for simplicity

This staged approach reduces risk and complexity.
