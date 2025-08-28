#!/bin/bash

# PyFlink Dataproc Cluster Deletion Script
# This script deletes the Dataproc cluster to avoid ongoing charges

set -e

# Configuration - Update these values for your project
PROJECT_ID="${PROJECT_ID:-your-project-id}"
REGION="${REGION:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-pyflink-demo-cluster}"

echo "Deleting PyFlink Dataproc cluster..."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Cluster: ${CLUSTER_NAME}"

# Delete the Dataproc cluster
echo "Deleting Dataproc cluster..."
gcloud dataproc clusters delete ${CLUSTER_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --quiet

echo "Cluster '${CLUSTER_NAME}' deleted successfully!"
echo ""
echo "Note: GCS bucket and data are preserved."
echo "To delete the bucket and all data, run:"
echo "  gsutil -m rm -r gs://\${BUCKET_NAME}"
