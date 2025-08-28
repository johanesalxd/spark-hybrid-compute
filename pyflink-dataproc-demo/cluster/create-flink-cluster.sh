#!/bin/bash

# PyFlink DataProc Cluster Creation Script
# This script creates a DataProc cluster with Flink component enabled

set -e

# Configuration - Update these values for your project
PROJECT_ID="${PROJECT_ID:-your-project-id}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
CLUSTER_NAME="${CLUSTER_NAME:-pyflink-demo-cluster}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-pyflink-demo}"

# Cluster configuration
MACHINE_TYPE="n1-standard-4"
NUM_WORKERS=2
DISK_SIZE="100GB"
IMAGE_VERSION="2.2-debian12"

echo "Creating PyFlink DataProc cluster..."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Cluster: ${CLUSTER_NAME}"
echo "Bucket: ${BUCKET_NAME}"

# Create GCS bucket if it doesn't exist
echo "Creating GCS bucket if it doesn't exist..."
gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://${BUCKET_NAME} 2>/dev/null || echo "Bucket already exists or creation failed"

# Create the DataProc cluster with Flink component
echo "Creating DataProc cluster with Flink component..."
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --zone=${ZONE} \
    --num-masters=1 \
    --num-workers=${NUM_WORKERS} \
    --worker-machine-type=${MACHINE_TYPE} \
    --master-machine-type=${MACHINE_TYPE} \
    --worker-boot-disk-size=${DISK_SIZE} \
    --master-boot-disk-size=${DISK_SIZE} \
    --image-version=${IMAGE_VERSION} \
    --optional-components=FLINK \
    --enable-component-gateway \
    --bucket=${BUCKET_NAME} \
    --properties="yarn:yarn.scheduler.maximum-allocation-mb=14336,yarn:yarn.nodemanager.resource.memory-mb=14336,yarn:yarn.scheduler.maximum-allocation-vcores=4" \
    --max-idle=60m

echo "Cluster '${CLUSTER_NAME}' created successfully!"
echo ""
echo "Cluster details:"
gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --format="value(status.state,config.gceClusterConfig.zoneUri.scope(zones))"

echo ""
echo "To access Flink Web UI, go to Web Interfaces on Dataproc Clusters console"

echo ""
echo "To submit PyFlink jobs, use the scripts in the submit/ directory"
