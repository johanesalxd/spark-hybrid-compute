#!/bin/bash

# Submit PyFlink jobs to Dataproc using SSH wrapper
# This script uses the SSH approach since gcloud dataproc submit flink doesn't support PyFlink

set -e

# Configuration - Update these values for your project
PROJECT_ID="${PROJECT_ID:-your-project-id}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-us-central1-a}"
CLUSTER_NAME="${CLUSTER_NAME:-pyflink-demo-cluster}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-pyflink-demo}"

# Job configuration
JOB_TYPE="${1:-csv_processor}"  # csv_processor
JOB_NAME="pyflink-${JOB_TYPE}-$(date +%Y%m%d-%H%M%S)"

echo "Submitting PyFlink job to Dataproc via SSH..."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Zone: ${ZONE}"
echo "Cluster: ${CLUSTER_NAME}"
echo "Job Type: ${JOB_TYPE}"
echo "Job Name: ${JOB_NAME}"

# Check if cluster exists
if ! gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID} >/dev/null 2>&1; then
    echo "Error: Cluster '${CLUSTER_NAME}' not found in region '${REGION}'"
    echo "Please create the cluster first using: cd ../cluster && ./create-flink-cluster.sh"
    exit 1
fi

# Submit job based on type using SSH wrapper
case ${JOB_TYPE} in
    "csv_processor")
        echo "Submitting CSV Processor job via SSH..."
        gcloud compute ssh ${CLUSTER_NAME}-m \
            --zone=${ZONE} \
            --project=${PROJECT_ID} \
            --command="
                # Download the Python file from GCS to local filesystem
                gsutil cp gs://${BUCKET_NAME}/jobs/csv_processor.py /tmp/csv_processor.py

                # Submit the job using local file path (no external dependencies needed)
                flink run -m yarn-cluster \
                    -yD yarn.application.name='${JOB_NAME}' \
                    -yD execution.runtime-mode=BATCH \
                    -py /tmp/csv_processor.py \
                    --input gs://${BUCKET_NAME}/data/sample.csv \
                    --output gs://${BUCKET_NAME}/output/csv_results/
            "
        ;;

    *)
        echo "Error: Unknown job type '${JOB_TYPE}'"
        echo "Usage: $0 [csv_processor]"
        echo ""
        echo "Examples:"
        echo "  $0                 # Submit CSV processing job (default)"
        echo "  $0 csv_processor   # Submit CSV processing job"
        exit 1
        ;;
esac

echo ""
echo "Job '${JOB_NAME}' submitted successfully via SSH!"
echo ""
echo "Monitor job progress:"
echo "  # Check YARN applications"
echo "  gcloud compute ssh ${CLUSTER_NAME}-m --zone=${ZONE} --command='yarn application -list'"
echo ""
echo "  # Check Flink jobs"
echo "  gcloud compute ssh ${CLUSTER_NAME}-m --zone=${ZONE} --command='flink list'"
echo ""
echo "View job logs:"
echo "  # Get application ID from YARN list, then:"
echo "  gcloud compute ssh ${CLUSTER_NAME}-m --zone=${ZONE} --command='yarn logs -applicationId <app-id>'"
echo ""
echo "Check output in GCS:"
echo "  gsutil ls gs://${BUCKET_NAME}/output/"
echo ""
echo "Access Flink Web UI:"
echo "  # Via Dataproc Component Gateway or port forwarding:"
echo "  gcloud compute ssh ${CLUSTER_NAME}-m --zone=${ZONE} -- -L 8081:localhost:8081"
echo "  # Then open http://localhost:8081 in your browser"
