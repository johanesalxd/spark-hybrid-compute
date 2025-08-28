#!/bin/bash

# Upload PyFlink jobs and data to GCS
# This script uploads all necessary files to GCS for job submission

set -e

# Configuration - Update these values for your project
PROJECT_ID="${PROJECT_ID:-your-project-id}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-pyflink-demo}"

echo "Uploading PyFlink jobs and data to GCS..."
echo "Bucket: gs://${BUCKET_NAME}"

# Create bucket if it doesn't exist
echo "Creating GCS bucket if it doesn't exist..."
gsutil mb -p ${PROJECT_ID} gs://${BUCKET_NAME} 2>/dev/null || echo "Bucket already exists or creation failed"

# Upload job files
echo "Uploading PyFlink job files..."
gsutil cp ../jobs/csv_processor.py gs://${BUCKET_NAME}/jobs/

# Upload sample data
echo "Uploading sample data files..."
gsutil cp ../data/sample.csv gs://${BUCKET_NAME}/data/

# Note: No external dependencies needed - using only built-in PyFlink libraries
echo "Note: No external dependencies needed - using built-in PyFlink and Hadoop GCS connector"

echo "Upload completed successfully!"
echo ""
echo "Uploaded files:"
echo "- Jobs: gs://${BUCKET_NAME}/jobs/"
echo "- Data: gs://${BUCKET_NAME}/data/"
echo ""
echo "You can now submit PyFlink jobs using submit-pyflink.sh"
