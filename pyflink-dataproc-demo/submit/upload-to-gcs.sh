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
gsutil cp ../jobs/word_count.py gs://${BUCKET_NAME}/jobs/
gsutil cp ../jobs/csv_processor.py gs://${BUCKET_NAME}/jobs/
gsutil cp ../jobs/requirements.txt gs://${BUCKET_NAME}/jobs/

# Upload sample data
echo "Uploading sample data files..."
gsutil cp ../data/sample.txt gs://${BUCKET_NAME}/data/
gsutil cp ../data/sample.csv gs://${BUCKET_NAME}/data/

# Create Python dependencies archive if requirements.txt exists
if [ -f "../jobs/requirements.txt" ]; then
    echo "Creating Python dependencies archive..."
    cd ../jobs
    pip install -r requirements.txt --target ./deps --quiet
    zip -r deps.zip deps/ > /dev/null
    gsutil cp deps.zip gs://${BUCKET_NAME}/deps/
    rm -rf deps/ deps.zip
    cd ../submit
fi

echo "Upload completed successfully!"
echo ""
echo "Uploaded files:"
echo "- Jobs: gs://${BUCKET_NAME}/jobs/"
echo "- Data: gs://${BUCKET_NAME}/data/"
echo "- Dependencies: gs://${BUCKET_NAME}/deps/"
echo ""
echo "You can now submit PyFlink jobs using submit-pyflink.sh"
