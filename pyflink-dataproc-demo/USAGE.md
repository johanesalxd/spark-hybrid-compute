# PyFlink on Dataproc - Usage Guide

This guide provides step-by-step instructions for running the PyFlink on Dataproc demo.

## Prerequisites

1. **Google Cloud SDK** installed and configured
2. **GCP Project** with Dataproc API enabled
3. **Billing** enabled on your GCP project
4. **IAM Permissions**:
   - Dataproc Admin
   - Storage Admin
   - Compute Admin

## Quick Start

### 1. Configure Environment Variables

```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export CLUSTER_NAME="pyflink-demo-cluster"
export BUCKET_NAME="${PROJECT_ID}-pyflink-demo"
```

### 2. Create Dataproc Cluster with Flink

```bash
cd cluster/
./create-flink-cluster.sh
```

This will:
- Create a GCS bucket for the demo
- Create a Dataproc cluster with Flink component enabled
- Configure YARN for optimal Flink performance

### 3. Upload Jobs and Data to GCS

```bash
cd ../submit/
./upload-to-gcs.sh
```

This will:
- Upload PyFlink job files to GCS
- Upload sample data files
- Note: No external dependencies needed - using built-in PyFlink libraries

### 4. Submit PyFlink Job

The demo uses SSH wrapper approach since `gcloud dataproc jobs submit flink` doesn't support PyFlink.

#### CSV Processing Job
```bash
./submit-pyflink.sh
```

Or explicitly:
```bash
./submit-pyflink.sh csv_processor
```

**Note**: Jobs are submitted via `gcloud compute ssh` + `flink run -m yarn-cluster` for full PyFlink support.

### 5. Monitor Jobs

#### Check Job Status
```bash
gcloud dataproc jobs list --region=${REGION} --filter="status.state=ACTIVE"
```

#### View Job Logs
```bash
# Replace JOB_ID with actual job ID from previous command
gcloud dataproc jobs wait JOB_ID --region=${REGION}
```

#### Access Flink Web UI
```bash
# Enable port forwarding
gcloud compute ssh ${CLUSTER_NAME}-m --zone=${REGION}-a -- -L 8081:localhost:8081

# Open http://localhost:8081 in your browser
```

### 6. Check Results

```bash
# List output files
gsutil ls gs://${BUCKET_NAME}/output/

# View CSV processing results
gsutil cat gs://${BUCKET_NAME}/output/csv_results/*
```

### 7. Clean Up

```bash
cd ../cluster/
./delete-cluster.sh

# Optional: Delete GCS bucket and all data
gsutil -m rm -r gs://${BUCKET_NAME}
```

## Orchestration Integration

For production workflows, you can integrate PyFlink jobs with various orchestration tools:

- **Cloud Workflows**: YAML-based workflow orchestration
- **Cloud Composer (Airflow)**: Full-featured workflow management
- **Cloud Scheduler**: Simple cron-like scheduling
- **Custom solutions**: Cloud Functions, Cloud Run, etc.

The key is to use the SSH wrapper approach for PyFlink job submission within your chosen orchestration framework.

## Troubleshooting

### Common Issues

1. **Cluster Creation Fails**
   - Check quotas in your GCP project
   - Verify Dataproc API is enabled
   - Ensure you have sufficient IAM permissions

2. **Job Submission Fails**
   - Verify cluster is running: `gcloud dataproc clusters list --region=${REGION}`
   - Check if files are uploaded to GCS: `gsutil ls gs://${BUCKET_NAME}/`
   - Review job logs for specific errors

3. **PyFlink Import Errors**
   - These are expected in local development environment
   - PyFlink is pre-installed on Dataproc clusters with Flink component

4. **GCS Access Issues**
   - Verify bucket exists and you have access
   - Check that service account has Storage permissions

### Debugging Commands

```bash
# Check cluster status
gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION}

# SSH into cluster master node
gcloud compute ssh ${CLUSTER_NAME}-m --zone=${REGION}-a

# Check Flink processes on cluster
gcloud compute ssh ${CLUSTER_NAME}-m --zone=${REGION}-a --command="ps aux | grep flink"

# View YARN applications
gcloud compute ssh ${CLUSTER_NAME}-m --zone=${REGION}-a --command="yarn application -list"
```

## Customization

### Modify Job Parameters

Edit the job submission script to change input/output paths:

```bash
# In submit-pyflink.sh, modify the arguments:
-- \
--input gs://${BUCKET_NAME}/your-input-file \
--output gs://${BUCKET_NAME}/your-output-path/
```

### Add New Dependencies

Since this demo uses only built-in PyFlink libraries, no external dependencies are needed. If you need to add custom dependencies in the future:

1. Create a `jobs/requirements.txt` file with your dependencies
2. Update the upload script to include dependency packaging
3. Modify the job submission to include the dependency archive

### Scale Cluster

Modify cluster configuration in `create-flink-cluster.sh`:

```bash
# Increase worker nodes
NUM_WORKERS=4

# Use larger machine types
MACHINE_TYPE="n1-standard-8"
```

## Cost Optimization

1. **Use Preemptible Instances**:
   Add `--preemptible` to cluster creation command

2. **Auto-delete Clusters**:
   Set shorter `--max-idle` time in cluster creation

3. **Right-size Resources**:
   Monitor resource usage and adjust machine types accordingly

## Next Steps

1. **Integrate with BigQuery**: Modify jobs to read/write BigQuery tables
2. **Add Monitoring**: Set up Cloud Monitoring alerts for job failures
3. **CI/CD Pipeline**: Automate job deployment using Cloud Build
4. **Stream Processing**: Adapt jobs for real-time data processing
