# PyFlink on DataProc - Deployment Patterns Guide

This guide covers different deployment patterns for PyFlink on Google Cloud DataProc, helping you choose the right approach for your use case.

## Overview of Patterns

| Pattern | Use Case | Cluster Lifecycle | YARN Mode | Cost | Complexity |
|---------|----------|-------------------|-----------|------|------------|
| **Ephemeral Batch** | ETL, Reports, ML Training | Create → Run → Delete | Per-job | Low | Simple |
| **Long-running Stream** | Real-time processing | Persistent | Session/Application | Medium | Medium |
| **Interactive Analysis** | Data exploration | Persistent | Session | Medium | Medium |
| **Production Workflow** | Scheduled jobs | Orchestrated | Per-job | Low | High |

---

## Pattern 1: Ephemeral Batch Processing (Recommended for Most Cases)

**Best for:** ETL jobs, daily reports, ML training, data transformations

### Architecture
```
Create Cluster → Submit PyFlink Job → Auto-delete Cluster
```

### Implementation
```bash
#!/bin/bash
# ephemeral-batch.sh

# Create cluster with auto-delete
gcloud dataproc clusters create batch-${JOB_ID} \
    --optional-components=FLINK \
    --max-idle=10m \
    --max-age=2h \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4

# Submit PyFlink job via SSH
gcloud compute ssh batch-${JOB_ID}-m --zone=${ZONE} --command="
    flink run -m yarn-cluster \
        -yD yarn.application.name='PyFlink-${JOB_ID}' \
        -yD execution.runtime-mode=BATCH \
        -py gs://${BUCKET}/jobs/etl_job.py \
        --input gs://${BUCKET}/input/ \
        --output gs://${BUCKET}/output/
"

# Cluster auto-deletes after job completion + idle time
```

### Pros
- **Cost-effective**: Pay only for job duration
- **Isolated**: Fresh environment for each job
- **Scalable**: Easy to parallelize multiple jobs
- **Maintenance-free**: No cluster management

### Cons
- **Startup overhead**: ~2-3 minutes cluster creation
- **No job sharing**: Can't reuse cluster for multiple jobs

### When to Use
- Scheduled ETL pipelines
- Daily/weekly reports
- ML model training
- Data migration jobs

---

## Pattern 2: Long-running Streaming Cluster

**Best for:** Real-time data processing, event streaming, continuous analytics

### Architecture
```
Create Persistent Cluster → Submit Multiple Streaming Jobs → Manual Management
```

### Implementation
```bash
#!/bin/bash
# streaming-cluster.sh

# Create long-running cluster
gcloud dataproc clusters create streaming-cluster \
    --optional-components=FLINK \
    --num-workers=4 \
    --worker-machine-type=n1-standard-8 \
    --enable-autoscaling \
    --max-workers=10 \
    --secondary-workers=2 \
    --preemptible

# Start Flink session for multiple jobs
gcloud compute ssh streaming-cluster-m --zone=${ZONE} --command="
    yarn-session.sh -d \
        -n 4 \
        -tm 4096 \
        -jm 2048 \
        -nm 'Streaming Session'
"

# Submit streaming jobs to the session
gcloud compute ssh streaming-cluster-m --zone=${ZONE} --command="
    flink run -py gs://${BUCKET}/jobs/kafka_processor.py \
        --kafka-servers ${KAFKA_SERVERS} \
        --output-topic processed-events
"

gcloud compute ssh streaming-cluster-m --zone=${ZONE} --command="
    flink run -py gs://${BUCKET}/jobs/anomaly_detector.py \
        --input-topic sensor-data \
        --alert-endpoint ${ALERT_URL}
"
```

### Pros
- **Resource sharing**: Multiple jobs on same cluster
- **Low latency**: No cluster startup time
- **Stateful processing**: Maintains state across restarts
- **Cost optimization**: Can use preemptible instances

### Cons
- **Higher cost**: Cluster runs continuously
- **Management overhead**: Monitoring, maintenance
- **Resource contention**: Jobs compete for resources

### When to Use
- Kafka/Pub/Sub stream processing
- Real-time analytics dashboards
- Fraud detection systems
- IoT data processing

---

## Pattern 3: Interactive Analysis Session

**Best for:** Data exploration, ad-hoc analysis, development/testing

### Architecture
```
Create Development Cluster → Interactive Session → Manual Jobs
```

### Implementation
```bash
#!/bin/bash
# interactive-session.sh

# Create development cluster
gcloud dataproc clusters create dev-cluster \
    --optional-components=FLINK,JUPYTER \
    --enable-component-gateway \
    --max-idle=4h \
    --num-workers=2

# Start interactive session
gcloud compute ssh dev-cluster-m --zone=${ZONE} --command="
    yarn-session.sh -d -n 2 -tm 2048 -jm 1024
"

# Submit interactive jobs
gcloud compute ssh dev-cluster-m --zone=${ZONE} --command="
    flink run -py gs://${BUCKET}/analysis/explore_data.py \
        --dataset gs://${BUCKET}/raw-data/ \
        --sample-size 1000
"
```

### Jupyter Integration
```python
# In Jupyter notebook on DataProc
import subprocess

def submit_pyflink_job(job_file, **kwargs):
    args = [f"--{k} {v}" for k, v in kwargs.items()]
    cmd = f"flink run -py {job_file} {' '.join(args)}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout
```

### When to Use
- Data science workflows
- Algorithm development
- Quick data exploration
- Testing new job logic

---

## Pattern 4: Production Workflow Orchestration

**Best for:** Production pipelines, complex dependencies, scheduled jobs

### Orchestration Options

For production workflows, you can use various orchestration tools:

**Cloud Workflows**: Simple YAML-based workflow orchestration
**Cloud Composer (Airflow)**: Full-featured workflow management
**Cloud Scheduler**: Simple cron-like scheduling
**Custom solutions**: Using Cloud Functions, Cloud Run, etc.

The key is to use the SSH wrapper approach for PyFlink job submission within your chosen orchestration framework.

---

## PyFlink Job Submission Methods

Since `gcloud dataproc jobs submit flink` only supports JAR files (not PyFlink), we need alternative approaches for submitting PyFlink jobs to DataProc.

### Method 1: SSH Wrapper (Recommended)

**Implementation:**
```bash
gcloud compute ssh ${CLUSTER_NAME}-m --zone=${ZONE} --command="
    # Download Python file from GCS to local filesystem
    gsutil cp gs://${BUCKET}/jobs/job.py /tmp/job.py

    # Submit job using local file path
    flink run -m yarn-cluster \
        -yD yarn.application.name='my-job' \
        -yD execution.runtime-mode=BATCH \
        -py /tmp/job.py \
        --input gs://${BUCKET}/input \
        --output gs://${BUCKET}/output
"
```

**Pros:**
- ✅ **Full Flink functionality**: Access to all Flink CLI options and parameters
- ✅ **YARN integration**: Jobs appear in Flink History Server and YARN Resource Manager
- ✅ **Direct execution**: No wrapper layers or compatibility issues
- ✅ **Error handling**: Clear error messages and debugging information
- ✅ **Resource control**: Fine-grained control over memory, parallelism, etc.
- ✅ **Monitoring**: Native Flink job monitoring and management
- ✅ **Orchestration compatible**: Works with various workflow tools

**Cons:**
- ❌ **Not in DataProc Jobs list**: Jobs don't appear in `gcloud dataproc jobs list`
- ❌ **Manual monitoring**: Need to use YARN/Flink UIs instead of DataProc console
- ❌ **File download overhead**: Need to download Python files from GCS first

**When to Use:**
- Production PyFlink workloads
- Complex jobs with custom dependencies
- When you need full Flink functionality
- Cloud Composer/Airflow orchestration

### Method 2: Pig Wrapper (Alternative)

**Implementation:**
```bash
# Create a Pig script that executes Flink commands
cat > run_pyflink.pig << EOF
sh gsutil cp gs://${BUCKET}/jobs/job.py /tmp/job.py;
sh flink run -m yarn-cluster -py /tmp/job.py --input gs://${BUCKET}/input --output gs://${BUCKET}/output;
EOF

# Submit via DataProc
gcloud dataproc jobs submit pig \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION} \
    --file=run_pyflink.pig
```

**Pros:**
- ✅ **DataProc Jobs integration**: Appears in `gcloud dataproc jobs list`
- ✅ **Native DataProc monitoring**: Shows up in DataProc console
- ✅ **Cloud Logging**: Automatic integration with Cloud Logging
- ✅ **Familiar workflow**: Uses standard DataProc job submission

**Cons:**
- ❌ **Limited error handling**: Pig wrapper can mask Flink errors
- ❌ **Complex debugging**: Harder to troubleshoot issues
- ❌ **Parameter passing**: Difficult to pass dynamic parameters
- ❌ **Pig dependency**: Requires Pig component on cluster
- ❌ **Shell command limitations**: Limited control over execution environment

**When to Use:**
- Simple PyFlink jobs with minimal parameters
- When DataProc job tracking is essential
- Legacy systems that rely on DataProc job lists

### Method 3: PySpark Wrapper (Not Recommended)

**Implementation:**
```python
# wrapper.py - PySpark job that runs Flink
import subprocess
import sys

def main():
    # Download PyFlink job
    subprocess.run(["gsutil", "cp", "gs://bucket/job.py", "/tmp/job.py"])

    # Run Flink job
    result = subprocess.run([
        "flink", "run", "-m", "yarn-cluster",
        "-py", "/tmp/job.py"
    ], capture_output=True, text=True)

    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```

```bash
gcloud dataproc jobs submit pyspark wrapper.py --cluster=${CLUSTER_NAME}
```

**Pros:**
- ✅ **DataProc integration**: Shows up in DataProc jobs
- ✅ **Python environment**: Familiar Python execution context

**Cons:**
- ❌ **Resource conflicts**: Spark and Flink competing for resources
- ❌ **Complex error handling**: Multiple layers of error propagation
- ❌ **Overhead**: Unnecessary Spark overhead for Flink jobs
- ❌ **Maintenance burden**: Extra wrapper code to maintain

### Comparison Matrix

| Feature | SSH Wrapper | Pig Wrapper | PySpark Wrapper |
|---------|-------------|-------------|-----------------|
| **Flink Functionality** | Full | Full | Full |
| **DataProc Jobs List** | ❌ | ✅ | ✅ |
| **Error Handling** | Excellent | Poor | Fair |
| **Debugging** | Easy | Hard | Medium |
| **Performance** | Best | Good | Poor |
| **Complexity** | Low | Medium | High |
| **Maintenance** | Low | Medium | High |
| **Cloud Composer** | Native | Workaround | Workaround |
| **Resource Efficiency** | Best | Good | Poor |

### Why We Choose SSH Wrapper

For this demo, we chose the **SSH wrapper approach** because:

1. **Production Ready**: Provides the most reliable and feature-complete solution
2. **Full Functionality**: No limitations on Flink features or parameters
3. **Better Debugging**: Direct access to Flink error messages and logs
4. **Cloud Composer Integration**: Works seamlessly with orchestration tools
5. **Resource Efficiency**: No unnecessary overhead from wrapper layers
6. **Industry Standard**: Commonly used pattern in production environments

The trade-off of not appearing in DataProc jobs list is acceptable because:
- YARN and Flink provide comprehensive job monitoring
- Most production environments use orchestration tools (Composer/Airflow)
- The benefits of full functionality outweigh the monitoring convenience

---

## YARN Deployment Modes Explained

### Per-Job Mode (Default for Batch)
```bash
flink run -m yarn-cluster -py job.py
```
- Creates new YARN application per job
- Resources allocated and released per job
- Best for batch processing

### Session Mode (Good for Multiple Jobs)
```bash
# Start session
yarn-session.sh -d -n 4 -tm 4096

# Submit to session
flink run -py job.py
```
- Shared YARN application
- Resources stay allocated
- Good for interactive work

### Application Mode (Production Streaming)
```bash
flink run-application -t yarn-application \
    -Dyarn.application.name="Production Stream" \
    -py streaming_job.py
```
- Main() runs on JobManager
- Better resource isolation
- Ideal for long-running production jobs

---

## Monitoring and Management

### Job Monitoring
```bash
# Check YARN applications
gcloud compute ssh ${CLUSTER}-m --command="yarn application -list"

# Check Flink jobs
gcloud compute ssh ${CLUSTER}-m --command="flink list"

# Get job details
gcloud compute ssh ${CLUSTER}-m --command="flink info <job-id>"

# Cancel job
gcloud compute ssh ${CLUSTER}-m --command="flink cancel <job-id>"
```

### Log Access
```bash
# YARN logs
gcloud compute ssh ${CLUSTER}-m --command="yarn logs -applicationId <app-id>"

# Flink logs
gcloud compute ssh ${CLUSTER}-m --command="cat /opt/flink/log/flink-*.log"

# Cloud Logging (if configured)
gcloud logging read "resource.type=dataproc_cluster AND resource.labels.cluster_name=${CLUSTER}"
```

### Web UIs
- **Flink Web UI**: `http://<master-ip>:8081`
- **YARN Resource Manager**: `http://<master-ip>:8088`
- **Flink History Server**: `http://<master-ip>:8082`

Access via DataProc Component Gateway or SSH port forwarding.

---

## Cost Optimization Strategies

### 1. Ephemeral Clusters
- Use `--max-idle` for auto-deletion
- Right-size clusters for workload
- Use preemptible workers when possible

### 2. Resource Tuning
```bash
# Optimize memory allocation
flink run -m yarn-cluster \
    -yD taskmanager.memory.process.size=4g \
    -yD jobmanager.memory.process.size=2g \
    -py job.py
```

### 3. Autoscaling
```bash
# Enable autoscaling for variable workloads
gcloud dataproc clusters create streaming-cluster \
    --enable-autoscaling \
    --max-workers=10 \
    --secondary-workers=5 \
    --preemptible
```

### 4. Spot Instances
```bash
# Use preemptible instances for cost savings
gcloud dataproc clusters create batch-cluster \
    --preemptible \
    --num-preemptible-workers=4
```

---

## Choosing the Right Pattern

### Decision Matrix

**Choose Ephemeral Batch if:**
- Jobs run < 4 hours
- Jobs are independent
- Cost is primary concern
- Simple deployment preferred

**Choose Long-running Stream if:**
- Processing continuous data streams
- Need low latency
- Stateful processing required
- Multiple related jobs

**Choose Interactive Session if:**
- Development/testing
- Ad-hoc analysis
- Iterative workflows
- Learning/experimentation

**Choose Production Workflow if:**
- Complex dependencies
- Scheduled pipelines
- Need orchestration
- Enterprise requirements

This guide should help you choose the right deployment pattern for your PyFlink workloads on DataProc.
