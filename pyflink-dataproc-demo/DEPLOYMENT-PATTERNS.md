# PyFlink on Dataproc - Deployment Patterns Guide

This guide covers different deployment patterns for PyFlink on Google Cloud Dataproc, helping you choose the right approach for your use case.

## Overview of Patterns

| Pattern | Use Case | Cluster Lifecycle | YARN Mode |
|---------|----------|-------------------|-----------|
| **Ephemeral Batch** | ETL, Reports, ML Training | Create → Run → Delete | Per-job |
| **Long-running Stream** | Real-time processing | Persistent | Session |
| **Production Workflow** | Scheduled jobs | Orchestrated | Per-job |

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

## Pattern 3: Production Workflow Orchestration

**Best for:** Production pipelines, complex dependencies, scheduled jobs

### Orchestration Options

For production workflows, you can use various orchestration tools:

**Cloud Workflows**: Simple YAML-based workflow orchestration
**Cloud Composer (Airflow)**: Full-featured workflow management
**Cloud Scheduler**: Simple cron-like scheduling
**Custom solutions**: Using Cloud Functions, Cloud Run, etc.

The key is to use the SSH wrapper approach for PyFlink job submission within your chosen orchestration framework.

---

## Dependency Management Best Practices

### Overview
PyFlink on Dataproc supports multiple approaches for managing dependencies. Choose the right approach based on your job complexity and requirements.

| Approach | Use Case | Pros | Cons |
|----------|----------|------|------|
| **No Dependencies** | Simple jobs using built-in libraries | Fast, simple | Limited functionality |
| **requirements.txt + pyarch** | External Python packages | Standard Python workflow | Requires packaging |
| **py-files** | Custom Python modules | Good for shared code | Manual file management |
| **JAR files** | Java dependencies | Full Flink ecosystem | Complex setup |

### Method 1: No External Dependencies (Recommended for Simple Jobs)
```bash
# This demo approach - uses only built-in libraries
flink run -m yarn-cluster \
    -py /tmp/job.py \
    --input gs://bucket/input \
    --output gs://bucket/output
```

**Best for:** Basic data processing, CSV operations, simple transformations

### Method 2: Python Dependencies with pyarch
```bash
# Create dependency archive
pip install -r requirements.txt --target ./deps
zip -r deps.zip deps/

# Upload to GCS
gsutil cp deps.zip gs://bucket/deps/

# Submit job with dependencies
flink run -m yarn-cluster \
    -pyarch gs://bucket/deps/deps.zip \
    -py /tmp/job.py
```

**Best for:** Jobs requiring external Python packages (pandas, numpy, etc.)

### Method 3: Custom Python Modules with py-files
```bash
# Upload custom modules
gsutil cp my_utils.py gs://bucket/modules/

# Submit job with custom modules
flink run -m yarn-cluster \
    -py-files gs://bucket/modules/my_utils.py \
    -py /tmp/job.py
```

**Best for:** Shared utility functions, custom business logic

### Known Limitations

#### yarn.ship-files with GCS Paths
```bash
# ❌ This doesn't work with gs:// paths
flink run -m yarn-cluster \
    -Dyarn.ship-files=gs://bucket/deps.zip \
    -py /tmp/job.py

# ✅ Use pyarch instead
flink run -m yarn-cluster \
    -pyarch gs://bucket/deps.zip \
    -py /tmp/job.py
```

**Reason:** YARN's ship-files parameter expects local filesystem paths, not GCS URLs. PyFlink's `-pyarch` parameter handles GCS downloads automatically.

### Best Practices

1. **Start Simple**: Begin with no dependencies, add only what's needed
2. **Version Pin**: Always pin dependency versions in requirements.txt
3. **Test Locally**: Validate dependency archives before deploying
4. **Cache Archives**: Reuse dependency archives across jobs when possible
5. **Monitor Size**: Keep dependency archives under 100MB for faster job startup

---

## PyFlink Job Submission Methods

Since `gcloud dataproc jobs submit flink` only supports JAR files (not PyFlink), we need alternative approaches for submitting PyFlink jobs to Dataproc.

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
- ❌ **Not in Dataproc Jobs list**: Jobs don't appear in `gcloud dataproc jobs list`
- ❌ **Manual monitoring**: Need to use YARN/Flink UIs instead of Dataproc console
- ❌ **File download overhead**: Need to download Python files from GCS first

**When to Use:**
- Production PyFlink workloads
- Complex jobs with custom dependencies
- When you need full Flink functionality
- Cloud Composer/Airflow orchestration

### Alternative Methods

**Pig Wrapper**: Uses `gcloud dataproc jobs submit pig` to execute Flink commands. Provides Dataproc job tracking but has limited error handling and debugging capabilities.

**PySpark Wrapper**: Runs Flink jobs within PySpark. Not recommended due to resource conflicts and unnecessary overhead.

**Why SSH Wrapper is Recommended:**
- Full Flink functionality and direct error messages
- Better performance and resource efficiency
- Native integration with orchestration tools
- Industry standard approach for production workloads

---

## YARN Deployment Modes Explained

### Per-Job Mode (Default for Batch)
```bash
flink run -m yarn-cluster -py job.py
```
- Creates new YARN application per job
- Resources allocated and released per job
- Best for batch processing and ephemeral clusters

### Session Mode (Good for Multiple Jobs)
```bash
# Start session (creates Flink cluster across YARN workers)
yarn-session.sh -d -n 4 -tm 4096

# Submit to session (jobs distributed across TaskManagers)
flink run -py job.py
```
- Shared YARN application for multiple jobs
- **TaskManagers run on worker nodes**: Jobs are distributed across the cluster
- Resources stay allocated between jobs
- Good for interactive work and streaming clusters

**How to verify distribution:**
```bash
# Check YARN application and TaskManager locations
yarn application -list
yarn logs -applicationId <app-id> | grep -E "(TaskManager|Started TaskManager)"

# Check Flink Web UI for TaskManager distribution
# Access via Dataproc Component Gateway or port forwarding
```

**Note:** Application mode is similar to per-job mode (both create dedicated YARN applications per job) but runs the main() method on the JobManager instead of the client. For most PyFlink use cases, per-job mode is sufficient and simpler.

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

### Granular Log Filtering

#### Cloud Logging Queries
```
# Flink JobManager logs
resource.type="cloud_dataproc_cluster" AND
resource.labels.cluster_name="your-cluster" AND
SEARCH("`org.apache.flink.runtime.jobmaster.JobMaster`")

# Flink TaskManager logs
resource.type="cloud_dataproc_cluster" AND
resource.labels.cluster_name="your-cluster" AND
SEARCH("`org.apache.flink.runtime.taskexecutor.TaskExecutor`")

# YARN ResourceManager logs
resource.type="cloud_dataproc_cluster" AND
resource.labels.cluster_name="your-cluster" AND
SEARCH("`org.apache.hadoop.yarn.server.resourcemanager`")
```

#### Direct Log Access
```bash
# Flink JobManager logs
gcloud compute ssh ${CLUSTER}-m --command="tail -f /opt/flink/log/flink-*-jobmanager-*.log"

# Flink TaskManager logs
gcloud compute ssh ${CLUSTER}-w-0 --command="tail -f /opt/flink/log/flink-*-taskmanager-*.log"

# YARN application logs
gcloud compute ssh ${CLUSTER}-m --command="yarn logs -applicationId <app-id> | grep -E '(ERROR|WARN)'"
```

Access via Dataproc Component Gateway or SSH port forwarding.

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

This guide should help you choose the right deployment pattern for your PyFlink workloads on Dataproc.
