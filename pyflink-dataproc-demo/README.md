# PyFlink on DataProc Demo

This demo addresses common questions about running PyFlink on Google Cloud DataProc and provides practical examples for deployment and job submission.

## Questions Answered

### 1. **Major Issues with Running PyFlink on DataProc**
- ✅ **No major issues**: DataProc officially supports Flink as an optional component since version 2.1+
- ✅ **Native integration**: Flink is pre-installed and configured when using `--optional-components=FLINK`
- ✅ **YARN integration**: Works out of the box with proper resource management

### 2. **Best YARN Submission Mode for DataProc**
- ✅ **Per-job mode is recommended** for DataProc
- Better resource isolation between jobs
- Easier cleanup after job completion
- More suitable for batch processing workloads
- Aligns well with DataProc's ephemeral cluster model

### 3. **Packaging Dependencies for PyFlink**
- ✅ **Use `requirements.txt`** for Python dependencies
- ✅ **Use `--py-files`** for custom Python modules
- ✅ **Use `--jars`** for additional JAR dependencies
- ✅ **Use `--archives`** for larger dependency packages

### 4. **Job Submission Methods**
- ✅ **SSH wrapper approach**: Use `gcloud compute ssh` + `flink run` for PyFlink jobs
- ✅ **YARN integration**: Jobs appear in Flink History Server and YARN Resource Manager
- ❌ **Note**: `gcloud dataproc jobs submit flink` only supports JAR files, not PyFlink

### 5. **GCS Path Handling**
- ✅ **Direct GCS support**: PyFlink can read/write directly from `gs://` paths
- ✅ **Dependency upload**: Upload Python files and dependencies to GCS

## Demo Focus

This demo focuses on **batch processing** using the **ephemeral cluster pattern** - the most common and cost-effective approach for PyFlink on DataProc.

For other deployment patterns (streaming, long-running clusters, interactive analysis), see **[DEPLOYMENT-PATTERNS.md](DEPLOYMENT-PATTERNS.md)**.

## Demo Structure

```
pyflink-dataproc-demo/
├── README.md                          # This file
├── DEPLOYMENT-PATTERNS.md             # Comprehensive deployment guide
├── USAGE.md                           # Step-by-step usage instructions
├── requirements.txt                   # Development dependencies
├── cluster/
│   ├── create-flink-cluster.sh       # Create DataProc with Flink
│   └── delete-cluster.sh             # Cleanup script
├── jobs/
│   ├── word_count.py                 # Simple PyFlink example
│   ├── csv_processor.py              # CSV processing from GCS
│   └── requirements.txt              # Runtime dependencies
├── submit/
│   ├── submit-pyflink.sh             # SSH-based job submission
│   └── upload-to-gcs.sh              # Upload jobs to GCS
└── data/
    ├── sample.csv                     # Sample CSV data
    └── sample.txt                     # Sample text data
```

## Quick Start

### 1. Set Configuration
```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"
export CLUSTER_NAME="pyflink-demo-cluster"
export BUCKET_NAME="${PROJECT_ID}-pyflink-demo"
```

### 2. Create Cluster
```bash
cd cluster/
./create-flink-cluster.sh
```

### 3. Submit Job
```bash
cd submit/
./submit-pyflink.sh
```

### 4. Clean Up
```bash
cd cluster/
./delete-cluster.sh
```

## Prerequisites

- Google Cloud SDK installed and configured
- A GCP project with DataProc API enabled
- A GCS bucket for storing jobs and data

## Development Setup

For local development and testing, install the required dependencies:

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -r requirements.txt
```

**Note**: PyFlink is not included in the requirements as it's pre-installed on DataProc clusters. For local PyFlink development, you would need to install Apache Flink separately.

## Next Steps

1. Run the basic word count example
2. Try the CSV processing job with your own data
3. Customize the jobs for your specific use cases
