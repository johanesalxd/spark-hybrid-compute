#!/usr/bin/env python3
"""
BigLake Metastore PySpark Client - Standalone Version

A PySpark-based client to connect to Google Cloud BigLake Metastore using the Iceberg REST API
with proper Java GoogleAuthManager authentication. This script is designed to run standalone
on local machines or GCP GCE VMs.

PREREQUISITES AND SETUP INSTRUCTIONS:
=====================================

0. Check permission required
   gcloud asset search-all-iam-policies --scope="projects/my-project-id" --query="policy:admin@johanesa.altostrat.com" --format="value(policy.bindings.role)" | tr ';' '\n' | sort -u | grep -E "roles/storage.admin|roles/biglake.admin"

1. JAVA INSTALLATION (Required):
   - Install Java 8 or 11 (Java 17+ may have compatibility issues)

   Ubuntu/Debian:
   sudo apt update
   sudo apt install openjdk-11-jdk

   CentOS/RHEL:
   sudo yum install java-11-openjdk-devel

   macOS:
   brew install openjdk@11

   Verify: java -version

2. APACHE SPARK INSTALLATION:
   Option A - Download and extract:
   wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
   tar -xzf spark-3.5.0-bin-hadoop3.tgz
   export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
   export PATH=$SPARK_HOME/bin:$PATH

   Option B - Using pip (simpler):
   pip install pyspark==3.5.0

3. PYTHON DEPENDENCIES:
   pip install pyspark==3.5.0 pandas pyarrow requests google-auth google-auth-oauthlib

4. GOOGLE CLOUD AUTHENTICATION:
   # Install gcloud CLI if not already installed
   curl https://sdk.cloud.google.com | bash
   exec -l $SHELL

   # Authenticate
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID

5. ENVIRONMENT VARIABLES (Optional):
   export BIGLAKE_PROJECT_ID="your-project-id"
   export BIGLAKE_REGION="us-central1"
   export BIGLAKE_BUCKET="your-bucket-name"
   export BIGLAKE_DATASET="your-dataset-name"

6. GCE VM SPECIFIC SETUP:
   # If running on GCE VM, ensure the VM has the following scopes:
   # - https://www.googleapis.com/auth/cloud-platform
   # - https://www.googleapis.com/auth/bigquery
   # - https://www.googleapis.com/auth/devstorage.read_write

USAGE:
======
1. Update configuration in main() function or use environment variables
2. Run: python biglake_pyspark_client.py
3. The script will automatically download required JAR files on first run

TROUBLESHOOTING:
===============
- If Java not found: Ensure JAVA_HOME is set and java is in PATH
- If Spark fails: Check SPARK_HOME and PYSPARK_PYTHON environment variables
- If authentication fails: Run 'gcloud auth application-default login'
- If JAR download fails: Check internet connectivity and try running again

This solution uses PySpark instead of PyIceberg because BigLake REST catalog requires the Java
GoogleAuthManager class which is not available in PyIceberg.
"""

import argparse
import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Optional
import urllib.request

import pandas as pd

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import IntegerType
    from pyspark.sql.types import StringType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StructType
except ImportError as e:
    print(f"Missing PySpark dependency: {e}")
    print("Please install PySpark: pip install pyspark==3.5.0")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# JAR URLs for Iceberg and BigQuery integration
ICEBERG_RUNTIME_JAR_URL = "https://storage-download.googleapis.com/maven-central/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
BIGQUERY_CATALOG_JAR_URL = "https://storage.googleapis.com/spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"


def verify_java_installation():
    """Verify Java installation and version."""
    try:
        result = subprocess.run(['java', '-version'],
                                capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("Java installation verified")
            return True
        else:
            logger.error("Java not found. Please install Java 8 or 11")
            return False
    except FileNotFoundError:
        logger.error("Java not found in PATH. Please install Java 8 or 11")
        return False


def download_jar(url: str, local_path: Path) -> bool:
    """Download JAR file if it doesn't exist locally."""
    if local_path.exists():
        logger.info(f"JAR already exists: {local_path}")
        return True

    try:
        logger.info(f"Downloading JAR: {url}")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, local_path)
        logger.info(f"Successfully downloaded: {local_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download JAR from {url}: {e}")
        return False


def setup_jars() -> List[str]:
    """Download required JAR files and return their paths."""
    jar_dir = Path.home() / ".biglake_spark" / "jars"
    jar_dir.mkdir(parents=True, exist_ok=True)

    iceberg_jar = jar_dir / "iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
    bigquery_jar = jar_dir / "iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar"

    jar_paths = []

    if download_jar(ICEBERG_RUNTIME_JAR_URL, iceberg_jar):
        jar_paths.append(str(iceberg_jar))

    if download_jar(BIGQUERY_CATALOG_JAR_URL, bigquery_jar):
        jar_paths.append(str(bigquery_jar))

    return jar_paths


def get_config_from_env():
    """Get configuration from environment variables."""
    return {
        'project_id': os.getenv('BIGLAKE_PROJECT_ID', 'my-project-id'),
        'region': os.getenv('BIGLAKE_REGION', 'us-central1'),
        'bucket_name': os.getenv('BIGLAKE_BUCKET', 'my-project-id-dataproc-bucket'),
        'dataset': os.getenv('BIGLAKE_DATASET', 'my_iceberg_rest_metastore'),
        'catalog_name': os.getenv('BIGLAKE_CATALOG', 'iceberg_rest_on_bq')
    }


class BigLakePySparkClient:
    """
    A PySpark-based client for connecting to Google Cloud BigLake Metastore using Iceberg REST API.

    This client uses the Java GoogleAuthManager for proper BigLake authentication,
    which is not available in PyIceberg.
    """

    def __init__(self, project_id: str, region: str, bucket_name: str, catalog_name: str = "iceberg_on_bq"):
        """
        Initialize the BigLake PySpark client.

        Args:
            project_id: Google Cloud project ID
            region: Google Cloud region
            bucket_name: GCS bucket name for Iceberg warehouse
            catalog_name: Name of the Iceberg catalog
        """
        self.project_id = project_id
        self.region = region
        self.bucket_name = bucket_name
        self.catalog_name = catalog_name
        self.warehouse_path = f"gs://{bucket_name}"
        self.spark = None

    def connect(self) -> None:
        """
        Connect to BigLake Metastore by creating a properly configured Spark session.
        """
        try:
            # Verify Java installation
            if not verify_java_installation():
                raise RuntimeError("Java installation verification failed")

            # Setup JAR files
            jar_paths = setup_jars()
            if not jar_paths:
                raise RuntimeError("Failed to setup required JAR files")

            logger.info(
                "Creating Spark session with BigLake Metastore configuration...")
            logger.info(f"  Project: {self.project_id}")
            logger.info(f"  Region: {self.region}")
            logger.info(f"  Warehouse: {self.warehouse_path}")
            logger.info(f"  Catalog: {self.catalog_name}")
            logger.info(f"  JARs: {jar_paths}")

            # Create Spark session with BigLake configuration
            builder = SparkSession.builder \
                .appName("BigLake-Iceberg-Client-Standalone") \
                .config("spark.jars", ",".join(jar_paths)) \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
                .config(f"spark.sql.catalog.{self.catalog_name}.type", "rest") \
                .config(f"spark.sql.catalog.{self.catalog_name}.uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog") \
                .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.warehouse_path) \
                .config(f"spark.sql.catalog.{self.catalog_name}.header.x-goog-user-project", self.project_id) \
                .config(f"spark.sql.catalog.{self.catalog_name}.rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager") \
                .config(f"spark.sql.catalog.{self.catalog_name}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
                .config(f"spark.sql.catalog.{self.catalog_name}.rest-metrics-reporting-enabled", "false") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # Add memory configurations for standalone mode
            builder = builder \
                .config("spark.driver.memory", "2g") \
                .config("spark.driver.maxResultSize", "1g") \
                .config("spark.executor.memory", "2g")

            self.spark = builder.getOrCreate()

            # Set the default catalog
            self.spark.sql(f"USE CATALOG {self.catalog_name}")

            logger.info("Successfully connected to BigLake Metastore!")

        except Exception as e:
            logger.error(f"Failed to connect to BigLake Metastore: {e}")
            sys.exit(1)
            raise

    def disconnect(self) -> None:
        """
        Disconnect from BigLake Metastore by stopping the Spark session.
        """
        if self.spark:
            self.spark.stop()
            self.spark = None
            logger.info("Disconnected from BigLake Metastore")

    def list_namespaces(self) -> List[str]:
        """
        List all namespaces (datasets) in the catalog.

        Returns:
            List of namespace names
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            # Use Spark SQL to show databases
            result = self.spark.sql("SHOW DATABASES").collect()
            namespaces = [row['namespace'] for row in result]

            logger.info(f"Found {len(namespaces)} namespaces:")
            for ns in namespaces:
                logger.info(f"  - {ns}")

            return namespaces

        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    def list_tables(self, namespace: str) -> List[str]:
        """
        List all tables in a specific namespace.

        Args:
            namespace: Namespace (dataset) name

        Returns:
            List of table names
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            # Use the namespace
            self.spark.sql(f"USE {namespace}")

            # Show tables in the namespace
            result = self.spark.sql("SHOW TABLES").collect()
            tables = [row['tableName'] for row in result]

            logger.info(
                f"Found {len(tables)} tables in namespace '{namespace}':")
            for table in tables:
                logger.info(f"  - {table}")

            return tables

        except Exception as e:
            logger.warning(
                f"Failed to list tables in namespace '{namespace}': {e}")
            return []

    def get_table_schema(self, namespace: str, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the schema of a specific table.

        Args:
            namespace: Namespace (dataset) name
            table_name: Table name

        Returns:
            Table schema information
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            # Describe the table
            result = self.spark.sql(
                f"DESCRIBE {namespace}.{table_name}").collect()

            fields = []
            for row in result:
                # Skip comments
                if row['col_name'] and not row['col_name'].startswith('#'):
                    fields.append((row['col_name'], row['data_type']))

            logger.info(f"Schema for table '{namespace}.{table_name}':")
            for field_name, field_type in fields:
                logger.info(f"  - {field_name}: {field_type}")

            return {
                "fields": fields,
                "table_name": table_name,
                "namespace": namespace
            }

        except Exception as e:
            logger.warning(
                f"Table '{namespace}.{table_name}' does not exist or error occurred: {e}")
            return None

    def query_table(self, namespace: str, table_name: str, limit: int = 10) -> Optional[pd.DataFrame]:
        """
        Query data from a specific table.

        Args:
            namespace: Namespace (dataset) name
            table_name: Table name
            limit: Maximum number of rows to return

        Returns:
            Pandas DataFrame with query results
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            logger.info(
                f"Querying table '{namespace}.{table_name}' (limit: {limit})...")

            # Query the table with limit
            df = self.spark.sql(
                f"SELECT * FROM {namespace}.{table_name} LIMIT {limit}")

            # Convert to Pandas DataFrame
            pandas_df = df.toPandas()

            logger.info(
                f"Retrieved {len(pandas_df)} rows from '{namespace}.{table_name}'")
            logger.info(f"Columns: {list(pandas_df.columns)}")

            return pandas_df

        except Exception as e:
            logger.warning(
                f"Failed to query table '{namespace}.{table_name}': {e}")
            return None

    def create_sample_table(self, namespace: str, table_name: str) -> bool:
        """
        Create a sample table for testing purposes.

        Args:
            namespace: Namespace (dataset) name
            table_name: Table name

        Returns:
            True if successful, False otherwise
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            # Create namespace if it doesn't exist
            try:
                self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
                logger.info(f"Created namespace '{namespace}'")
            except Exception:
                logger.info(f"Namespace '{namespace}' already exists")

            # Drop table if it exists
            try:
                self.spark.sql(
                    f"DROP TABLE IF EXISTS {namespace}.{table_name}")
            except Exception:
                pass

            # Create the table
            self.spark.sql(f"""
                CREATE TABLE {namespace}.{table_name} (
                    name STRING,
                    age INT
                ) USING ICEBERG
            """)

            # Insert sample data
            self.spark.sql(f"""
                INSERT INTO {namespace}.{table_name} VALUES
                ('Alice', 25),
                ('Bob', 30),
                ('Charlie', 35),
                ('Diana', 28)
            """)

            # Verify the data
            count = self.spark.sql(
                f"SELECT COUNT(*) as count FROM {namespace}.{table_name}").collect()[0]['count']

            logger.info(
                f"Successfully created sample table '{namespace}.{table_name}' with {count} rows")
            return True

        except Exception as e:
            logger.error(
                f"Failed to create sample table '{namespace}.{table_name}': {e}")
            return False

    def execute_sql(self, sql: str) -> Optional[pd.DataFrame]:
        """
        Execute arbitrary SQL query.

        Args:
            sql: SQL query to execute

        Returns:
            Pandas DataFrame with query results
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            logger.info(f"Executing SQL: {sql}")
            df = self.spark.sql(sql)
            pandas_df = df.toPandas()
            logger.info(f"Query returned {len(pandas_df)} rows")
            return pandas_df

        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            return None

    def interactive_repl(self) -> None:
        """
        Start a Python REPL with pre-configured Spark session.
        """
        if not self.spark:
            raise RuntimeError("Not connected. Call connect() first.")

        import code

        # Prepare the namespace for the REPL
        namespace = {
            'spark': self.spark,
            'client': self,
            'catalog_name': self.catalog_name,
            'project_id': self.project_id,
            'warehouse_path': self.warehouse_path,
        }

        # Add common imports
        namespace.update({
            'pyspark': __import__('pyspark'),
            'SparkSession': self.spark.__class__,
        })

        # Print info before starting REPL
        print(f"\nBigLake Spark session ready!")
        print(f"Catalog: {self.catalog_name}")
        print(f"Project: {self.project_id}")
        print(f"Warehouse: {self.warehouse_path}")
        print(f"\nAvailable variables: spark, client, catalog_name, project_id, warehouse_path")
        print(f"Example: spark.sql(\"SELECT CURRENT_TIMESTAMP()\").show()")
        print()

        # Start the interactive Python console with minimal banner
        code.interact(banner="", local=namespace)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='BigLake PySpark Client - Connect to BigLake Metastore using PySpark',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python biglake_pyspark_client.py                    # Run demo mode
  python biglake_pyspark_client.py --interactive      # Start interactive Python REPL
        """
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Start interactive SQL shell for running Spark SQL commands'
    )
    return parser.parse_args()


def main():
    """
    Main function for BigLake PySpark client.

    Supports both demo mode and interactive SQL shell mode based on command line arguments.
    Configuration can be provided via environment variables or modified below.
    """
    # Parse command line arguments
    args = parse_arguments()

    # Get configuration from environment variables or use defaults
    config = get_config_from_env()

    print("=== BigLake PySpark Client - Standalone Version ===")
    print(f"Configuration:")
    print(f"  Project ID: {config['project_id']}")
    print(f"  Region: {config['region']}")
    print(f"  Bucket: {config['bucket_name']}")
    print(f"  Dataset: {config['dataset']}")
    print(f"  Catalog: {config['catalog_name']}")
    print()

    # Initialize client
    client = BigLakePySparkClient(
        project_id=config['project_id'],
        region=config['region'],
        bucket_name=config['bucket_name'],
        catalog_name=config['catalog_name']
    )

    try:
        # Connect to BigLake Metastore
        print("=== Connecting to BigLake Metastore ===")
        client.connect()

        if args.interactive:
            # Start interactive Python REPL
            client.interactive_repl()
        else:
            # Run demo mode (original functionality)
            # List namespaces
            print("\n=== Listing Namespaces ===")
            namespaces = client.list_namespaces()

            # If we have namespaces, explore them
            if namespaces:
                for namespace in namespaces:
                    print(f"\n=== Exploring Namespace: {namespace} ===")

                    # List tables in this namespace
                    tables = client.list_tables(namespace)

                    # If we have tables, query the first one
                    if tables:
                        table_name = tables[0]

                        # Get table schema
                        print(f"\n--- Schema for {namespace}.{table_name} ---")
                        schema_info = client.get_table_schema(
                            namespace, table_name)

                        # Query table data
                        print(f"\n--- Data from {namespace}.{table_name} ---")
                        df = client.query_table(namespace, table_name, limit=5)
                        if df is not None:
                            print(df.to_string())

                        break  # Only process the first namespace with tables
            else:
                # Create a sample table for demonstration
                print(f"\n=== Creating Sample Table ===")
                success = client.create_sample_table(
                    config['dataset'], "sample_people")
                if success:
                    print(f"\n--- Querying Sample Table ---")
                    df = client.query_table(config['dataset'], "sample_people")
                    if df is not None:
                        print(df.to_string())

            print("\n=== BigLake PySpark Client Demo Complete ===")

    except Exception as e:
        logger.error(f"Operation failed: {e}")
        print(f"\nError: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure Java 8 or 11 is installed: java -version")
        print(
            "2. Verify Google Cloud authentication: gcloud auth application-default login")
        print("3. Check your project ID and bucket name in the configuration")
        print("4. Ensure your GCS bucket exists and is accessible")
        sys.exit(1)
    finally:
        # Always disconnect
        client.disconnect()


if __name__ == "__main__":
    main()


# ============================================================================
# QUICK START SCRIPT
# ============================================================================
"""
QUICK START - Copy and paste this into your terminal:

# 1. Install dependencies
pip install pyspark==3.5.0 pandas pyarrow google-auth google-auth-oauthlib

# 2. Set environment variables (replace with your values)
export BIGLAKE_PROJECT_ID="your-project-id"
export BIGLAKE_REGION="us-central1"
export BIGLAKE_BUCKET="your-bucket-name"
export BIGLAKE_DATASET="your-dataset-name"

# 3. Authenticate with Google Cloud
gcloud auth application-default login

# 4. Run the script
python biglake_pyspark_client.py

# ============================================================================
# JUPYTER NOTEBOOK VERSION
# ============================================================================

To use this in a Jupyter notebook, copy the BigLakePySparkClient class and use:

from biglake_pyspark_client import BigLakePySparkClient, get_config_from_env

config = get_config_from_env()
client = BigLakePySparkClient(
    project_id=config['project_id'],
    region=config['region'],
    bucket_name=config['bucket_name'],
    catalog_name=config['catalog_name']
)

client.connect()
namespaces = client.list_namespaces()
# ... use the client methods
client.disconnect()
"""
