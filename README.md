# Dataproc with BigLake Metastore

This repository provides a Jupyter notebook that demonstrates how to set up and use a single-node Dataproc cluster with Google Cloud's BigLake Metastore. The notebook covers various scenarios for data integration between HDFS, Hive, Iceberg, and BigQuery.

## Overview

The primary goal of this project is to showcase the integration of different data storage and management technologies within the Google Cloud ecosystem. It provides hands-on examples of:

-   Creating and managing Hive tables on HDFS and GCS.
-   Creating and managing Iceberg tables with both Hive Metastore and BigLake Metastore.
-   Pushing down computation to BigQuery from a Spark environment.
-   Running serverless Spark jobs for data processing.

## Prerequisites

Before you begin, ensure you have the following:

-   A Google Cloud Platform (GCP) project.
-   The `gcloud` command-line tool installed and configured.
-   A GCS bucket for storing initialization scripts and data.

## Setup & Usage

1.  **Clone the repository:**
    ```bash
    git clone git@gitlab.com:google-cloud-ce/googlers/johanesa/spark-hybrid-compute.git
    cd spark-hybrid-compute
    ```

2.  **Configure the notebook:**
    Open the `dataproc_hdfs_hive_jupyter.ipynb` notebook and replace the placeholder values in the configuration section with your GCP project details.

3.  **Run the notebook:**
    Execute the cells in the notebook sequentially to:
    -   Provision a single-node Dataproc cluster.
    -   Run the different data integration scenarios.
    -   Clean up the resources.

## Scenarios

The notebook covers the following scenarios:

1.  **Hive Table from HDFS:** Creating a Hive table from data stored in HDFS.
2.  **Iceberg Table from HDFS:** Creating an Iceberg table using the Hive metastore.
3.  **Hive/Iceberg Table from GCS:** Creating a Hive or Iceberg table from data in Google Cloud Storage.
4.  **Iceberg Table with BigLake Metastore:** Using BigLake Metastore to manage an Iceberg table.
5.  **Push Down Computation to BigQuery:** Offloading query execution to BigQuery for efficiency.
6.  **Serverless Spark Computation:** Running a data processing job using a serverless Dataproc cluster.

## Cleanup

The final step in the notebook deletes the Dataproc cluster to prevent ongoing charges. Ensure you run this step after you have finished experimenting.

## Architecture

Below is a high-level architecture diagram of the solution.

![Architecture Diagram](hybrid-compute.png)

## Additional details

TBA