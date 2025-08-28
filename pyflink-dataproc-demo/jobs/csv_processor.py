#!/usr/bin/env python3
"""
PyFlink CSV Processing Example for DataProc

This job demonstrates processing CSV data from GCS, performing transformations,
and writing results back to GCS in different formats.
"""

import argparse
import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def csv_processing_job(input_path: str, output_path: str):
    """
    PyFlink job that processes CSV data from GCS.

    Args:
        input_path: GCS path to input CSV file (e.g., gs://bucket/input.csv)
        output_path: GCS path for output results (e.g., gs://bucket/output/)
    """

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Create table environment in batch mode for aggregations
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)

    # Create source table for CSV input
    table_env.execute_sql(f"""
        CREATE TABLE source_csv (
            id INT,
            name STRING,
            age INT,
            city STRING,
            salary DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{input_path}',
            'format' = 'csv',
            'csv.ignore-parse-errors' = 'true',
            'csv.allow-comments' = 'true'
        )
    """)

    # Create sink table for processed results
    table_env.execute_sql(f"""
        CREATE TABLE sink_results (
            city STRING,
            avg_age DOUBLE,
            avg_salary DOUBLE,
            total_people BIGINT,
            age_category STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{output_path}',
            'format' = 'csv'
        )
    """)

    # Process data: aggregate by city and categorize by age
    table_env.execute_sql("""
        INSERT INTO sink_results
        SELECT
            city,
            AVG(CAST(age AS DOUBLE)) as avg_age,
            AVG(salary) as avg_salary,
            COUNT(*) as total_people,
            CASE
                WHEN AVG(CAST(age AS DOUBLE)) < 30 THEN 'Young'
                WHEN AVG(CAST(age AS DOUBLE)) < 50 THEN 'Middle-aged'
                ELSE 'Senior'
            END as age_category
        FROM source_csv
        WHERE age IS NOT NULL AND salary IS NOT NULL
        GROUP BY city
        HAVING COUNT(*) > 0
    """)

    print(f"CSV processing job completed!")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")


def main():
    """Main function to parse arguments and run the job."""
    parser = argparse.ArgumentParser(description='PyFlink CSV Processing Job')
    parser.add_argument('--input', required=True,
                       help='Input GCS CSV path (e.g., gs://bucket/data.csv)')
    parser.add_argument('--output', required=True,
                       help='Output GCS path (e.g., gs://bucket/output/)')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting CSV processing job...")
    logger.info(f"Input path: {args.input}")
    logger.info(f"Output path: {args.output}")

    try:
        csv_processing_job(args.input, args.output)
        logger.info("Job completed successfully!")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
