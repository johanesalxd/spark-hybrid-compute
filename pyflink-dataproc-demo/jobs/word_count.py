#!/usr/bin/env python3
"""
Simple PyFlink Word Count Example for DataProc

This job demonstrates basic PyFlink functionality by counting words
in a text file stored in Google Cloud Storage.
"""

import argparse
import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, FileSink
from pyflink.datastream.formats.csv import CsvRowSerializationSchema
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FlatMapFunction, KeySelector
from collections import Counter


class WordSplitter(FlatMapFunction):
    """Split lines into words."""

    def flat_map(self, line):
        # Split line into words and filter out empty strings
        words = line.strip().split()
        for word in words:
            word = word.strip().lower()
            if word:
                yield (word, 1)


class WordCounter(MapFunction):
    """Count words using a simple counter."""

    def map(self, word_count_tuple):
        word, count = word_count_tuple
        return f"{word},{count}"


def word_count_job(input_path: str, output_path: str):
    """
    PyFlink word count job that reads from GCS and writes results back to GCS.

    Args:
        input_path: GCS path to input text file (e.g., gs://bucket/input.txt)
        output_path: GCS path for output results (e.g., gs://bucket/output/)
    """

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Read text file
    text_stream = env.read_text_file(input_path)

    # Split lines into words and count
    word_counts = text_stream \
        .flat_map(WordSplitter(), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    # Convert to CSV format and write to output
    result_stream = word_counts.map(WordCounter(), output_type=Types.STRING())

    # Write results to output path
    result_stream.write_to_file(output_path)

    # Execute the job
    env.execute("PyFlink Word Count")

    print(f"Word count job completed!")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")


def main():
    """Main function to parse arguments and run the job."""
    parser = argparse.ArgumentParser(description='PyFlink Word Count Job')
    parser.add_argument('--input', required=True,
                       help='Input GCS path (e.g., gs://bucket/input.txt)')
    parser.add_argument('--output', required=True,
                       help='Output GCS path (e.g., gs://bucket/output/)')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting word count job...")
    logger.info(f"Input path: {args.input}")
    logger.info(f"Output path: {args.output}")

    try:
        word_count_job(args.input, args.output)
        logger.info("Job completed successfully!")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
