import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import argparse

def parse_json(element):
    """
    Parse each JSON element and extract relevant fields to transform into rows
    """
    record = json.loads(element)
    return {
        'userId': record['userId'],
        'id': record['id'],
        'title': record['title'],
        'body': record['body']
    }

def run(argv=None):
    """
    Main pipeline function to read JSON from GCS, transform it, and write to BigQuery.
    """
    parser = argparse.ArgumentParser()
    
    # Arguments to pass input GCS file and BigQuery output table
    parser.add_argument('--input', dest='input', required=True, help='Input GCS file path')
    parser.add_argument('--output', dest='output', required=True, help='BigQuery table name: project:dataset.table')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',  # Change this to 'DirectRunner' for local testing
        project='test-402517',    # Your GCP project ID
        region='us-central1',     # Your GCP region
        temp_location='gs://my-github-actions-bucket-jdl/temp/',  # Temporary location for Dataflow jobs
        staging_location='gs://my-github-actions-bucket-jdl/staging/'
    )

    # Define the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from GCS' >> beam.io.ReadFromText(known_args.input)  # Read JSON lines from GCS
            | 'Parse JSON' >> beam.Map(parse_json)  # Parse each JSON line into a dictionary
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                known_args.output,  # Output to BigQuery
                schema='userId:INTEGER, id:INTEGER, title:STRING, body:STRING',  # BigQuery schema
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  # Append to the table
            )
        )

if __name__ == '__main__':
    run()
