import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
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
    Main pipeline function to read JSON, transform, and write to BigQuery.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input GCS file path')
    parser.add_argument('--output', dest='output', required=True, help='BigQuery table name: project:dataset.table')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'test-402517'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = 'gcp-pipeline-demo'

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from GCS' >> beam.io.ReadFromText(known_args.input)
            | 'Parse JSON' >> beam.Map(parse_json)
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                known_args.output,
                schema='userId:INTEGER, id:INTEGER, title:STRING, body:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
