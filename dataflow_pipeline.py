from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import apache_beam as beam
import json
import argparse

def parse_json(element):
    try:
        record = json.loads(element)
        return {
            'userId': record['userId'],
            'id': record['id'],
            'title': record['title'],
            'body': record['body']
        }
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input GCS file path')
    parser.add_argument('--output', dest='output', required=True, help='BigQuery table name: project:dataset.table')
    parser.add_argument('--gcp_key', dest='gcp_key', required=True, help='Path to GCP credentials JSON file')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'test-402517'
    google_cloud_options.region = 'us-central1'

    # Set the service account key.
    google_cloud_options.service_account_email = known_args.gcp_key

    # Set GCS temp locations
    google_cloud_options.temp_location = 'gs://my-github-actions-bucket-jdl/temp'
    google_cloud_options.staging_location = 'gs://my-github-actions-bucket-jdl/staging'

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from GCS' >> beam.io.ReadFromText(known_args.input)
            | 'Filter empty lines' >> beam.Filter(lambda line: line.strip() != '')
            | 'Parse JSON' >> beam.Map(parse_json)
            | 'Filter invalid JSON' >> beam.Filter(lambda x: x is not None)
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                known_args.output,
                schema='userId:INTEGER, id:INTEGER, title:STRING, body:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
