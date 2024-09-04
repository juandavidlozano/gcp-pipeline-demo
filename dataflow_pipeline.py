name: GCP Pipeline

on:
  push:
    branches:
      - main

jobs:
  upload-to-gcs-and-run-dataflow:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v2

    - name: Set up Google Cloud SDK
      uses: google-github-actions/setup-gcloud@v1
      with:
        project_id: test-402517
        service_account_key: ${{ secrets.SECRET }}
        export_default_credentials: true

    - name: Decode and write service account key to file
      run: |
        echo "${{ secrets.SECRET }}" | base64 --decode > $HOME/gcp-key.json
        echo "=== START OF JSON FILE CONTENT ==="
        cat $HOME/gcp-key.json
        echo "=== END OF JSON FILE CONTENT ==="

    - name: Authenticate with GCP
      run: gcloud auth activate-service-account --key-file=$HOME/gcp-key.json

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.x'

    - name: Install dependencies
      run: pip install -r requirements.txt

    - name: Run Python script to fetch API data
      run: python fetch_data.py

    - name: Upload file to GCS
      run: gsutil cp api_response.json gs://my-github-actions-bucket-jdl/api_response.json

    - name: Run Dataflow Pipeline
      run: |
        python dataflow_pipeline.py \
          --input gs://my-github-actions-bucket-jdl/api_response.json \
          --output test-402517:my_dataset.api_data_table \
          --gcp_key $HOME/gcp-key.json
