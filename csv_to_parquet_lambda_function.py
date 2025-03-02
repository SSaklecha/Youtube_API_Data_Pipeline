import os
import urllib.parse
import awswrangler as wr
import pandas as pd

# Environment variables
CLEANSED_S3_PATH = os.environ['s3_cleansed_layer']
GLUE_DB = os.environ['glue_catalog_db_name']
GLUE_TABLE = os.environ['glue_catalog_table_name']
WRITE_MODE = os.environ['write_data_operation']


def handler(event, context):
    """
    Lambda function that reads a JSON file from S3, processes it,
    and writes the resulting DataFrame back to S3 as a Parquet dataset.
    """
    try:
        # Retrieve S3 bucket and object key from the event
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        raw_key = record['s3']['object']['key']
        decoded_key = urllib.parse.unquote_plus(raw_key, encoding='utf-8')
    except KeyError as key_err:
        error_msg = f"Missing expected key in event: {key_err}"
        print(error_msg)
        raise

    try:
        # Load the JSON data from S3 into a DataFrame
        json_content = wr.s3.read_json(f's3://{bucket_name}/{decoded_key}')
        
        # Normalize the nested 'items' field into a flat table
        df_processed = pd.json_normalize(json_content['items'])
        
        # Write the DataFrame to S3 in Parquet format
        response = wr.s3.to_parquet(
            df=df_processed,
            path=CLEANSED_S3_PATH,
            dataset=True,
            database=GLUE_DB,
            table=GLUE_TABLE,
            mode=WRITE_MODE
        )
        return response

    except Exception as ex:
        error_message = (
            f"Failed to process object '{decoded_key}' from bucket '{bucket_name}'. "
            f"Ensure the file exists and is accessible. Error details: {ex}"
        )
        print(error_message)
        raise
