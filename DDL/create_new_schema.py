from google.cloud import bigquery
import os

def create_new_schema(schema_name: str):
    # This should work both locally and through GitHub actions, although we likely will never run it through GitHub actions
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if creds_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

    client = bigquery.Client(project="invoiceanalysispipeline")

    client.query(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name}
        OPTIONS (location = 'EU')
    """).result()