import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import modal
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# Connects to the already-deployed app — no new app is created
extract_invoice = modal.Function.from_name("invoice-extractor", "extract_invoice")

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
)

bq = bigquery.Client(project="invoiceanalysispipeline")


def processNewFilesInDatasource(DataSourceId: int):
    bucket_name = f"ds{DataSourceId}"
    bigquery_destination_table = f"invoiceanalysispipeline.bronze.ds_{DataSourceId}_raw_json"

    MAX_FILES_PER_RUN = 520 # GitHub actions has a max runtime of 6 hours, and based on testing, processing each file takes around 30-40 seconds on average. So we can process around 540 files in one run to avoid timeouts, but to be safe we set the limit a bit lower.

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="unprocessed/")
    files = [
        obj["Key"] for obj in response.get("Contents", []) 
        if obj["Key"].endswith((".jpg", ".jpeg", ".png", ".pdf"))
        ][:MAX_FILES_PER_RUN]

    if not files:
        print("No new files to process for datasource", DataSourceId)
        return

    for key in files:
        filename = Path(key).name
        print(f"Processing {filename}")

        # Download image bytes
        img_bytes = s3.get_object(Bucket=bucket_name, Key=key)["Body"].read()

        # Move to startedprocessing/ immediately to prevent double-processing
        processing_key = f"startedprocessing/{filename}"
        s3.copy_object(
            CopySource={"Bucket": bucket_name, "Key": key},
            Bucket=bucket_name,
            Key=processing_key,
        )
        s3.delete_object(Bucket=bucket_name, Key=key)

        # Call already-deployed Modal function
        result_json = extract_invoice.remote(img_bytes)

        # Insert into BigQuery bronze
        row = {
            "id": str(uuid.uuid4()),
            "raw_json": json.dumps(result_json),
            "inserted_at": datetime.now(timezone.utc).isoformat(),
        }
        bq.insert_rows_json(bigquery_destination_table, [row])
        print(f"Done: {filename}")


if __name__ == "__main__":
    rows = bq.query("SELECT id FROM `invoiceanalysispipeline.conf.conf_data_source`").result()
    list_of_datasource_ids = [row["id"] for row in rows]

    for ds_id in list_of_datasource_ids:
        print(f"Processing datasource {ds_id}...")
        processNewFilesInDatasource(ds_id)