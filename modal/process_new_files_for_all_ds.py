import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import fitz

import boto3
import modal
from dotenv import load_dotenv
from google.cloud import bigquery

from pydantic_models.registry import DS_MODEL_NAME_REGISTRY
from common_util_functions import move_file_in_r2, get_list_of_datasource_ids

load_dotenv()

# Connects to the already-deployed app — no new app is created
extract_invoice = modal.Function.from_name("invoice-extractor", "extract_invoice")

''' INITIALIZING CLIENTS: '''

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
)

bq = bigquery.Client(project="invoiceanalysispipeline")


''' HELPER FUNCTIONS: '''

# Run a dbt command and raise error if it fails.
def run_dbt(command: str):
    result = subprocess.run(
        #cwd and dbt profiles are set to dbt/ relative to the script's own location via Path(__file__).parent, so they resolve correctly when ran from both vs code and from github actions
        ["dbt", command],
        cwd=Path(__file__).parent.parent / "dbt",
        env={**os.environ, "DBT_PROFILES_DIR": str(Path(__file__).parent.parent / "dbt")},
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt {command} failed with exit code {result.returncode}")


# Convert each page of a PDF into JPEG bytes. Useful for data source 2, as raw data is PDF there and invoice_to_json.py expects bytes.
def pdf_to_image_bytes_list(pdf_bytes: bytes, dpi: int = 150) -> list[bytes]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_as_images = []
    for page in doc:
        mat = fitz.Matrix(dpi / 72, dpi / 72)  # 72 is PDF's base DPI
        pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
        pages_as_images.append(pix.tobytes("jpeg"))
    doc.close()
    return pages_as_images


def processOneFile(key: str, bucket_name: str, bigquery_destination_table: str, model_name: str):
    filename = Path(key).name
    print(f"\nProcessing {filename}")

    processing_key = f"startedprocessing/{filename}"
    processed_key = f"processed/{filename}"

    # STEP 1: Move file from unprocessed/ to startedprocessing/
    move_file_in_r2(s3, bucket_name, key, processing_key)
    print(f"Moved {filename} to {processing_key}, starting processing...")

    # STEP 2: Run ML model inference
    s3_read_download = s3.get_object(Bucket=bucket_name, Key=processing_key)["Body"].read()

    # 2.1: Download image bytes
    if processing_key.endswith(".pdf"):
        pages = pdf_to_image_bytes_list(s3_read_download)
        # Invoices are typically single-page, and they always are for DS 2, so take page 0. Can be changed to a loop in the future if it ever becomes an issue.
        img_bytes = pages[0]
    else:
        img_bytes = s3_read_download  # already an image, pass through unchanged   

    # 2.2: Call cloud-deployed Modal function
    try:
        result_json = extract_invoice.remote(img_bytes, model_name)
    except Exception as e:
        print(f"Model inference failed for {filename} with error: {str(e)}")    #Will not raise a runtime error because I want the processing of the other files to continue
        return
    else:
        print(f"Completed ML inference for {filename}")

    # STEP 3: Validate JSON output to account for network errors
    if (result_json is None):
        print(f"JSON is empty, aborting processing for {filename}...")
        return                   #no automatic retries because model inference is expensive, better in this case to intervene manually

    # STEP 4: Insert into BigQuery bronze
    row = {
        "id": str(uuid.uuid4()),
        "raw_json": json.dumps(result_json),
        "inserted_at": datetime.now(timezone.utc).isoformat(),
        "exclude_from_silver": False,
    }
    try:
        bq.insert_rows_json(bigquery_destination_table, [row])
    except Exception as e:
        print(f"Inserting into BigQuery's bronze layer for file {filename} failed with error {str(e)}.\nAborting process for this file...")
        return
    else:
        print(f"Inserted into BigQuery's bronze layer: {filename}")

    # STEP 5: Run dbt models to insert into silver and gold layers in BigQuery
    run_dbt("run")
    print("Completed dbt run command succesfully.")

    # STEP 6: Run dbt tests to validate data quality in silver and gold layers
    run_dbt("test")
    print("DBT tests passed successfully.")

    # STEP 7: If everything is successful, move file from startedprocessing/ to processed/
    # If something failed midway, the file will remain in startedprocessing/ and a second pipeline will periodically move files back to unprocessed/ for retries
    move_file_in_r2(s3, bucket_name, processing_key, processed_key)


''' MAIN LOOP: '''

# Main function ran by this script - 
# it polls the unprocessed/ folder of every R2 bucket for new files and runs the steps of the pipeline if any are found.
def processNewFilesInDatasource(DataSourceId: int):
    bucket_name = f"ds{DataSourceId}"
    bigquery_destination_table = f"invoiceanalysispipeline.bronze.ds_{DataSourceId}_raw_json"

    model_name = DS_MODEL_NAME_REGISTRY.get(DataSourceId) #mapping between ds id and pydantic model is defined in registry.py
    if model_name is None:
        raise ValueError(f"No Pydantic model registered for datasource {DataSourceId}")   
    
    MAX_FILES_PER_RUN = 500 # GitHub actions has a max runtime of 6 hours, and based on testing, processing each file takes around 30-40 seconds on average. So we can process around 540 files in one run to avoid timeouts, but to be safe we set the limit a bit lower.

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="unprocessed/")
    files = [
        obj["Key"] for obj in response.get("Contents", []) 
        if obj["Key"].endswith((".jpg", ".jpeg", ".png", ".pdf"))
        ][:MAX_FILES_PER_RUN]

    if not files:
        print("No new files to process for datasource", DataSourceId)
        return

    problematic_files = []
    # For every file in unprocessed/ bucket, run this loop:
    for key in files:
        filename = Path(key).name
        try:
            processOneFile(key, bucket_name, bigquery_destination_table, model_name)
        except RuntimeError as e: 
            # If it's a RuntimeError (from dbt), kill the loop for this datasource
            print(f"CRITICAL: dbt failed. Aborting loop for {DataSourceId}. Error: {e}")
            problematic_files.append(str(DataSourceId) + "/" + filename) 
            break
        except Exception as e:
            # If it failed for any other reason than dbt run/dbt test, then abort only this file and continue with the rest.
            print(f"Processing file {filename} failed with error {str(e)}.")
            problematic_files.append(str(DataSourceId) + "/" + filename)
        else:
            print(f"Done: {filename}")
    
    print(f"Finished processing data source {DataSourceId}.")
    if problematic_files:
        print("The following files were not processed successfully and need manual review:")
        print("\n".join(problematic_files))


if __name__ == "__main__":
    list_of_datasource_ids = get_list_of_datasource_ids()

    for ds_id in list_of_datasource_ids:
        print(f"\nProcessing datasource {ds_id}...")
        processNewFilesInDatasource(ds_id)