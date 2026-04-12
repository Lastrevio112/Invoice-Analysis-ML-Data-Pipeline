import boto3
from pathlib import Path
import os
from datetime import datetime, timedelta, timezone

from common_util_functions import move_file_in_r2, get_list_of_datasource_ids

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    region_name="auto",
)


def cleanupFilesInDatasource(DataSourceId: int):
    bucket_name = f"ds{DataSourceId}"

    SOURCE_PREFIX = "startedprocessing/"
    DESTINATION_PREFIX = "unprocessed/"

    response_startedprocessing = s3.list_objects_v2(Bucket=bucket_name, Prefix=SOURCE_PREFIX)
    files = [
        obj["Key"] for obj in response_startedprocessing.get("Contents", []) 
        if obj["Key"].endswith((".jpg", ".jpeg", ".png", ".pdf"))
        and obj["LastModified"] < datetime.now(timezone.utc) - timedelta(hours=1) # Only consider files that have been in startedprocessing/ for more than 1 hour
        ]

    if not files:
        print("No files in startedprocessing/ to clean up for datasource", DataSourceId)
        return
    
    print(f"Found {len(files)} files in startedprocessing/ for datasource {DataSourceId}, moving them back to unprocessed/ for retry...")

    for key in files:
        filename = Path(key).name

        source_key = key
        destination_key = DESTINATION_PREFIX + filename

        print(f"\Moving {filename}...")
        move_file_in_r2(s3, bucket_name, source_key, destination_key)
    
    print(f"Finished moving files from startedprocessing/ back to unprocessed/ for datasource {DataSourceId}.\n")

if __name__ == "__main__":
    list_of_datasource_ids = get_list_of_datasource_ids()

    for ds_id in list_of_datasource_ids:
        print(f"\Checking datasource {ds_id}...")
        cleanupFilesInDatasource(ds_id)
    

