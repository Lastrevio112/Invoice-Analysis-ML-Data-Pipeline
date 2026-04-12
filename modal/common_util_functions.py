# Cut + paste between two R2 paths:
import sys
sys.path.append('/workspace/modal') #So that this is visible from workspace/ and not only from workspace/modal/

import boto3

from pydantic_models.registry import DS_MODEL_REGISTRY

# Cut + paste between two R2 paths:
def move_file_in_r2(boto3_client, bucket_name: str, source_key: str, destination_key: str):
    # File in R2 is first copied to new location
    boto3_client.copy_object(
        CopySource={"Bucket": bucket_name, "Key": source_key},
        Bucket=bucket_name,
        Key=destination_key,
    )
    # Then deleted from original location
    boto3_client.delete_object(Bucket=bucket_name, Key=source_key)

# Get list of datasource ids from the registry 
# this is how we know which buckets to poll in process_new_files_for_all_ds.py and in cleanup_startedprocessing_folder.py
def get_list_of_datasource_ids():
    return list(DS_MODEL_REGISTRY.keys()) # much more efficient than connecting to bigquery and selecting from conf_data_source