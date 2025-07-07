from vertexai import agent_engines
from google.cloud import storage
import os

PROJECT_ID = os.environ.get("PROJECT_ID")
ORCHESTRATE_AGENT_ID = os.environ.get('ORCHESTRATE_AGENT_ID')
agent_engine = agent_engines.get(ORCHESTRATE_AGENT_ID)

agent_engine_bucket_name = f"{PROJECT_ID}-agent-engine"
cloud_build_bucket_name = f"{PROJECT_ID}-agent-engine"

agent_engine = agent_engines.get(ORCHESTRATE_AGENT_ID)
agent_engine.delete(force=True)

def delete_bucket (bucket_name):
    # check in project_id variable is set
    if not PROJECT_ID:
        raise ValueError("PROJECT_ID environment variable not set.")
      
    storage_client = storage.Client()
    try:
        bucket = storage_client.get_bucket(bucket_name)
        bucket.delete(force=True)  # Using force=True to handle potential non-empty buckets
        print(f"Bucket {bucket_name} deleted.")
    except Exception as e:
        print(f"Error deleting bucket {bucket_name}: {e}")

# Run the function
delete_bucket(agent_engine_bucket_name)
delete_bucket(cloud_build_bucket_name)
