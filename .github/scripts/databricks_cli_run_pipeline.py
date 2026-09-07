import argparse
import time
from databricks.sdk import WorkspaceClient

parser = argparse.ArgumentParser()

# Search by a name (because prod and dev orgs have diferant pipeline ID)
parser.add_argument("--pipeline-name", required=True)
# parser.add_argument("--pipeline-id", required=True)
# parser.add_argument("--run-id", required=True)
# parser.add_argument("--full-refresh")
args = parser.parse_args()

client = WorkspaceClient()
print(f"Searching for pipeline with name: {args.pipeline_name}...")

# for different org pipeline id is different or if we do full refreash id would be required threfore search by name
target_pipeline_id = None
for pipeline in client.pipelines.list_pipelines():
    if args.pipeline_name in pipeline.name:
        target_pipeline_id = pipeline.pipeline_id
        break

if not target_pipeline_id:
    print(f"Error: Pipeline '{args.pipeline_name}' not found.")
    exit(1)

response = client.pipelines.start_update(
    pipeline_id = target_pipeline_id
)

update_id = response.update_id
print(f"Update ID = {update_id}. Monitoring status...")

while True:
    info = client.pipelines.get_update(
        pipeline_id = target_pipeline_id,
        update_id = update_id
    )
    
    state = info.update.state.value
    print("Current state:", state)
    
    if state == "COMPLETED":
        print("Pipeline finished successfully!")
        break
    elif state in ["FAILED", "CANCELED"]:
        print("Pipeline run failed or was canceled.")
        exit(1)
        
    time.sleep(20)