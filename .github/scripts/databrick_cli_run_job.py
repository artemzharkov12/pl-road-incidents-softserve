import argparse
import time
from databricks.sdk import WorkspaceClient

parser = argparse.ArgumentParser()

parser.add_argument("--job-name", required=True)

args = parser.parse_args()
client = WorkspaceClient()

target_job_id = None
for job in client.jobs.list():
    if args.job_name in job.settings.name:
        target_job_id = job.job_id
        break

if not target_job_id:
    print(f"Error: Job '{args.job_name}' not found.")
    exit(1)

response = client.jobs.run_now(job_id=target_job_id)
run_id = response.run_id
print(f"Run ID = {run_id}")

while True:
    info = client.jobs.get_run(run_id=run_id)
    life_cycle = info.state.life_cycle_state.value

    print(f"Current life cycle: {life_cycle}")
    # check result when already done 
    if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        result_state = info.state.result_state.value
        print(f"Final result state: {result_state}")
        
        if result_state == "SUCCESS":
            print("Job finished successfully!")
            break
        else:
            print("Fail")
            exit(1)
            
    time.sleep(20)

