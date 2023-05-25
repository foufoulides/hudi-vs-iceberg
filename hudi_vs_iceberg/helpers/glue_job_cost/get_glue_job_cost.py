import boto3

DPU_HOUR_COST = 0.44

def get_job_cost(job_name: str,
                 job_id: str,
                 glue_client: boto3.client,
                 dpu_hour_cost: str=DPU_HOUR_COST):
    """get the cost of a glue job run"""

    r = glue_client.get_job_run(JobName=job_name,
                                RunId=job_id,
                                PredecessorsIncluded=False)

    dpu_seconds = r["JobRun"]["DPUSeconds"]
    cost = (dpu_seconds / 3600) * dpu_hour_cost

    return {
        "no_workers": r["JobRun"]["NumberOfWorkers"],
        "execution_time": r["JobRun"]["ExecutionTime"],
        "dpu_seconds": dpu_seconds,
        "cost": cost
    }
