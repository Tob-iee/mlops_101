from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


DeploymentSpec(
    flow_location="infer_pipeline.py",
    name="duration_pred_pipeline",
    parameters={
        "taxi_type": "green",
        "run_id": "model.bin"
    },
    flow_storage="dbf2efd9-7a2e-4408-b41d-4db75d8b954d",
    schedule=CronSchedule(cron="0 3 2 * *"),
    flow_runner=SubprocessFlowRunner(),
    tags=["ml"]
)
