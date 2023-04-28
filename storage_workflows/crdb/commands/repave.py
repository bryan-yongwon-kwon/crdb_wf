import typer
from storage_workflows.crdb.operations.workflow_pre_run_check import WorkflowPreRunCheck
from storage_workflows.setup_env import setup_env

app = typer.Typer()

@app.command()
def repave_pre_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region)
    if (WorkflowPreRunCheck.backup_job_is_running(cluster_name)
        or WorkflowPreRunCheck.restore_job_is_running(cluster_name)
        or WorkflowPreRunCheck.schema_change_job_is_running(cluster_name)
        or WorkflowPreRunCheck.row_level_ttl_job_is_running(cluster_name)
        or WorkflowPreRunCheck.unhealthy_ranges_exist(cluster_name)
        or WorkflowPreRunCheck.instances_not_in_service_exist(cluster_name)):
        raise Exception("Pre run check failed")
    else:
        print("Check passed")


if __name__ == "__main__":
    app()