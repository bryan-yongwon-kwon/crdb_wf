import typer
from storage_workflows.crdb.operations.workflow_pre_run_check import WorkflowPreRunCheck
from storage_workflows.setup_env import setup_env
from storage_workflows.crdb.connect.ssh_client import SSHClient

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

@app.command()
def check_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region)
    ssh_client = SSHClient()
    ssh_client.connect_to_node("10.4.112.109")
    stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")
    print("stdin")
    print(stdin)
    print("stdout")
    print(stdout)
    print("stderr")
    print(stderr)


if __name__ == "__main__":
    app()