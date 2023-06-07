import typer
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.setup_env import setup_env

app = typer.Typer()

@app.command()
def read_asg_capacity(cluster_name, deployment_env, region):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    print("Autoscaling group name: " + asg._api_response['AutoScalingGroupName'])
    capacity = AutoScalingGroupGateway.get_auto_scaling_group_capacity(asg._api_response['AutoScalingGroupName'])
    print("Read capacity:" + str(capacity))
    return capacity

if __name__ == "__main__":
    app()
