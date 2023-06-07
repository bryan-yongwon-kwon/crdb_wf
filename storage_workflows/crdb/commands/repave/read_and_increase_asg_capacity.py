import typer
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.setup_env import setup_env

app = typer.Typer()

@app.command()
def read_and_increase_asg_capacity(cluster_name, deployment_env, region):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    asg_name = asg._api_response['AutoScalingGroupName']
    print("Autoscaling group name: " + asg_name)
    capacity = AutoScalingGroupGateway.get_auto_scaling_group_capacity(asg_name)
    # TODO: persist_asg_capacity(capacity)
    AutoScalingGroupGateway.update_auto_scaling_group_capacity(asg_name, 2*capacity)
    capacity = AutoScalingGroupGateway.get_auto_scaling_group_capacity(asg_name)
    # TODO: persist updated capacity persist_asg_capacity(capacity)
    return

if __name__ == "__main__":
    app()
