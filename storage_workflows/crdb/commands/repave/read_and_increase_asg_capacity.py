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
    capacity = asg.capacity
    print("ASG capacity: " + str(capacity))
    # TODO: persist_asg_capacity(capacity)
    AutoScalingGroupGateway.update_auto_scaling_group_capacity(asg.name, 2*capacity)
    # TODO: persist updated capacity
    return

if __name__ == "__main__":
    app()
