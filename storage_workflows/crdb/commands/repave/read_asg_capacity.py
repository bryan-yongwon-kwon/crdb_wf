import typer
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.setup_env import setup_env

app = typer.Typer()

@app.command()
def read_asg_capacity(cluster_name, deployment_env, region):
    setup_env(deployment_env, region, cluster_name)
    asg_name = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    capacity = AutoScalingGroupGateway.get_auto_scaling_group_capacity(asg_name)
    print("Read capacity:" + capacity)
    return insert_into_cluster_info(cluster_name, capacity)

def insert_into_cluster_info(cluster_name, node_count):
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    query = "INSERT INTO clusters_info (cluster_name, node_count) VALUES ('{}', {});".format(cluster_name, node_count)
    response = connection.execute_sql(query)
    print("response received: " + response)
    connection.close()
    return response

if __name__ == "__main__":
    app()