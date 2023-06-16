import typer
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.setup_env import setup_env
from storage_workflows.metadata_db.metadata_db_operations import MetadataDBOperations


app = typer.Typer()

@app.command
def read_and_increase_asg_capacity(cluster_name, deployment_env, region):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    capacity = asg.capacity
    print("ASG capacity: " + str(capacity))
    instances=[]
    for instance in asg.instances:
        instances.append(instance.instance_id)
    MetadataDBOperations.persist_asg_old_instance_ids(cluster_name, instances)
    delete_old_nodes_from_asg(asg.name, cluster_name)
    #AutoScalingGroupGateway.update_auto_scaling_group_capacity(asg.name, 2*capacity)

def delete_old_nodes_from_asg(asg_name, cluster_name):
    old_instances = MetadataDBOperations.get_old_nodes(cluster_name)
    AutoScalingGroupGateway.remove_instance_from_autoscaling_group(old_instances[0], asg_name)

if __name__ == "__main__":
    app()
