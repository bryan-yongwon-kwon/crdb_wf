import typer
import time
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from storage_workflows.crdb.operations.workflow_pre_run_check import WorkflowPreRunCheck
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.crdb.cluster.node import Node
from storage_workflows.setup_env import setup_env
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection

app = typer.Typer()

@app.command()
def pre_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
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
def refresh_etl_load_balancer(deployment_env, region, cluster_name):
    if deployment_env == 'staging':
        print("Staging clusters doesn't have ETL load balancers.")
        return
    setup_env(deployment_env, region, cluster_name)
    etl_load_balancer_name = (cluster_name.replace("_", "-") + "-crdb-etl")[:32]
    load_balancers = ElasticLoadBalancer.find_elastic_load_balancers([etl_load_balancer_name])
    if not load_balancers:
        print("Mode not enabled. ETL load balancer doesn't exist.")
        return
    old_instances = load_balancers[0].instances
    print("Old instances: {}".format(old_instances))
    new_instances = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances
    new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id}, new_instances))
    print("New instances: {}".format(new_instances))
    if old_instances:
        ElasticLoadBalancerGateway.deregister_instances_from_load_balancer(etl_load_balancer_name, old_instances)
    if new_instances:
        ElasticLoadBalancerGateway.register_instances_with_load_balancer(etl_load_balancer_name, new_instances)

@app.command()
def drain_node(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    nodes = Node.get_nodes()
    old_nodes = nodes.sort(key=lambda node: node.started_at, reverse=False)[0:len(nodes)/2]
    crdb_conn = CrdbConnection.get_crdb_connection(cluster_name=cluster_name)
    for node in old_nodes:
        print("node drain start: {}".format(node.id))
        crdb_conn.drain_node(node)
        print("node drain complete: {}".format(node.id))


@app.command()
def mute_alerts_repave(cluster_name):    
    cluster_name_label_matcher = {
            "name": "cluster",
            "type": "EXACT",
            "value": cluster_name
        }
    
    live_node_count_changed_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "The count of live nodes has decreased"
        }

    changefeed_stoppped_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "Changefeed is Stopped"
        }
    
    underreplicated_range_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "Underreplicated Range Detected"
        }
    
    backup_failed_label_matcher = {
            "name": "Description",
            "type": "EXACT",
            "value": "Incremental or full backup failed."
        }
    
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, live_node_count_changed_label_matcher])
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, changefeed_stoppped_label_matcher])
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, underreplicated_range_label_matcher])
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, backup_failed_label_matcher])

if __name__ == "__main__":
    app()