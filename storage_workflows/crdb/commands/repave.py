import typer
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from storage_workflows.crdb.operations.workflow_pre_run_check import WorkflowPreRunCheck
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.cluster.node import Node
from storage_workflows.setup_env import setup_env

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
def mute_alerts_repave(cluster_name):    
    def make_alert_label_matcher(name, type, value):
        return {"name": name, "type": type, "value": value}
    cluster_name_label_matcher = make_alert_label_matcher("cluster", "EXACT", cluster_name)
    live_node_count_changed_label_matcher = make_alert_label_matcher("Description", "EXACT", "The count of live nodes has decreased")
    changefeed_stoppped_label_matcher = make_alert_label_matcher("Description", "EXACT", "Changefeed is Stopped")    
    underreplicated_range_label_matcher = make_alert_label_matcher("Description", "EXACT", "Underreplicated Range Detected")
    backup_failed_label_matcher = make_alert_label_matcher("Description", "EXACT", "Incremental or full backup failed.")
    
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, live_node_count_changed_label_matcher])
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, changefeed_stoppped_label_matcher])
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, underreplicated_range_label_matcher])
    ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, backup_failed_label_matcher])

@app.command()
def decommission_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    ids = [1,2,3]
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    nodes = list(filter(lambda node: node.id in ids, Node.get_nodes()))
    connection.decommission_nodes(nodes)

if __name__ == "__main__":
    app()
