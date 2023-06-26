import typer
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.metadata_db.metadata_db_operations import MetadataDBOperations
from storage_workflows.crdb.models.node import Node
from storage_workflows.crdb.models.jobs.changefeed_job import ChangefeedJob
from storage_workflows.setup_env import setup_env
from storage_workflows.logging.logger import Logger

app = typer.Typer()
logger = Logger()

@app.command()
def pre_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    if (cluster.backup_job_is_running()
        or cluster.restore_job_is_running()
        or cluster.schema_change_job_is_running()
        or cluster.row_level_ttl_job_is_running()
        or cluster.unhealthy_ranges_exist()
        or cluster.instances_not_in_service_exist()):
        raise Exception("Pre run check failed")
    else:
        logger.info("Check passed")

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
def read_and_increase_asg_capacity(cluster_name, deployment_env, region):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    capacity = asg.capacity
    print("ASG capacity: " + str(capacity))
    instances=[]
    for instance in asg.instances:
        instances.append(instance.instance_id)
    MetadataDBOperations.persist_asg_old_instance_ids(cluster_name, instances)
    #detach_old_nodes_from_asg(asg.name, cluster_name)
    #AutoScalingGroupGateway.update_auto_scaling_group_capacity(asg.name, 2*capacity)
    return

@app.command()
def detach_old_nodes_from_asg(asg_name, cluster_name):
    old_instances = MetadataDBOperations.get_old_nodes(cluster_name)
    AutoScalingGroupGateway.detach_instance_from_autoscaling_group(old_instances[0], asg_name)
    return

@app.command()
def terminate_instances(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    instance_ids = [] # place holder, should get instance ids from metadata database
    for id in instance_ids:
        ec2_instance = Ec2Instance.find_ec2_instance(id)
        ec2_instance.terminate_instance()

@app.command()
def stop_crdb_on_old_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    instance_ids = [] # place holder, should get instance ids from metadata database
    instances_ips = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, instance_ids))
    nodes = list(filter(lambda node: node.ip_address in instances_ips, Node.get_nodes()))
    for node in nodes:
        node.stop_crdb()

@app.command()
def resume_all_paused_changefeeds(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
    paused_changefeed_jobs = list(filter(lambda job: job.status == 'paused', changefeed_jobs))
    for job in paused_changefeed_jobs:
        print("Resuming changefeed job {}".format(job.id))
        job.resume()
    print("Resumed all paused changefeed jobs!")

@app.command()
def pause_all_changefeeds(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
    for job in changefeed_jobs:
        print("Pausing changefeed job {}".format(job.id))
        job.pause()
    print("Paused all changefeed jobs!")
    


if __name__ == "__main__":
    app()
