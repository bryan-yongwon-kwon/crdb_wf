import json
import math
import statistics
import sys
import time
import typer
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from storage_workflows.crdb.metadata_db.metadata_db_operations import MetadataDBOperations
from storage_workflows.crdb.models.node import Node
from storage_workflows.crdb.models.jobs.changefeed_job import ChangefeedJob
from storage_workflows.setup_env import setup_env
from storage_workflows.logging.logger import Logger
from storage_workflows.global_change_log.global_change_log_gateway import GlobalChangeLogGateway
from storage_workflows.global_change_log.service_name import ServiceName

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
        logger.info("Staging clusters doesn't have ETL load balancers.")
        return
    setup_env(deployment_env, region, cluster_name)
    etl_load_balancer_name = (cluster_name.replace("_", "-") + "-crdb-etl")[:32]
    load_balancers = ElasticLoadBalancer.find_elastic_load_balancers([etl_load_balancer_name])
    if not load_balancers:
        logger.warning("Mode not enabled. ETL load balancer doesn't exist.")
        return
    old_instances = load_balancers[0].instances
    logger.info("Old instances: {}".format(old_instances))
    new_instances = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances
    new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id}, new_instances))
    logger.info("New instances: {}".format(new_instances))
    if old_instances:
        ElasticLoadBalancerGateway.deregister_instances_from_load_balancer(etl_load_balancer_name, old_instances)
    if new_instances:
        ElasticLoadBalancerGateway.register_instances_with_load_balancer(etl_load_balancer_name, new_instances)

@app.command()
def mute_alerts_repave(deployment_env, cluster_name):    
    def make_alert_label_matcher(name, type, value):
        return {"name": name, "type": type, "value": value}
    cluster_name_label_matcher = make_alert_label_matcher("cluster", "EXACT", cluster_name+"_"+deployment_env)
    live_node_count_changed_label_matcher = make_alert_label_matcher("Description", "EXACT", "The count of live nodes has decreased")
    changefeed_stoppped_label_matcher = make_alert_label_matcher("Description", "EXACT", "Changefeed is Stopped")    
    underreplicated_range_label_matcher = make_alert_label_matcher("Description", "EXACT", "Underreplicated Range Detected")
    backup_failed_label_matcher = make_alert_label_matcher("Description", "EXACT", "Incremental or full backup failed.")
    slug_list = []
    slug_list.append(ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, live_node_count_changed_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, changefeed_stoppped_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, underreplicated_range_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, backup_failed_label_matcher]))
    output_file = open("/tmp/slugs.json", "w")
    output_file.write(json.dumps(slug_list))
    output_file.close()

@app.command()
def delete_mute_alerts(slugs:str):
    logger.info("Unmuting following rules: {}".format(slugs))
    slug_list = json.loads(slugs)
    for slug in slug_list:
        ChronosphereApiGateway.delete_muting_rule(slug)
    


@app.command()
def read_and_increase_asg_capacity(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    initial_capacity = asg.capacity
    logger.info("ASG capacity: " + str(initial_capacity))
    instances=[]
    for instance in asg.instances:
        instances.append(instance.instance_id)
    metadata_db_operations = MetadataDBOperations()
    metadata_db_operations.persist_asg_old_instance_ids(cluster_name, deployment_env, instances)

    if initial_capacity % 3 != 0:
        logger.error("The number of nodes in this cluster are not balanced.")
        raise Exception("Imbalanced cluster, exiting.")
        return
    all_new_instance_ids=[]
    current_capacity = initial_capacity
    while current_capacity < 2*initial_capacity:
        current_capacity+=3
        new_instance_ids = add_ec2_instances(asg.name, current_capacity)
        all_new_instance_ids.append(new_instance_ids)
        AutoScalingGroupGateway.enter_instances_into_standby(asg.name, new_instance_ids)
        wait_for_hydration(asg.name)
    return


def add_ec2_instances(asg_name, desired_capacity):
    asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg_name)[0]["Instances"]
    initial_capacity = len(asg_instances)
    old_instance_ids = set()
    # Retrieve the existing instance IDs
    for instance in asg_instances:
        old_instance_ids.add(instance["InstanceId"])

    AutoScalingGroupGateway.update_auto_scaling_group_capacity(asg_name, desired_capacity)
    # Wait for the new instances to be added to the Auto Scaling group
    while True:
        asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg_name)[0]["Instances"]
        new_instance_ids = set()  # Store new instance IDs
        # Retrieve the instance IDs of the newly added instances
        for instance in asg_instances:
            if instance["InstanceId"] not in old_instance_ids and instance["LifecycleState"] == "InService":
                new_instance_ids.add(instance["InstanceId"])
        # Check if all new instances are found
        if len(new_instance_ids) == desired_capacity-initial_capacity:
            logger.info("All new instances are ready.")
            break
        # Wait before checking again
        time.sleep(10)

    return list(new_instance_ids)


def wait_for_hydration(asg_name):
    asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg_name)[0]["Instances"]
    instance_ids=[]
    for instance in asg_instances:
        instance_ids.append(instance["InstanceId"])
    nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, instance_ids))

    while True:
        nodes_replications_dict = {node: node.replicas for node in nodes}
        replications_values = list(nodes_replications_dict.values())
        avg_replications = statistics.mean(replications_values)
        outliers = [node for node, replications in nodes_replications_dict.items() if abs(replications - avg_replications) / avg_replications > 0.1]
        if not any(outliers):
            logger.info("Hydration complete")
            break
        logger.info("Waiting for nodes to hydrate.")
        for outlier in outliers:
            logger.info(f"Node: {outlier.id} Replications: {nodes_replications_dict[outlier]}")
        time.sleep(60)

    return


@app.command()
def exit_new_nodes_from_standby(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    logger.info(f"Autoscaling group name is {asg.name}")
    asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg.name)[0]["Instances"]
    standby_instance_ids = []
    for instance in asg_instances:
        if instance["LifecycleState"] == "Standby":
            standby_instance_ids.append(instance["InstanceId"])

    # move instances out of standby 3 at a time
    for index in range(0, len(standby_instance_ids), 3):
        logger.info(f"Moving following instances {standby_instance_ids[index:index+3]} out of standby mode.")
        AutoScalingGroupGateway.exit_instances_from_standby(asg.name, standby_instance_ids[index:index+3])


@app.command()
def detach_old_nodes_from_asg(asg_name, cluster_name):
    old_instances = MetadataDBOperations.get_old_nodes(cluster_name)
    AutoScalingGroupGateway.detach_instance_from_autoscaling_group(old_instances[0], asg_name)
    return

@app.command()
def terminate_instances(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_nodes(cluster_name, deployment_env)
    for id in instance_ids:
        ec2_instance = Ec2Instance.find_ec2_instance(id)
        ec2_instance.terminate_instance()

@app.command()
def stop_crdb_on_old_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_nodes(cluster_name, deployment_env)
    instances_ips = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, instance_ids))
    for ip in instances_ips:
        Node.stop_crdb(ip)

@app.command()
def decommission_old_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_nodes(cluster_name, deployment_env)
    nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, instance_ids))
    cluster = Cluster()
    cluster.decommission_nodes(nodes)

@app.command()
def resume_all_paused_changefeeds(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
    paused_changefeed_jobs = list(filter(lambda job: job.status == 'paused', changefeed_jobs))
    for job in paused_changefeed_jobs:
        logger.info("Resuming changefeed job {}".format(job.id))
        job.resume()
    logger.info("Resumed all paused changefeed jobs!")

@app.command()
def pause_all_changefeeds(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
    for job in changefeed_jobs:
        logger.info("Pausing changefeed job {}".format(job.id))
        job.pause()
    logger.info("Paused all changefeed jobs!")
    
@app.command()
def complete_repave_global_change_log(deployment_env, region, cluster_name):
    GlobalChangeLogGateway.post_event(deployment_env=deployment_env,
                                      service_name=ServiceName.CRDB,
                                      message="Repave completed for cluster {} in operator service.".format(cluster_name))

@app.command()
def start_repave_global_change_log(deployment_env, region, cluster_name):
    GlobalChangeLogGateway.post_event(deployment_env=deployment_env,
                                      service_name=ServiceName.CRDB,
                                      message="Repave started for cluster {} in operator service.".format(cluster_name))


@app.command()
def move_changefeed_coordinator_node(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
    for job in changefeed_jobs:
        logger.info("Pausing changefeed job {}".format(job.id))
        job.pause()
    logger.info("Paused all changefeed jobs!")

    for job in changefeed_jobs:
        logger.info("Removing coordinator node for job {}".format(job.id))
        job.remove_coordinator_node()
    logger.info("Removed coordinator node for all changefeed jobs!")

    # get old nodes
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_nodes(cluster_name, deployment_env)
    old_nodes = set(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, instance_ids))

    for job in changefeed_jobs:
        logger.info("Resuming changefeed job {}".format(job.id))
        job.resume()
        time.sleep(10)
        coordinator_node = None
        while coordinator_node is None:
            logger.info("Checking coordinator node.")
            coordinator_node = job.get_coordinator_node()
            if coordinator_node in old_nodes:
                coordinator_node = None
                logger.info("Removing coordinator node for job {}".format(job.id))
                job.remove_coordinator_node()
                logger.info("Pausing job {}".format(job.id))
                job.pause()
                time.sleep(30)
        logger.info("Coordinator node updated to {}".format(coordinator_node))
    logger.info("Resumed all changefeed jobs!")


if __name__ == "__main__":
    app()
