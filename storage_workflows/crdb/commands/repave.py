import datetime
import json
import math
import os
import statistics
import sys
import time
import typer
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.crdb.metadata_db.metadata_db_operations import MetadataDBOperations
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.models.node import Node
from storage_workflows.crdb.models.jobs.changefeed_job import ChangefeedJob
from storage_workflows.crdb.slack.content_templates import ContentTemplate
from storage_workflows.global_change_log.global_change_log_gateway import GlobalChangeLogGateway
from storage_workflows.global_change_log.service_name import ServiceName
from storage_workflows.logging.logger import Logger
from storage_workflows.metadata_db.metadata_db_connection import MetadataDBConnection
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification

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
        or cluster.instances_not_in_service_exist()
        or cluster.paused_changefeed_jobs_exist()):
        raise Exception("Pre run check failed")
    else:
        logger.info("Check passed")

@app.command()
def refresh_etl_load_balancer(deployment_env, region, cluster_name):
    if deployment_env == 'staging':
        logger.info("Staging clusters doesn't have ETL load balancers.")
        return
    setup_env(deployment_env, region, cluster_name)
    load_balancer = ElasticLoadBalancer.find_elastic_load_balancer_by_cluster_name(cluster_name)
    old_lb_instances = load_balancer.instances
    old_instance_id_set = set(map(lambda old_instance: old_instance['InstanceId'], old_lb_instances))
    metadata_db_operations = MetadataDBOperations()
    old_instance_id_set.update(metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env))
    logger.info("Old instances: {}".format(old_instance_id_set))
    new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id},
                             filter(lambda instance: instance.instance_id not in old_instance_id_set, 
                                    AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances)))
    logger.info("New instances: {}".format(new_instances))
    if not new_instances:
        logger.warning("No new instances, no need to refresh. Step complete.")
        return
    load_balancer.register_instances(new_instances)
    if old_lb_instances:
        load_balancer.deregister_instances(old_lb_instances)
    new_instance_list = list(map(lambda instance: instance['InstanceId'], new_instances))
    lb_instance_list = list(map(lambda instance: instance['InstanceId'], load_balancer.instances))
    if set(new_instance_list) == set(lb_instance_list):
        logger.info("ETL load balancer refresh completed!")
    else:
        raise Exception("Instances don't match. ETL load balancer refresh failed!")

@app.command()
def mute_alerts(deployment_env, cluster_name):
    def make_alert_label_matcher(name, type, value):
        return {"name": name, "type": type, "value": value}
    cluster_name_label_matcher = make_alert_label_matcher("cluster", "EXACT", cluster_name+"_"+deployment_env)
    live_node_count_changed_label_matcher = make_alert_label_matcher("Description", "EXACT", "The count of live nodes has decreased")
    changefeed_stopped_label_matcher = make_alert_label_matcher("Description", "EXACT", "Changefeed is Stopped")
    underreplicated_range_label_matcher = make_alert_label_matcher("Description", "EXACT", "Underreplicated Range Detected")
    backup_failed_label_matcher = make_alert_label_matcher("Description", "EXACT", "Incremental or full backup failed.")
    slug_list = []
    slug_list.append(ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, live_node_count_changed_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule([cluster_name_label_matcher, changefeed_stopped_label_matcher]))
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
def extend_muting_rules(slugs:str):
    slug_list = json.loads(slugs)
    rules = list(map(lambda slug: ChronosphereApiGateway.read_muting_rule(slug), slug_list))
    ends_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
    ends_at = ends_at.strftime('%Y-%m-%dT%H:%M:%SZ')
    for rule in rules:
        comment = rule["comment"] if "comment" in rule else ""
        ChronosphereApiGateway.update_muting_rule(create_if_missing=False,
                                                  slug=rule["slug"],
                                                  name=rule["name"],
                                                  label_matchers=rule["label_matchers"],
                                                  starts_at=rule["starts_at"],
                                                  ends_at=ends_at,
                                                  comment=comment)
    logger.info("Extended ending time for following alerts to {}: {}".format(ends_at, slug_list))

@app.command()
def copy_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    old_instance_ips = set(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, instance_ids))
    nodes = Node.get_nodes()
    new_nodes = list(filter(lambda node: node.ip_address not in old_instance_ips, nodes))
    new_nodes.sort(key=lambda node: node.id)
    new_node = new_nodes[0]
    logger.info("Copying crontab jobs to new node: {}".format(new_node.id))
    for ip in old_instance_ips:
        ssh_client = SSH(ip)
        ssh_client.connect_to_node()
        stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")
        lines = stdout.readlines()
        errors = stderr.readlines()
        ssh_client.close_connection()
        logger.info("Listing cron jobs for {}: {}".format(ip, lines))
        if errors:
            continue
        new_node.copy_cron_scripts_from_old_node(ssh_client)
        new_node.schedule_cron_jobs(lines)
    logger.info("Copied all the crontab jobs to new node successfully!")

@app.command()
def read_and_increase_asg_capacity(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    initial_capacity = len(asg.instances)
    desired_capacity = 2*len(old_instance_ids)
    current_capacity = initial_capacity
    logger.info("Initial Capacity is:" + str(initial_capacity))
    logger.info("Desired Capacity is:" + str(desired_capacity))
    cluster = Cluster()

    if initial_capacity % 3 != 0 or current_capacity % 3 != 0:
        logger.error("The number of nodes in this cluster are not balanced.")
        raise Exception("Imbalanced cluster, exiting.")
        return

    all_new_instance_ids = []
    # current_capacity = initial_capacity
    while current_capacity < desired_capacity:
        #current_capacity determines number of nodes in standby + number of nodes in-service
        #according to asg desired_capacity is the count of number of nodes in-service state only
        #hence we only set intial_capacity+3 as desired capacity in each loop
        new_instance_ids = asg.add_ec2_instances(initial_capacity+3)
        all_new_instance_ids.append(new_instance_ids)
        AutoScalingGroupGateway.enter_instances_into_standby(asg.name, new_instance_ids)
        cluster.wait_for_hydration()
        asg.reload(cluster_name)
        current_capacity = len(asg.instances)
        logger.info("Current Capacity is:" + str(current_capacity))
    return

@app.command()
def exit_new_instances_from_standby(deployment_env, region, cluster_name):
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
def detach_old_instances_from_asg(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    logger.info(f"Autoscaling group name is {asg.name}")
    # get instance ids of old nodes
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    AutoScalingGroupGateway.detach_instance_from_autoscaling_group(old_instance_ids, asg.name)
    return

@app.command()
def terminate_instances(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    for id in old_instance_ids:
        ec2_instance = Ec2Instance.find_ec2_instance(id)
        ec2_instance.terminate_instance()

@app.command()
def stop_crdb_on_old_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    instances_ips = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, old_instance_ids))
    for ip in instances_ips:
        Node.stop_crdb(ip)

@app.command()
def drain_old_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    old_nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, old_instance_ids))
    for node in old_nodes:
        logger.info("Draining node {} ...".format(node.id))
        node.drain()
        logger.info("Draining complete for node {}".format(node.id))
    logger.info("Nodes drain complete!")

@app.command()
def decommission_old_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    old_nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, old_instance_ids))
    cluster = Cluster()
    cluster.decommission_nodes(old_nodes)
    logger.info("Decommission completed!")

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
    if deployment_env == "staging":
        logger.info("GCL skipped for staging.")
        return
    GlobalChangeLogGateway.post_event(deployment_env=deployment_env,
                                      service_name=ServiceName.CRDB,
                                      message="Repave completed for cluster {} in operator service.".format(cluster_name))

@app.command()
def start_repave_global_change_log(deployment_env, region, cluster_name):
    if deployment_env == "staging":
        logger.info("GCL skipped for staging.")
        return
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
    time.sleep(30)
    logger.info("Paused all changefeed jobs!")

    for job in changefeed_jobs:
        logger.info("Removing coordinator node for job {}".format(job.id))
        job.remove_coordinator_node()
    logger.info("Removed coordinator node for all changefeed jobs!")

    # get instance ids of old nodes
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    old_nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, old_instance_ids))
    old_node_ids = set(map(lambda node: node.id, old_nodes))
    for node_id in old_node_ids:
        logger.info(node_id)

    for job in changefeed_jobs:
        logger.info("Resuming changefeed job {}".format(job.id))
        job.resume()
        time.sleep(10)
        coordinator_node = None
        while coordinator_node is None:
            logger.info("Checking coordinator node.")
            # expected value in int, if this returns anything else exception would be thrown
            coordinator_node = int(job.get_coordinator_node())
            logger.info("Coordinator node is {}".format(coordinator_node))

            if coordinator_node in old_node_ids:
                coordinator_node = None
                logger.info("Removing coordinator node for job {}".format(job.id))
                job.remove_coordinator_node()
                logger.info("Pausing job {}".format(job.id))
                job.pause()
                time.sleep(10)
                job.resume()
                time.sleep(10)
        logger.info("Coordinator node updated to {}".format(coordinator_node))
    logger.info("Resumed all changefeed jobs!")

@app.command()
def persist_instance_ids(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    instance_ids = list(map(lambda instance: instance.instance_id, asg.instances))
    logger.info("Instance IDs to be persist: {}".format(instance_ids))
    metadata_db_operations = MetadataDBOperations()
    metadata_db_operations.persist_old_instance_ids(cluster_name, deployment_env, instance_ids)
    logger.info("Persist completed!")

@app.command()
def send_workflow_failure_notification(deployment_env, region, cluster_name, namespace, workflow_name):
    webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB') if deployment_env == 'prod' else os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB_STAGING')
    notification = SlackNotification(webhook_url)
    notification.send_notification(ContentTemplate.get_workflow_failure_content(namespace, 
                                                                                workflow_name,
                                                                                cluster_name,
                                                                                deployment_env,
                                                                                region))


if __name__ == "__main__":
    app()
