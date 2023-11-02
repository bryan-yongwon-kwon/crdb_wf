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
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification

app = typer.Typer()
logger = Logger()

@app.command()
def pre_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    workflow_id = os.getenv('WORKFLOW-ID')
    cluster = Cluster()
    logger.info(f"workflow_id: {workflow_id}")
    if (cluster.backup_job_is_running()
        or cluster.restore_job_is_running()
        or cluster.schema_change_job_is_running()
        or cluster.row_level_ttl_job_is_running()
        or cluster.instances_not_in_service_exist()
        or cluster.paused_changefeed_jobs_exist()
        or cluster.unhealthy_ranges_exist()):
        raise Exception("Pre run check failed")
    else:
        logger.info(f"{cluster_name} Check passed")
        ChangefeedJob.persist_to_metadata_db(workflow_id, cluster_name)


def get_old_instance_ids(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} get_old_instance_ids")
    # STORAGE-7583: do nothing if scaling up
    metadata_db_operations = MetadataDBOperations()
    return metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)


@app.command()
def refresh_etl_load_balancer(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} refresh_etl_load_balancer")
    if deployment_env == 'staging':
        logger.info(f"{cluster_name} Staging clusters doesn't have ETL load balancers.")
        return
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    load_balancer = ElasticLoadBalancer.find_elastic_load_balancer_by_cluster_name(cluster_name)
    old_lb_instances = load_balancer.instances
    old_instance_id_set = set(map(lambda old_instance: old_instance['InstanceId'], old_lb_instances))
    metadata_db_operations = MetadataDBOperations()
    old_instance_id_set.update(metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env))
    logger.info(f"{cluster_name} Old instances: {old_instance_id_set}")
    new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id},
                             filter(lambda instance: instance.instance_id not in old_instance_id_set, 
                                    AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances)))
    logger.info(f"{cluster_name} New instances: {new_instances}")
    if not new_instances:
        logger.warning("No new instances, no need to refresh. Step complete.")
        return
    load_balancer.register_instances(new_instances)
    if old_lb_instances:
        load_balancer.deregister_instances(old_lb_instances)
    new_instance_list = list(map(lambda instance: instance['InstanceId'], new_instances))
    lb_instance_list = list(map(lambda instance: instance['InstanceId'], load_balancer.instances))
    if set(new_instance_list) == set(lb_instance_list):
        logger.info(f"{cluster_name} ETL load balancer refresh completed!")
    else:
        raise Exception("Instances don't match. ETL load balancer refresh failed!")

@app.command()
def mute_alerts(deployment_env, cluster_name, region='us-west-2'):
    logger.info(f"{cluster_name} mute_alerts")
    # TODO: update workflow template to pass in region for this step
    setup_env(deployment_env, region, cluster_name)
    def make_alert_label_matcher(name, type, value):
        return {"name": name, "type": type, "value": value}
    aws_region = region

    cluster_name_label_matcher = make_alert_label_matcher("cluster", "EXACT", cluster_name + "_" + deployment_env)
    live_node_count_decreased_label_matcher = make_alert_label_matcher("Description", "EXACT",
                                                                       "The count of live nodes has decreased")
    live_node_count_increased_label_matcher = make_alert_label_matcher("Description", "EXACT",
                                                                       "The count of live nodes has increased")  # New Matcher
    changefeed_stopped_label_matcher = make_alert_label_matcher("Description", "EXACT", "Changefeed is Stopped")
    underreplicated_range_label_matcher = make_alert_label_matcher("Description", "EXACT",
                                                                   "Underreplicated Range Detected")
    backup_failed_label_matcher = make_alert_label_matcher("Description", "EXACT", "Incremental or full backup failed.")

    # Create a label matcher for the AWS region
    region_label_matcher = make_alert_label_matcher("region", "EXACT", aws_region)

    slug_list = []
    slug_list.append(ChronosphereApiGateway.create_muting_rule(
        [cluster_name_label_matcher, live_node_count_decreased_label_matcher, region_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule(
        [cluster_name_label_matcher, live_node_count_increased_label_matcher, region_label_matcher]))  # New Mute Rule
    slug_list.append(ChronosphereApiGateway.create_muting_rule(
        [cluster_name_label_matcher, changefeed_stopped_label_matcher, region_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule(
        [cluster_name_label_matcher, underreplicated_range_label_matcher, region_label_matcher]))
    slug_list.append(ChronosphereApiGateway.create_muting_rule(
        [cluster_name_label_matcher, backup_failed_label_matcher, region_label_matcher]))

    output_file = open("/tmp/slugs.json", "w")
    output_file.write(json.dumps(slug_list))
    output_file.close()

@app.command()
def delete_mute_alerts(slugs:str):
    logger.info(f"Unmuting following rules: {slugs}")
    try:
        slug_list = json.loads(slugs)
    except json.decoder.JSONDecodeError:
        logger.error("Invalid input!")
        return
    for slug in slug_list:
        ChronosphereApiGateway.delete_muting_rule(slug)

@app.command()
def extend_muting_rules(slugs:str):
    try:
        slug_list = json.loads(slugs)
    except json.decoder.JSONDecodeError:
        logger.error("Invalid input!")
        logger.info(f"Will retry after sleeping 300s...")
        time.sleep(300)
        return
    slug_list = list(filter(lambda slug: ChronosphereApiGateway.muting_rule_exist(slug), slug_list))
    if not slug_list:
        logger.info(f"Rules don't exist, skip step.")
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
    logger.info(f"Extended ending time for following alerts to {ends_at}: {slug_list}")
    logger.info(f"Wait for 50 mins...")
    for count in range(5):
        rules_valid = any(list(map(lambda slug: ChronosphereApiGateway.muting_rule_exist(slug), slug_list)))
        if not rules_valid:
            logger.info(f"Muting rules deleted, step completed.")
            return
        time.sleep(600)

@app.command()
def copy_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    # STORAGE-7583: do nothing if scaling up
    if instance_ids:
        old_instance_ips = set(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, instance_ids))
        logger.info(f"{cluster_name} - copy_crontab - old_instance_ips - {old_instance_ips}")
        nodes = Node.get_nodes()
        new_nodes = list(filter(lambda node: node.ip_address not in old_instance_ips, nodes))
        logger.info(f"{cluster_name} - copy_crontab - new_nodes - {new_nodes}")
        new_nodes.sort(key=lambda node: node.id)
        new_node = new_nodes[0]
        logger.info(f"{cluster_name} Copying crontab jobs to new node: {new_node.id}")
        for ip in old_instance_ips:
            ssh_client = SSH(ip)
            ssh_client.connect_to_node()
            stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")
            lines = stdout.readlines()
            errors = stderr.readlines()
            ssh_client.close_connection()
            logger.info(f"{cluster_name} Listing cron jobs for {ip}: {lines}")
            if errors:
                continue
            new_node.copy_cron_scripts_from_old_node(ssh_client)
            new_node.schedule_cron_jobs(lines)
        logger.info(f"{cluster_name} Copied all the crontab jobs to new node successfully!")
    else:
        logger.info(f"{cluster_name} no instance_id found. skipping copy_crontab.")

@app.command()
def read_and_increase_asg_capacity(deployment_env, region, cluster_name, hydration_timeout_mins, desired_capacity=None):
    hydration_timeout_mins = int(hydration_timeout_mins)
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    logger.info(f"{cluster_name} retrieved following old_instance_ids:" + str(old_instance_ids))
    # STORAGE-7583: repurpose repave workflow to handle cluster scaling
    is_scaling_event = False
    if desired_capacity is None:
        logger.info(f"{cluster_name} desired_capacity not provided.")
        desired_capacity = 2*len(old_instance_ids)
        initial_capacity = len(old_instance_ids)
    else:
        logger.info(f"{cluster_name} desired_capacity provided: {desired_capacity}")
        is_scaling_event = True
        desired_capacity = int(desired_capacity)
        initial_capacity = len(asg.instances)
    current_capacity = len(asg.instances)
    logger.info(f"{cluster_name} Current Capacity at the beginning is:" + str(current_capacity))
    logger.info(f"{cluster_name} Initial Capacity is:" + str(initial_capacity))
    logger.info(f"{cluster_name} Desired Capacity is:" + str(desired_capacity))
    cluster = Cluster()

    if len(cluster.nodes) != len(asg.instances):
        raise Exception(f"{cluster_name} Instances count in ASG doesn't match nodes count in cluster.")

    # STORAGE-7583: also check desired_capacity
    if initial_capacity % 3 != 0 or current_capacity % 3 != 0 or desired_capacity % 3 != 0:
        logger.error("The number of nodes in this cluster are not balanced.")
        raise Exception(f"{cluster_name} Imbalanced cluster, exiting.")
        return

    # STORAGE-7583: if the cluster is scaling down, remove nodes
    if desired_capacity < current_capacity:
        logger.info(f"{cluster_name} is scaling down. selecting instances to remove...")
        # Calculate the number of instances to terminate
        instances_to_terminate = current_capacity - desired_capacity
        if instances_to_terminate <= 0:
            logger.info(f"{cluster_name} No instances to terminate.")
        else:
            # Get a list of instance IDs to terminate
            instance_ids_to_terminate = asg.get_instances_to_terminate(instances_to_terminate)
            logger.info(f"{cluster_name} instance_ids_to_terminate: {instance_ids_to_terminate}")

            if instance_ids_to_terminate:
                # set new old_instance_ids as nodes that are being removed
                persist_instance_ids_to_terminate(deployment_env, region, cluster_name, instance_ids_to_terminate)
                logger.info(f"{cluster_name}: upserted {len(instance_ids_to_terminate)} instances as old_instance_ids")
            else:
                logger.info(f"{cluster_name}: No instances selected for termination.")
    else:
        if is_scaling_event:
            # STORAGE-7583: we're scaling up. no instance removal needed. reset old_instance_ids in metadata_db.
            persist_instance_ids_to_terminate(deployment_env, region, cluster_name, [])
        all_new_instance_ids = []
        while current_capacity < desired_capacity:
            #current_capacity determines number of nodes in standby + number of nodes in-service
            #according to asg desired_capacity is the count of number of nodes in-service state only
            #hence we only set intial_capacity+3 as desired capacity in each loop
            new_instance_ids = asg.add_ec2_instances(initial_capacity+3, autoscale=True, deployment_env=deployment_env)
            logger.info(f"{cluster_name} adding instances to asg: {new_instance_ids}")
            all_new_instance_ids.append(new_instance_ids)
            logger.info(f"{cluster_name} set instances to standby")
            AutoScalingGroupGateway.enter_instances_into_standby(asg.name, new_instance_ids)
            logger.info(f"{cluster_name} waiting for hydration to complete. hydration_timeout_mins: {hydration_timeout_mins}")
            cluster.wait_for_hydration(hydration_timeout_mins)
            asg.reload(cluster_name)
            current_capacity = len(asg.instances)
            logger.info(f"{cluster_name} checking for az distribution")
            if not asg.check_equal_az_distribution_in_asg():
                raise Exception(f"{cluster_name} Imbalanced nodes added.")
    return

@app.command()
def exit_new_instances_from_standby(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    logger.info(f"{cluster_name} Autoscaling group name is {asg.name}")
    asg_instances = AutoScalingGroupGateway.describe_auto_scaling_groups_by_name(asg.name)[0]["Instances"]
    standby_instance_ids = []
    for instance in asg_instances:
        if instance["LifecycleState"] == "Standby":
            standby_instance_ids.append(instance["InstanceId"])

    # move instances out of standby 3 at a time
    for index in range(0, len(standby_instance_ids), 3):
        logger.info(f"{cluster_name} Moving following instances {standby_instance_ids[index:index+3]} out of standby mode.")
        AutoScalingGroupGateway.exit_instances_from_standby(asg.name, standby_instance_ids[index:index+3])

@app.command()
def detach_old_instances_from_asg(deployment_env, region, cluster_name, timeout_minus):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    logger.info(f"{cluster_name} Autoscaling group name is {asg.name}")
    # get instance ids of old nodes
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    # STORAGE-7583: do nothing if scaling up
    if old_instance_ids:
        for index in range(0, len(old_instance_ids), 12):
          AutoScalingGroupGateway.detach_instance_from_autoscaling_group(old_instance_ids[index:index+12], asg.name)
        cluster = Cluster()
        cluster.wait_for_connections_drain_on_old_nodes(int(timeout_minus))
        logger.info(f"{cluster_name} detached instances from asg")
        return
    else:
        logger.info(f"{cluster_name} no instances found. skipping detach instances from asg. ")


@app.command()
def terminate_instances(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    logger.info(f"{cluster_name} terminating instances")
    # STORAGE-7583: do nothing if scaling up
    if old_instance_ids:
        for id in old_instance_ids:
            ec2_instance = Ec2Instance.find_ec2_instance(id)
            ec2_instance.terminate_instance()
        logger.info(f"{cluster_name} terminated ec2 instances")
    else:
        logger.info(f"{cluster_name} no instances found. skipping ec2 instance termination.")

@app.command()
def stop_crdb_on_old_nodes(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} stopping old instances")
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    # STORAGE-7583: do nothing if scaling up
    if old_instance_ids:
        instances_ips = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, old_instance_ids))
        for ip in instances_ips:
            Node.stop_crdb(ip)
        logger.info(f"{cluster_name} stopped all crdb instances")
    else:
        logger.info(f"{cluster_name} no nodes found. skipping crdb process stop.")

@app.command()
def drain_old_nodes(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} draining old nodes")
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    # STORAGE-7583: do nothing if scaling up
    if old_instance_ids:
        old_nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, old_instance_ids))
        for node in old_nodes:
            logger.info(f"{cluster_name} Draining node {node.id} ...")
            node.drain()
            logger.info(f"{cluster_name} Draining complete for node {node.id}")
        logger.info(f"{cluster_name} Nodes drain complete!")
    else:
        logger.info(f"{cluster_name} No nodes to drain")


@app.command()
def decommission_old_nodes(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} decommission_old_nodes")
    setup_env(deployment_env, region, cluster_name)
    cluster_name = os.environ['CLUSTER_NAME']
    workflow_id = os.getenv('WORKFLOW-ID')
    if not handle_old_instances(cluster_name, deployment_env):
        logger.info(f"{cluster_name} No nodes to decommission")
        return
    logger.info(f"workflow_id: {workflow_id}")
    check_and_handle_changefeeds(cluster_name, workflow_id)
    logger.info(f"{cluster_name} Check passed")


def handle_old_instances(cluster_name, deployment_env):
    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    if not old_instance_ids:
        return False
    old_nodes = [Ec2Instance.find_ec2_instance(instance_id).crdb_node for instance_id in old_instance_ids]
    decommission_nodes_if_healthy(cluster_name, old_nodes)
    return True


def decommission_nodes_if_healthy(cluster_name, old_nodes):
    cluster = Cluster()
    if cluster.unhealthy_ranges_exist():
        raise Exception("Abort decommission, unhealthy ranges exist!")
    cluster.decommission_nodes(old_nodes)
    logger.info(f"{cluster_name} Decommission completed!")


def check_and_handle_changefeeds(cluster_name, workflow_id):
    check_changefeeds(cluster_name, workflow_id, "initial")
    time.sleep(30)
    check_changefeeds(cluster_name, workflow_id, "post-resume")


def check_changefeeds(cluster_name, workflow_id, stage):
    paused_changefeeds, failed_changefeeds, unexpected_changefeeds = ChangefeedJob.compare_current_to_persisted_metadata(
        cluster_name, workflow_id)
    if stage == "initial":
        for changefeed in paused_changefeeds:
            changefeed.resume()
    if failed_changefeeds:
        raise Exception("Found failed changefeeds after decommission.")
    if paused_changefeeds:
        raise Exception("Found paused changefeeds after decommission.")
    if unexpected_changefeeds:
        raise Exception(
            f"Found changefeeds with unexpected statuses after decommission: {[cf.id for cf in unexpected_changefeeds]}")


@app.command()
def resume_all_paused_changefeeds(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} resume_all_paused_changefeeds")
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    old_instance_ids = get_old_instance_ids(deployment_env, region, cluster_name)
    if old_instance_ids:
        changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
        paused_changefeed_jobs = list(filter(lambda job: job.status == 'paused', changefeed_jobs))
        for job in paused_changefeed_jobs:
            logger.info(f"{cluster_name} Resuming changefeed job {job.id}")
            job.resume()
        logger.info(f"{cluster_name} Resumed all paused changefeed jobs!")
    else:
        logger.info(f"{cluster_name} old_instance_ids not found. skipping resume_all_paused_changefeeds.")

@app.command()
def pause_all_changefeeds(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} pause_all_changefeeds")
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    old_instance_ids = get_old_instance_ids(deployment_env, region, cluster_name)
    if old_instance_ids:
        changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
        for job in changefeed_jobs:
            logger.info(f"{cluster_name} Pausing changefeed job {job.id}")
            job.pause()
        logger.info(f"{cluster_name} Paused all changefeed jobs!")
    else:
        logger.info(f"{cluster_name} no old_instance_ids found. skipping pause_all_changefeeds.")
    
@app.command()
def complete_repave_global_change_log(deployment_env, region, cluster_name):
    if deployment_env == "staging":
        logger.info(f"{cluster_name} GCL skipped for staging.")
        return
    GlobalChangeLogGateway.post_event(deployment_env=deployment_env,
                                      service_name=ServiceName.CRDB,
                                      message="Repave completed for cluster {} in operator service.".format(cluster_name))

@app.command()
def start_repave_global_change_log(deployment_env, region, cluster_name):
    if deployment_env == "staging":
        logger.info(f"{cluster_name} GCL skipped for staging.")
        return
    GlobalChangeLogGateway.post_event(deployment_env=deployment_env,
                                      service_name=ServiceName.CRDB,
                                      message="Repave started for cluster {} in operator service.".format(cluster_name))

@app.command()
def move_changefeed_coordinator_node(deployment_env, region, cluster_name):
    logger.info(f"{cluster_name} move_changefeed_coordinator_node")
    setup_env(deployment_env, region, cluster_name)
    cluster_name = os.environ['CLUSTER_NAME']
    old_instance_ids = get_old_instance_ids(deployment_env, region, cluster_name)

    if not old_instance_ids:
        return

    changefeed_jobs = ChangefeedJob.find_all_changefeed_jobs(cluster_name)
    valid_changefeed_jobs = [job for job in changefeed_jobs if job.status not in ["failed", "canceled"]]

    # Pause all jobs at once
    for job in valid_changefeed_jobs:
        logger.info(f"{cluster_name} Pausing changefeed job {job.id}")
        job.pause()

    # Check pause status
    for job in valid_changefeed_jobs:
        logger.info(f"{cluster_name} Checking to see if {job.id} is paused")
        job.wait_for_job_to_pause()

    logger.info(f"{cluster_name} Paused all changefeed jobs!")

    metadata_db_operations = MetadataDBOperations()
    old_instance_ids = metadata_db_operations.get_old_instance_ids(cluster_name, deployment_env)
    old_nodes = [Ec2Instance.find_ec2_instance(instance_id).crdb_node for instance_id in old_instance_ids]
    old_node_ids = set(node.id for node in old_nodes)
    nodes = Node.get_nodes()
    new_nodes = [node for node in nodes if
                 node.ip_address not in [Ec2Instance.find_ec2_instance(instance_id).private_ip_address for instance_id
                                         in old_instance_ids]]

    node_index = 0
    num_new_nodes = len(new_nodes)

    for job in valid_changefeed_jobs:
        target_node = new_nodes[node_index]
        ssh_client = SSH(target_node.ip_address)
        ssh_client.connect_to_node()

        # Try assigning the coordinator directly, or isolating the environment as discussed above
        # Then, resume the job
        ssh_client.execute_command(f"crdb sql -e \"resume job {job.id}\"")
        ssh_client.close_connection()

        coordinator_node = job.get_coordinator_node()
        retries = 3  # or any desired number
        while coordinator_node in old_node_ids and retries:
            logger.info(f"{cluster_name} Coordinator node is {coordinator_node}. It's an old node.")
            job.remove_coordinator_node()
            time.sleep(10)
            coordinator_node = job.get_coordinator_node()
            retries -= 1

        if coordinator_node in old_node_ids:
            logger.error(f"Failed to move coordinator node for job {job.id}")
            continue  # or take other corrective measures

        logger.info(f"{cluster_name} Coordinator node updated to {coordinator_node}")
        node_index = (node_index + 1) % num_new_nodes

    logger.info(f"{cluster_name} Resumed all changefeed jobs!")

@app.command()
def persist_instance_ids(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    metadata_db_operations = MetadataDBOperations()
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    instance_ids = list(map(lambda instance: instance.instance_id, asg.instances))
    logger.info(f"{cluster_name} Instance IDs to be persist: {instance_ids}")
    metadata_db_operations.persist_old_instance_ids(cluster_name, deployment_env, instance_ids)
    logger.info(f"{cluster_name} Persist completed!")


def persist_instance_ids_to_terminate(deployment_env, region, cluster_name, instance_ids):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    metadata_db_operations = MetadataDBOperations()
    logger.info(f"{cluster_name} Instance IDs to be persist: {instance_ids}")
    metadata_db_operations.persist_old_instance_ids(cluster_name, deployment_env, instance_ids)
    logger.info(f"{cluster_name} Persist completed!")


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
