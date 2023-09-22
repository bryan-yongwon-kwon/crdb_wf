import json
import os
import typer
import logging
from collections import defaultdict
import psycopg2
import requests
import csv
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
# from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.slack.content_templates import ContentTemplate
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification, send_to_slack
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.metadata_db.storage_metadata.storage_metadata import StorageMetadata
from storage_workflows.crdb.api_gateway.iam_gateway import IamGateway

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def get_cluster_names(deployment_env, region):
    setup_env(deployment_env, region)
    asgs = AutoScalingGroup.find_all_auto_scaling_groups([AutoScalingGroup.build_filter_by_crdb_tag()])
    names = list(map(lambda asg: asg.name.split("_{}-".format(deployment_env))[0], asgs))
    names.sort()
    logger.info("Found {} clusters.".format(len(names)))
    logger.info(names)
    output_file = open("/tmp/cluster_names.json", "w")
    output_file.write(json.dumps(names))
    output_file.close()


# the deployment_env is either staging or prod
# the region is us-west-2 in most cases
# the cluster name is the one with underscore like ao_test
@app.command()
def asg_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "asg_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    # debugging_start
    instance_info = [(instance.instance_id, instance.health_status) for instance in asg.instances]
    info_str = ', '.join([f"ID: {id}, Health: {status}" for id, status in instance_info])
    logger.info(f"{cluster_name}: Instances: {info_str}")
    # debugging_end
    # Filtering out only the unhealthy instances
    unhealthy_asg_instances = [instance for instance in asg.instances if instance.health_status == 'Unhealthy']
    # Extracting instance_ids from the filtered instances
    unhealthy_asg_instance_ids = [instance.instance_id for instance in unhealthy_asg_instances]
    if unhealthy_asg_instances:
        logger.warning(f"{cluster_name}: Displaying all unhealthy instances for the {cluster_name} cluster: "
                       f"{unhealthy_asg_instance_ids}")
        logger.warning(f"{cluster_name}: Auto Scaling Group name: {asg.name}")
        # TODO: provide useful output
        check_output = unhealthy_asg_instance_ids
        check_result = "fail"
    else:
        # TODO: provide useful output
        check_output = "asg_health_check_passed"
        check_result = "pass"
        logger.info(f"{cluster_name}: asg_healthcheck_passed")
    # save results to metadatadb
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result,
                                         check_output=check_output)
    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def changefeed_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "changefeed_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    cluster = Cluster()
    try:
        unhealthy_changefeed_jobs = list(
            filter(lambda job: job.status != "running" and job.status != "canceled", cluster.changefeed_jobs))
        if unhealthy_changefeed_jobs:
            logger.warning(f"{cluster_name}: Changefeeds Not Running:")
            # TODO: provide useful output
            check_output = str(unhealthy_changefeed_jobs)
            check_result = "fail"
            for job in unhealthy_changefeed_jobs:
                logger.warning(f"{cluster_name}: Job id is {job.id}. Status is {job.status}.")
        else:
            logger.info(f"{cluster_name}: {check_type} passed")
            # TODO: provide useful output
            check_output = "changefeed_health_check_passed"
            check_result = "pass"
    except (psycopg2.DatabaseError, ValueError) as error:
        #logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "fail"
    # save results to metadatadb
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def orphan_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "orphan_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    # Get the count of AWS instances
    instances_with_cluster_tag = Ec2Instance.find_ec2_instances_by_cluster_tag(cluster_name)
    aws_cluster_instances = list(
        filter(lambda instance: instance.state != "terminated" and instance.state != "shutting-down",
               instances_with_cluster_tag))
    aws_cluster_instance_count = len(aws_cluster_instances)
    try:
        # Get the IP count of CRDB nodes
        find_crdb_node_ip_sql = "select address from crdb_internal.kv_node_status;"
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        crdb_node_ips = connection.execute_sql(find_crdb_node_ip_sql)
        connection.close()
        crdb_cluster_instance_count = len(crdb_node_ips)
        logger.info(f"{cluster_name} crdb_node_ips: {crdb_node_ips}")
        # Compare the IP count of AWS instances and CRDB nodes
        if aws_cluster_instance_count != crdb_cluster_instance_count:
            orphan_instances = list(
                map(lambda instance: {"InstanceId": instance.instance_id, "PrivateIpAddress": instance.private_ip_address},
                    filter(lambda instance: instance.private_ip_address not in crdb_node_ips, aws_cluster_instances)))
            logger.warning(f"{cluster_name}: Orphan instances found")
            logger.warning(f"{cluster_name}: AWS instance count is {aws_cluster_instance_count} and CRDB instance count is {crdb_cluster_instance_count}.")
            logger.warning(f"{cluster_name}: Orphan instances are: {orphan_instances}")
            # TODO: provide useful output
            check_output = f"Orphan instances are: {orphan_instances}"
            check_result = "fail"
        else:
            logger.info(f"{cluster_name}: No orphan instances found.")
            # TODO: provide useful output
            check_output = "orphan_health_check_passed"
            check_result = "pass"
    except (psycopg2.DatabaseError, ValueError) as error:
        #logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "fail"
    # save results to metadatadb
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)
    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def ptr_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "ptr_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    FIND_PTR_SQL = ("select (ts/1000000000)::int::timestamp as \"pts timestamp\", now()-(("
                    "ts/1000000000)::int::timestamp) as \"pts age\", *,crdb_internal.cluster_name() from "
                    "system.protected_ts_records where ((ts/1000000000)::int::timestamp) < now() - interval '2d';")
    try:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(FIND_PTR_SQL)
        contains_ptr = any(response)
        if contains_ptr:
            logger.warning(f"{cluster_name}: Protected timestamp records found")
            check_output = str(response)
            check_result = "fail"
        else:
            logger.info(f"{cluster_name}: Protected timestamp record not found")
            check_output = "ptr_health_check_passed"
            check_result = "pass"
        connection.close()
    except (psycopg2.DatabaseError, ValueError) as error:
        #logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "fail"

    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def send_slack_notification(deployment_env, message):
    # TODO: Read healthy check result from metadata DB
    results = ["text1", "text2"]  # this is a place holder
    webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB') if deployment_env == 'prod' else os.getenv(
        'SLACK_WEBHOOK_STORAGE_ALERTS_CRDB_STAGING')
    # webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERT_TEST')
    notification = SlackNotification(webhook_url)
    notification.send_notification(ContentTemplate.get_health_check_template(message))


@app.command()
def etl_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "etl_health_check"
    check_output = "{}"
    logger.info(f"{cluster_name}: starting {check_type}")
    if deployment_env == 'staging':
        logger.info(f"{cluster_name}: Staging clusters doesn't have ETL load balancers.")
        return
    load_balancer = ElasticLoadBalancer.find_elastic_load_balancer_by_cluster_name(cluster_name)
    if load_balancer is not None:
        old_lb_instances = load_balancer.instances
        old_instance_id_set = set(map(lambda old_instance: old_instance['InstanceId'], old_lb_instances))
        logger.info(f"{cluster_name}: Old instances: {old_instance_id_set}")
        new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id},
                                 filter(lambda instance: instance.instance_id not in old_instance_id_set,
                                        AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances)))
        logger.info(f"{cluster_name}: New instances: {new_instances}")
        if not new_instances:
            logger.warning(f"{cluster_name}: No new instances, no need to refresh. Step complete.")
            check_output = "no_action_needed"
            check_result = "pass"
            # write results to storage_metadata
            storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                                 aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                                 check_type=check_type, check_result=check_result,
                                                 check_output=check_output)
            return
        if load_balancer.register_instances(new_instances):
            if old_lb_instances:
                load_balancer.deregister_instances(old_lb_instances)
            new_instance_list = list(map(lambda instance: instance['InstanceId'], new_instances))
            lb_instance_list = list(map(lambda instance: instance['InstanceId'], load_balancer.instances))
            if set(new_instance_list) == set(lb_instance_list):
                logger.info(f"{cluster_name}: ETL load balancer refresh completed!")
                check_result = "pass"
                check_output = "etl_loadbalancer_refreshed"
            else:
                check_result = "fail"
                check_output = "etl_loadbalancer_refresh_failed"
        else:
            logger.info(f"{cluster_name}: Invalid instance found while registering instances on etl loadbalancer. Skipping...")
            check_result = "skipped"
            check_output = "etl_loadbalancer_registration_failed"
    else:
        logger.info(f"{cluster_name}: ETL load balancer not found. Skipping...")
        check_result = "skipped"
        check_output = "etl_loadbalancer_not_found"

    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


def extract_major_minor_from_tag(tag):
    """
    Extracts the major.minor version from the tag (e.g., "v22.2.13" -> "22.2").
    """
    parts = tag[1:].split('.')  # Remove the 'v' prefix and split by dot
    return f"{parts[0]}.{parts[1]}"


@app.command()
def version_mismatch_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "version_mismatch_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    crdb_sql_version = ("SELECT node_id, server_version, tag FROM crdb_internal.kv_node_status;")
    crdb_cluster_version = ("show cluster setting version;")
    try:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(crdb_sql_version)
        connection.connect()
        cluster_ver_response = connection.execute_sql(crdb_cluster_version)
        connection.close()
        # save sql response
        check_output = response
        # init dict
        tag_dict = defaultdict(list)
        mismatched_nodes = []
        # debug response
        logger.info(f"{cluster_name} response: {response}")
        logger.info(f"{cluster_name} cluster setting version: {cluster_ver_response[0][0]}")
        for node in response:
            node_id, server_version, tag = node
            major_minor_from_tag = extract_major_minor_from_tag(tag)
            tag_dict[tag].append(node_id)
            if server_version != major_minor_from_tag:
                mismatched_nodes.append(node_id)
        if mismatched_nodes:
            logger.warning(f"{cluster_name}: server version mismatch detected. cluster version is {cluster_ver_response}"
                           f". cluster upgrade may be incomplete.")
            logger.warning(f"{cluster_name}: Nodes with IDs {', '.join(map(str, mismatched_nodes))} have server_version"
                           f" not matching the major.minor part of their tag.")
            check_output = str(mismatched_nodes)
            check_result = "fail"
            storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env,
                                                 region=region,
                                                 aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                                 check_type=check_type, check_result=check_result,
                                                 check_output=check_output)
        elif len(tag_dict) > 1:
            logger.warning(f"{cluster_name}: Nodes have mismatched tags.")
            for tag_version, node_ids in tag_dict.items():
                logger.warning(f"Tag {tag_version} is found on nodes: {', '.join(map(str, node_ids))}")
            check_output = str(tag_dict.items())
            check_result = "fail"
            storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env,
                                                 region=region,
                                                 aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                                 check_type=check_type, check_result=check_result,
                                                 check_output=check_output)
        else:
            logger.info(f"{cluster_name}: All nodes have server_version matching the major.minor part of their tag.")
            check_output = "All nodes have server_version matching the major.minor part of their tag."
            check_result = "pass"
            storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env,
                                                 region=region,
                                                 aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                                 check_type=check_type, check_result=check_result,
                                                 check_output=check_output)
    except (psycopg2.DatabaseError, ValueError) as error:
        #logger.error(f"{cluster_name}: encountered error - {error}")
        check_result = "fail"
        check_output = "db_connection_error"
        storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                             aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                             check_type=check_type, check_result=check_result,
                                             check_output=check_output)
    logger.info(f"{cluster_name}: {check_type} complete")


def az_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    #logger.info("account_alias: %s", aws_account_alias)
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "az_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    if region == 'us-west-2':
        availability_zones = ['us-west-2a', 'us-west-2b', 'us-west-2c']
    else:
        availability_zones = ['eu-west-1a', 'eu-west-1b', 'eu-west-1c']
    # Get the count of AWS instances
    counts = {}
    for az in availability_zones:
        instances_with_cluster_tag = Ec2Instance.find_ec2_instances_by_cluster_tag(cluster_name)
        aws_cluster_instances = list(
            filter(lambda instance: instance.state != "terminated" and instance.state != "shutting-down",
                   instances_with_cluster_tag))
        counts[az] = len(aws_cluster_instances)
    logger.info(f"{cluster_name}: Node counts in each availability zone:")
    for az, count in counts.items():
        logger.info("{}: {} nodes".format(az, count))
    unique_counts = set(counts.values())
    if len(unique_counts) == 1:
        logger.info(f"{cluster_name}: All availability zones have the same number of nodes!")
        check_result = "pass"
    else:
        logger.info(f"{cluster_name}: Mismatch in node counts across availability zones!")
        check_result = "fail"
    check_output = "{}"
    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def zone_config_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "zone_config_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    # check that default replication factor is five
    FIND_ZONE_CONFIG_SQL = "select raw_config_sql from [show zone configuration from range default]"
    try:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        response = connection.execute_sql(FIND_ZONE_CONFIG_SQL)
        statement = response[0][0]
        if 'num_replicas = 5' in statement:
            logger.info(f"{cluster_name}: The default replication factor is correctly set to 5.")
            check_output = response
            check_result = "pass"
        else:
            logger.info(f"{cluster_name}: The default replication factor is not set to 5.")
            check_output = response
            check_result = "fail"
        connection.close()
    except (psycopg2.DatabaseError, ValueError) as error:
        logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "fail"

    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def backup_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "backup_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    # check that there are two backup schedules running
    get_backup_schedule_count_sql = ("select count(*) from [show schedules] where label = 'backup_schedule' and  "
                                     "schedule_status = 'ACTIVE'")
    try:
        connection = CrdbConnection.get_crdb_connection(cluster_name)
        connection.connect()
        backup_count = connection.execute_sql(get_backup_schedule_count_sql)
        count = backup_count[0][0]
        if count is None:
            logger.info(f"{cluster_name}: Failed to fetch the backup schedule count.")
            check_output = count
            check_result = "fail"
        elif count == 2:
            logger.info(f"{cluster_name}: There are two backup jobs scheduled. All is good!")
            check_output = count
            check_result = "pass"
        else:
            logger.info(f"{cluster_name}: Warning: Expected 2 backup jobs but found {count}.")
            check_output = count
            check_result = "fail"
        connection.close()
    except (psycopg2.DatabaseError, ValueError) as error:
        logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "fail"
    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def run_health_check_single(deployment_env, region, cluster_name, workflow_id=None):
    # List of methods in healthcheck workflow
    hc_methods = [version_mismatch_check, ptr_health_check, etl_health_check, az_health_check, zone_config_health_check,
                  backup_health_check, orphan_health_check, changefeed_health_check, asg_health_check]

    storage_metadata = StorageMetadata()
    aws_account_alias = IamGateway.get_account_alias()

    if workflow_id:
        # If given a workflow_id, get the state from DB
        state = storage_metadata.get_hc_workflow_id_state(workflow_id)
        last_successful_step = hc_methods.index(state.check_type) if state.status == 'Success' else hc_methods.index(
            state.check_type) - 1
    else:
        last_successful_step = -1
        # set workflow_id if not provided
        workflow_id = os.getenv('WORKFLOW-ID')
        storage_metadata.initiate_hc_workflow(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                              aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                              check_type=str(hc_methods[0]), status='InProgress', retry_count=0)

    logger.info(f"{cluster_name}: Running healthchecks on {cluster_name}")
    for idx, method in enumerate(hc_methods):
        if idx > last_successful_step:
            try:
                state = storage_metadata.get_hc_workflow_id_state(workflow_id)
                if state.status == 'Failed':
                    break
                method(deployment_env, region, cluster_name)
                storage_metadata.update_workflow_state_with_retry(cluster_name=cluster_name,
                                                                  deployment_env=deployment_env, region=region,
                                                                  aws_account_name=aws_account_alias,
                                                                  workflow_id=workflow_id,
                                                                  check_type=method.__name__, status='Success',
                                                                  retry_count=0)
            except Exception as e:
                storage_metadata.update_workflow_state_with_retry(cluster_name=cluster_name,
                                                                  deployment_env=deployment_env, region=region,
                                                                  aws_account_name=aws_account_alias,
                                                                  workflow_id=workflow_id,
                                                                  check_type=method.__name__, status='Failure')
                logger.error(e)
                raise

    logger.info(f"{cluster_name}: Healthcheck complete for {cluster_name}")


@app.command()
def run_health_check_all(deployment_env, region):
    # cluster names saved to /tmp/cluster_names.json
    get_cluster_names(deployment_env, region)
    storage_metadata = StorageMetadata()
    workflow_id = os.getenv('WORKFLOW-ID')
    # Open and load the JSON file
    with open('/tmp/cluster_names.json', 'r') as file:
        items = json.load(file)

    logger.info("Healthcheck for all all CRDB clusters started...")
    # run healthcheck on each cluster
    for cluster_name in items:
        run_health_check_single(deployment_env, region, cluster_name)
    logger.info("Healthcheck for all all CRDB clusters complete...")
    # find failed healthchecks and send report to Slack
    failed_checks = storage_metadata.get_hc_results(workflow_id=workflow_id, check_result='fail')
    # NOTE: slack file upload is not ready yet
    base_message = (f"**********************************************************************************************\n"
                    f"HEALTH CHECK REPORT\n"
                    f"workflow_id: {workflow_id} - deployment_env: {deployment_env} - region: {region} \n"
                    f"For full report run - SELECT * FROM cluster_health_check WHERE workflow_id={workflow_id} AND "
                    f"check_result='fail';\n"
                    f"**********************************************************************************************\n")
    message_chunk = ""

    for check in failed_checks:
        if check.cluster_name == 'test_prod' or check.check_output == 'db_connection_error':  # Skip the checks for test cluster
            continue
        new_line = f"cluster_name: {check.cluster_name}, check_type: {check.check_type}, check_result: {check.check_result}\n"
        if len(base_message + message_chunk + new_line) > 3900:  # Keeping some buffer
            send_to_slack("test", base_message + message_chunk)
            message_chunk = ""
        message_chunk += new_line

    if message_chunk:
        send_to_slack("test", base_message + message_chunk)

