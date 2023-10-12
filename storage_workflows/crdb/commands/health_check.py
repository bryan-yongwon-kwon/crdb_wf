import json
import os
import typer
from collections import defaultdict
import psycopg2
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.slack.content_templates import ContentTemplate
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification, send_to_slack, generate_csv_file
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.metadata_db.storage_metadata.storage_metadata import StorageMetadata
from storage_workflows.crdb.api_gateway.iam_gateway import IamGateway
from storage_workflows.crdb.api_gateway.ebs_gateway import EBSGateway
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway

app = typer.Typer()
logger = Logger()


@app.command()
def get_cluster_names(deployment_env, region):
    setup_env(deployment_env, region)
    asgs = AutoScalingGroup.find_all_auto_scaling_groups([AutoScalingGroup.build_filter_by_crdb_tag()])
    names = list(map(lambda asg: asg.name.split("_{}-".format(deployment_env))[0], asgs))
    names="parcel_service"
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
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "asg_health_check"
    logger.info(f"{cluster_name}: starting {check_type}")
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)

    # Debugging info
    instance_info = [(instance.instance_id, instance.health_status) for instance in asg.instances]
    info_str = ', '.join([f"ID: {id}, Health: {status}" for id, status in instance_info])
    logger.info(f"{cluster_name}: Instances: {info_str}")

    # Separate lists for unhealthy and standby instances
    unhealthy_asg_instances = [instance for instance in asg.instances if instance.health_status == 'Unhealthy']
    standby_asg_instances = [instance for instance in asg.instances if instance.health_status == 'Standby']

    # Extracting instance_ids from the filtered instances
    unhealthy_asg_instance_ids = [instance.instance_id for instance in unhealthy_asg_instances]
    standby_asg_instance_ids = [instance.instance_id for instance in standby_asg_instances]

    if unhealthy_asg_instances or standby_asg_instances:
        logger.warning(f"{cluster_name}: Displaying all unhealthy instances for the {cluster_name} cluster: "
                       f"{unhealthy_asg_instance_ids}")
        if standby_asg_instances:
            logger.warning(f"{cluster_name}: Displaying all standby instances for the {cluster_name} cluster: "
                           f"{standby_asg_instance_ids}")
        logger.warning(f"{cluster_name}: Auto Scaling Group name: {asg.name}")
        check_output = f"unhealthy_asg_instance_ids: {unhealthy_asg_instance_ids}, standby_asg_instance_ids: {standby_asg_instance_ids}"
        check_result = "fail"
    else:
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
def volume_mismatch_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)  # Assuming setup_env is defined somewhere else
    storage_metadata = StorageMetadata()  # Assuming StorageMetadata is defined somewhere else
    aws_account_alias = IamGateway.get_account_alias()  # Assuming IamGateway is defined somewhere else
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "volume_mismatch_health_check"

    logger.info(f"{cluster_name}: starting {check_type}")

    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)

    all_volumes = []
    for instance in asg.instances:
        all_volumes.extend((instance.instance_id, vol) for vol in EBSGateway.get_ebs_volumes_for_instance(instance.instance_id))

    reference_instance_id, reference_volume = all_volumes[0]
    consistent = True
    check_output = []  # List to store mismatch data

    for instance_id, volume in all_volumes[1:]:
        if volume != reference_volume:
            consistent = False
            mismatch_data = {
                "instance_id": instance_id,
                "ebs_volume_size": volume.size,
                "ebs_volume_iops": volume.iops,
                "ebs_volume_throughput": volume.throughput
            }
            check_output.append(mismatch_data)

    if consistent:
        check_output = "ebs_volumes_are_consistent"
        check_result = "pass"
        logger.info(f"{cluster_name}: All EBS volumes have the same size, IOPS, and throughput!")
    else:
        check_output = str(check_output)
        check_result = "fail"
        logger.warning(f"{cluster_name}: Found inconsistencies in EBS volumes.")

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
    check_output=[]
    check_result="pass"
    cluster = Cluster()
    try:
        list_of_changefeed_jobs = list(cluster.changefeed_jobs)
        if len(list_of_changefeed_jobs) == 0:
            logger.info(f"{cluster_name}: No changefeed jobs found.")
            return
        else:
            logger.info(f"{cluster_name}: Found {len(list_of_changefeed_jobs)} changefeed jobs.")
        for job in list_of_changefeed_jobs:
            changefeed_job_id = job.id
            changefeed_status = job.status
            if changefeed_status == "succeeded":
                logger.info(f"PASS: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status}")
                continue

            changefeed_metadata = job.changefeed_metadata
            latency = changefeed_metadata.latency
            running_status = changefeed_metadata.running_status
            error = changefeed_metadata.error
            is_initial_scan_only = changefeed_metadata.is_initial_scan_only
            finished_ago_seconds = changefeed_metadata.finished_ago_seconds
            if changefeed_status == "running" and latency > -1800:
                logger.info(f"PASS: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status} with latency {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}.")
                continue
            elif changefeed_status == "running" and latency <= -1800 and is_initial_scan_only is True:
                logger.info(f"WARN: {cluster_name}: INITIAL_SCAN_ONLY job_id {changefeed_job_id} is {changefeed_status} with latency {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}.")
                check_output.append(f"WARN: INITIAL_SCAN_ONLY {changefeed_job_id}: {changefeed_status} latency: {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}. RUNNING_STATUS: {running_status}. ERROR: {error}")
                continue
            elif changefeed_status == "running" and latency <= -1800 and is_initial_scan_only is False:
                logger.info(f"FAIL: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status} with latency {latency}.")
                check_output.append(f"FAIL: {changefeed_job_id}: {changefeed_status} latency: {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}. RUNNING_STATUS: {running_status}. ERROR: {error}")
                check_result = "fail"
            elif changefeed_status == "running" and (latency == "NULL" or latency is None) and is_initial_scan_only is False:
                logger.info(f"FAIL: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status} with latency {latency}.")
                check_output.append(f"FAIL: {changefeed_job_id}: {changefeed_status} latency: {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}. RUNNING_STATUS: {running_status}. ERROR: {error}")
                check_result = "fail"
            elif changefeed_status == "paused" and latency < -300:
                logger.info(f"FAIL: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status} with latency {latency}.")
                check_output.append(f"FAIL: {changefeed_job_id}: {changefeed_status} latency: {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}. RUNNING_STATUS: {running_status}. ERROR: {error}")
                check_result = "fail"
            elif changefeed_status == "failed" and finished_ago_seconds > (86400*3):
                logger.info(f"FAIL: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status} with latency {latency}.")
                check_output.append(f"FAIL: {changefeed_job_id}: {changefeed_status} latency: {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}. RUNNING_STATUS: {running_status}. ERROR: {error}. FINISHED_AGO_SECONDS: {finished_ago_seconds}")
                check_result = "fail"
            else:
                logger.info(f"ELSE: {cluster_name}: job_id {changefeed_job_id} is {changefeed_status} with latency {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}.")
                check_output.append(f"ELSE: {changefeed_job_id}: {changefeed_status} latency: {latency}. INITIAL_SCAN_ONLY: {is_initial_scan_only}. RUNNING_STATUS: {running_status}. ERROR: {error}. FINISHED_AGO_SECONDS: {finished_ago_seconds}")
                pass
    except (psycopg2.DatabaseError, ValueError) as error:
        # logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "error"
    # save results to metadatadb
    if not check_output:
        check_output = "changefeed_health_check_passed"
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
                map(lambda instance: {"InstanceId": instance.instance_id,
                                      "PrivateIpAddress": instance.private_ip_address},
                    filter(lambda instance: instance.private_ip_address not in crdb_node_ips, aws_cluster_instances)))
            logger.warning(f"{cluster_name}: Orphan instances found")
            logger.warning(
                f"{cluster_name}: AWS instance count is {aws_cluster_instance_count} and CRDB instance count is {crdb_cluster_instance_count}.")
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
        # logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "error"
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
                    "system.protected_ts_records where ((ts/1000000000)::int::timestamp) < now() - interval '25h';")
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
        # logger.error(f"{cluster_name}: encountered error - {error}")
        check_output = "db_connection_error"
        check_result = "error"

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
    aws_account_alias = IamGateway.get_account_alias()  # Assuming this method is defined somewhere in your codebase
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "etl_health_check"
    check_output = "{}"

    logger.info(f"{cluster_name}: starting {check_type}")
    if deployment_env == 'staging':
        logger.info(f"{cluster_name}: Staging clusters doesn't have ETL load balancers.")
        return

    elb_load_balancer = ElasticLoadBalancer.find_elastic_load_balancer_by_cluster_name(cluster_name)

    if elb_load_balancer is not None:
        elb_instances = elb_load_balancer.instances
        old_lb_instances = [instance for instance in elb_instances if
                                    instance.get('InstanceState', {}).get('State') == 'InService']
        old_instance_id_set = set(map(lambda old_instance: old_instance['InstanceId'], old_lb_instances))
        logger.info(f"{cluster_name}: Old instances: {old_instance_id_set}")

        new_instances = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances
        filtered_instances = filter(lambda instance: instance.is_healthy, new_instances)
        new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id}, filtered_instances))
        logger.info(f"{cluster_name}: New instances: {new_instances}")

        if not new_instances:
            logger.warning(f"{cluster_name}: No new instances, no need to refresh. Step complete.")
            check_output = "no_action_needed"
            check_result = "pass"
        else:
            new_instance_list = list(map(lambda instance: instance['InstanceId'], new_instances))
            lb_instance_list = list(map(lambda instance: instance['InstanceId'], old_lb_instances))

            if new_instance_list:
                if old_lb_instances:
                    elb_load_balancer.deregister_instances(old_lb_instances)
                elb_load_balancer.register_instances(new_instances)
                elb_load_balancer_name = elb_load_balancer.load_balancer_name
                unhealthy_instances = ElasticLoadBalancerGateway.get_out_of_service_instances(elb_load_balancer_name)
                if not unhealthy_instances:
                    logger.info(f"{cluster_name}: ETL load balancer refresh completed!")
                    check_result = "pass"
                    check_output = "etl_loadbalancer_refreshed"
                else:
                    logger.info(f"{cluster_name}: ETL load balancer refresh failed!")
                    logger.info(f"{cluster_name}: unhealthy_instances are: {unhealthy_instances}")
                    logger.info(f"{cluster_name}: new_instance_list are: {new_instance_list}")
                    logger.info(f"{cluster_name}: lb_instance_list are: {lb_instance_list}")
                    check_result = "fail"
                    check_output = f"etl_loadbalancer_refresh_failed, OutOfService instances: {unhealthy_instances}"

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
            logger.warning(
                f"{cluster_name}: server version mismatch detected. cluster version is {cluster_ver_response}"
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
        # logger.error(f"{cluster_name}: encountered error - {error}")
        check_result = "error"
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
    # logger.info("account_alias: %s", aws_account_alias)
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
        check_result = "error"

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
        check_result = "error"
    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                         aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                         check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info(f"{cluster_name}: {check_type} complete")


@app.command()
def run_health_check_single(deployment_env, region, cluster_name, workflow_id=None):
    # List of methods in healthcheck workflow
    hc_methods = [version_mismatch_check, ptr_health_check, etl_health_check,
                  az_health_check, zone_config_health_check, backup_health_check, orphan_health_check,
                  changefeed_health_check, asg_health_check]

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
    # find failed healthchecks
    failed_checks = storage_metadata.get_hc_results(workflow_id=workflow_id, check_result='fail')

    # TODO: uncomment to use send msg with attachment
    # header = ["cluster_name", "check_type", "check_result", "check_output"]
    # Generate CSV file with the failed checks
    # csv_file_path = generate_csv_file(failed_checks, header)

    base_message = (f"**********************************************************************************************\n"
                    f"HEALTH CHECK REPORT\n"
                    f"workflow_id: {workflow_id} - deployment_env: {deployment_env} - region: {region} \n"
                    f"For full report run - SELECT * FROM cluster_health_check WHERE workflow_id={workflow_id} AND "
                    f"check_result='fail';\n"
                    f"**********************************************************************************************\n")
    message_chunk = ""

    for check in failed_checks:
        if check.cluster_name == 'test_prod':  # Skip the checks for test cluster
            continue
        new_line = f"cluster_name: {check.cluster_name}, check_type: {check.check_type}, check_result: {check.check_result}\n"
        if len(base_message + message_chunk + new_line) > 3900:  # Keeping some buffer
            send_to_slack(deployment_env, base_message + message_chunk, msg_type='hc')
            message_chunk = ""
        message_chunk += new_line

    if message_chunk:
        send_to_slack(deployment_env, base_message + message_chunk, msg_type='hc')

    # TODO: troubleshoot sending slack msg with attachments. we get 200 response from slack, but no msg is
    #   sent to the channel
    # bot_user_oauth_token=os.getenv('BOT-USER-OAUTH-TOKEN')
    # slack_notification = SlackNotification(webhook_url=os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB'),
    #                                       bearer_token=bot_user_oauth_token)


    # Send the CSV file as attachment to the Slack channel
    # response = slack_notification.send_to_slack_with_attachment(csv_file_path, "CRDB HEALTH REPORT", "storage-alerts-crdb")
    # logger.info(f"response from slack: {response}")
