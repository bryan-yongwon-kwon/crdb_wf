import json
import os
import typer
import logging
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
#from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.slack.content_templates import ContentTemplate
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification
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
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    unhealthy_asg_instances = list(filter(lambda instance: not instance.in_service(), asg.instances))
    if unhealthy_asg_instances:
        unhealthy_asg_instance_ids = list(map(lambda instance: instance.instance_id, unhealthy_asg_instances))
        logger.warning("Displaying all unhealthy instances for the $cluster_name cluster:")
        logger.warning(unhealthy_asg_instance_ids)
        logger.warning("Auto Scaling Group name: {}".format(asg.name))
    # TODO: Write result into metadata DB


@app.command()
def changefeed_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    unhealthy_changefeed_jobs = list(
        filter(lambda job: job.status != "running" and job.status != "canceled", cluster.changefeed_jobs))
    if unhealthy_changefeed_jobs:
        logger.warning("Changefeeds Not Running:")
        for job in unhealthy_changefeed_jobs:
            logger.warning("Job id is {}. Status is {}.".format(job.id, job.status))
    # TODO: Write result into metadata DB 


@app.command()
def orphan_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    # Get the count of AWS instances
    instances_with_cluster_tag = Ec2Instance.find_ec2_instances_by_cluster_tag(cluster_name)
    aws_cluster_instances = list(
        filter(lambda instance: instance.state != "terminated" and instance.state != "shutting-down",
               instances_with_cluster_tag))
    aws_cluster_instance_count = len(aws_cluster_instances)
    # Get the IP count of CRDB nodes
    crdb_node_ips = list(map(lambda node: node.ip_address, cluster.nodes))
    crdb_cluster_instance_count = len(crdb_node_ips)
    # Compare the IP count of AWS instances and CRDB nodes
    if aws_cluster_instance_count != crdb_cluster_instance_count:
        orphan_instances = list(
            map(lambda instance: {"InstanceId": instance.instance_id, "PrivateIpAddress": instance.private_ip_address},
                filter(lambda instance: instance.private_ip_address not in crdb_node_ips, aws_cluster_instances)))
        logger.warning("Orphan instances found.")
        logger.warning("AWS instance count is {} and CRDB instance count is {}.".format(aws_cluster_instance_count,
                                                                                        crdb_cluster_instance_count))
        logger.warning("Orphan instances are: {}".format(orphan_instances))
    else:
        logger.info("No orphan instances found.")
    # TODO: Write result into metadata DB 


@app.command()
def ptr_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "ptr_health_check"
    logger.info("Running protected timestamp record check...")
    FIND_PTR_SQL = ("select (ts/1000000000)::int::timestamp as \"pts timestamp\", now()-(("
                    "ts/1000000000)::int::timestamp) as \"pts age\", *,crdb_internal.cluster_name() from "
                    "system.protected_ts_records where ((ts/1000000000)::int::timestamp) < now() - interval '2d';")
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    response = connection.execute_sql(FIND_PTR_SQL)
    connection.close()
    contains_ptr = any(response)
    if contains_ptr:
        logger.warning("Protected timestamp records found on {} cluster: " + response).format(cluster_name)
        check_output = response
        check_result = "ptr_health_check_failed"
    else:
        logger.info("Protected timestamp record not found")
        check_output = "{}"
        check_result = "ptr_health_check_passed"

    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                       aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                       check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info("PTR Health Check Complete")

@app.command()
def send_slack_notification(deployment_env):
    # TODO: Read healthy check result from metadata DB
    results = ["text1", "text2"]  # this is a place holder
    webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB') if deployment_env == 'prod' else os.getenv(
        'SLACK_WEBHOOK_STORAGE_ALERTS_CRDB_STAGING')
    notification = SlackNotification(webhook_url)
    notification.send_notification(ContentTemplate.get_health_check_template(results))


@app.command()
def etl_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    logger.info("account_alias: %s", aws_account_alias)
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "etl_health_check"
    check_output = "{}"
    if deployment_env == 'staging':
        logger.info("Staging clusters doesn't have ETL load balancers.")
        return
    load_balancer = ElasticLoadBalancer.find_elastic_load_balancer_by_cluster_name(cluster_name)
    old_lb_instances = load_balancer.instances
    old_instance_id_set = set(map(lambda old_instance: old_instance['InstanceId'], old_lb_instances))
    logger.info("Old instances: {}".format(old_instance_id_set))
    new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id},
                             filter(lambda instance: instance.instance_id not in old_instance_id_set,
                                    AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances)))
    logger.info("New instances: {}".format(new_instances))
    if not new_instances:
        logger.warning("No new instances, no need to refresh. Step complete.")
        check_result = "etl_health_check_no_action_needed"
        # write results to storage_metadata
        storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                           aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                           check_type=check_type, check_result=check_result, check_output=check_output)
        return
    load_balancer.register_instances(new_instances)
    if old_lb_instances:
        load_balancer.deregister_instances(old_lb_instances)
    new_instance_list = list(map(lambda instance: instance['InstanceId'], new_instances))
    lb_instance_list = list(map(lambda instance: instance['InstanceId'], load_balancer.instances))
    if set(new_instance_list) == set(lb_instance_list):
        logger.info("ETL load balancer refresh completed!")
        check_result = "pass"
    else:
        check_result = "fail"
        raise Exception("Instances don't match. ETL load balancer refresh failed!")

    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                       aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                       check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info("ETL Health Check Complete")

@app.command()
def version_mismatch_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    # aws_account_alias = IamGateway.get_account_alias()
    # workflow_id = os.getenv('WORKFLOW-ID')
    # check_type = "version_mismatch_check"
    cluster = Cluster()
    logger.info("Running version mismatch check...")
    crdb_sql_version = ("show cluster setting version;")
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    response = connection.execute_sql(crdb_sql_version)
    connection.close()
    crdb_node_version_major = list(map(lambda node: node.minor_version, cluster.nodes))
    crdb_node_version_minor = list(map(lambda node: node.minor_version, cluster.nodes))
    if all(version == crdb_node_version_major[0] for version in crdb_node_version_major):
        logger.info("All nodes are running the same version on {} cluster" + response).format(cluster_name)
        # check_output = response
        # check_result = "version_mismatch_check_passed"
        if crdb_sql_version == crdb_node_version_minor:
            logger.info("The SQL version matches the Node version on {} cluster" + response).format(cluster_name)
            # check_output = response
            # check_result = "version_mismatch_check_passed"
        else:
            logger.warning("The SQL version does not match the Node version on {} cluster" + response).format(cluster_name)
            # check_output = response
            # check_result = "version_mismatch_check_failed"
    else:
        logger.warning("Nodes are running different versions on {} cluster: " + response).format(cluster_name)
        # check_output = response
        # check_result = "version_mismatch_check_failed"

    # write results to storage_metadata
    # storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
    #                                    aws_account_name=aws_account_alias, workflow_id=workflow_id,
    #                                    check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info("Version Mismatch Check Complete")
def az_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    logger.info("account_alias: %s", aws_account_alias)
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "az_health_check"
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
    logger.info("Node counts in each availability zone:")
    for az, count in counts.items():
        logger.info("{}: {} nodes".format(az, count))
    unique_counts = set(counts.values())
    if len(unique_counts) == 1:
        logger.info("All availability zones have the same number of nodes!")
        check_result = "pass"
    else:
        logger.info("Mismatch in node counts across availability zones!")
        check_result = "fail"
    check_output = "{}"
    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                       aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                       check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info("AZ Health Check Complete")

@app.command()
def zone_config_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "zone_config_health_check"
    logger.info("Running replication factor check...")
    # check that default replication factor is five
    FIND_ZONE_CONFIG_SQL = "select raw_config_sql from [show zone configuration from range default]"
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    response = connection.execute_sql(FIND_ZONE_CONFIG_SQL)
    connection.close()
    logger.info(response)
    statement = response[0][0]
    if 'num_replicas = 5' in statement:
        logger.info("The default replication factor is correctly set to 5.")
        check_output = response
        check_result = "zone_config_health_check_passed"
    else:
        logger.info("The default replication factor is not set to 5.")
        check_output = response
        check_result = "zone_config_health_check_failed"

    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                       aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                       check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info("Zone Config Health Check Complete")

@app.command()
def backup_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    storage_metadata = StorageMetadata()
    # Usually an AWS account has one alias, but the response is a list.
    # Thus, this will return the first alias, or None if there are no aliases.
    aws_account_alias = IamGateway.get_account_alias()
    workflow_id = os.getenv('WORKFLOW-ID')
    check_type = "backup_health_check"
    logger.info("Running backup schedule check...")
    # check that there are two backup schedules running
    get_backup_schedule_count_sql = ("select count(*) from [show schedules] where label = 'backup_schedule' and  "
                                     "schedule_status = 'ACTIVE'")
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    backup_count = connection.execute_sql(get_backup_schedule_count_sql)
    connection.close()
    count = backup_count[0][0]
    if count is None:
        logger.info("Failed to fetch the backup schedule count.")
        check_output = count
        check_result = "backup_health_check_failed"
    elif count == 2:
        logger.info("There are two backup jobs scheduled. All is good!")
        check_output = count
        check_result = "backup_health_check_passed"
    else:
        logger.info(f"Warning: Expected 2 backup jobs but found {count}.")
        check_output = count
        check_result = "backup_health_check_failed"
    # write results to storage_metadata
    storage_metadata.insert_health_check(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                       aws_account_name=aws_account_alias, workflow_id=workflow_id,
                                       check_type=check_type, check_result=check_result, check_output=check_output)

    logger.info("Backup Health Check Complete")

@app.command()
def run_all_health_checks(deployment_env, region, cluster_name):
    orphan_health_check(deployment_env, region, cluster_name)
    ptr_health_check(deployment_env, region, cluster_name)
    etl_health_check(deployment_env, region, cluster_name)
    version_mismatch_check(deployment_env, region, cluster_name)
    az_health_check(deployment_env, region, cluster_name)
    zone_config_health_check(deployment_env, region, cluster_name)
    backup_health_check(deployment_env, region, cluster_name)
