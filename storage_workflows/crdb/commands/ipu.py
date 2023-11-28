import datetime
import json
import math
import os
import statistics
import sys
import time
import typer
from storage_workflows.metadata_db.crdb_workflows.crdb_dbops_workflows import CrdbDbOpsWorkflows
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
def run_debug_doctor(deployment_env, region, cluster_name):
    """
    Checks for table descriptor corruption in preparation for major version upgrade
    """
    setup_env(deployment_env, region, cluster_name)
    nodes = Node.get_nodes()
    if nodes:
        nodes[0].check_table_descriptor_corruption()
    else:
        logger.error("No nodes found in the cluster.")


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

@app.command()
def start_ipu(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    # handle unusual cluster names with dashes: e.g. url-shortener
    cluster_name = os.environ['CLUSTER_NAME']
    workflow_id = os.environ['WORKFLOW_ID']
    operator_name = os.environ['OPERATOR_NAME']
    db_ops = CrdbDbOpsWorkflows()
    db_ops.start_workflow(cluster_name, region, deployment_env, "ipu", operator_name)

