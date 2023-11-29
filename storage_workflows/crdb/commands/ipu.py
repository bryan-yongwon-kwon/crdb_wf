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
def update_and_drain_nodes(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    logger.info(f"Starting update and drain process for {cluster_name} cluster.")

    # Get the list of nodes from the cluster
    nodes = Node.get_nodes()

    # Get the name of the auto-scaling group associated with the cluster
    asg_name = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).name

    for node in nodes:
        try:
            # Connect to the node and run the download_and_setup_cockroachdb method
            node.ssh_client.connect_to_node()
            node.ssh_client.download_and_setup_cockroachdb()
            node.ssh_client.close_connection()

            # Detach the node from its auto-scaling group
            AutoScalingGroupGateway.detach_instance_from_autoscaling_group([node.ip_address], asg_name)

            # Drain the node
            node.drain()

            logger.info(f"Successfully updated and drained node with IP {node.ip_address}.")

        except Exception as e:
            logger.error(f"Failed to update and drain node with IP {node.ip_address}: {str(e)}")

    logger.info("Update and drain process completed for all nodes in the cluster.")



