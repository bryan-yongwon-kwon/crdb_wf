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
def check_avg_cpu(deployment_env, region, cluster_name):
    AVG_CPU_THRESHOLD = 0.5 # 50% CPU usage
    OFFSET_MINS = 5 # > threshold for OFFSET_MINS minutes to trigger action
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    if cluster.is_avg_cpu_exceed_threshold(AVG_CPU_THRESHOLD, OFFSET_MINS):
        logger.info("Average CPU usage is above threshold. Start reducing balancing rate.")
    else:
        logger.info("Average CPU usage is below threshold. No action needed.")