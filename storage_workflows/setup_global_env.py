import os
import sys
from storage_workflows.crdb.aws.sts_role import StsRole
from storage_workflows.logging.logger import Logger

logger = Logger()
deployment_env = None
region = None
cluster_name = None


def setup_global_env():
    global deployment_env, region, cluster_name
    deployment_env = os.getenv('DEPLOYMENT_ENV')
    region = os.getenv('REGION')
    cluster_name = os.getenv('CLUSTER_NAME')

    logger.info(f"deployment_env from setup_env: {deployment_env}")
    logger.info(f"region from setup_env: {region}")
    logger.info(f"cluster_name from setup_env: {cluster_name}")
