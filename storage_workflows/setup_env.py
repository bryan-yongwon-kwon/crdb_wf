import os
from storage_workflows.crdb.aws.sts_role import StsRole

def setup_env(deployment_env, region, cluster_name):

    allowedlist_prod_clusters = ["ao_test"]
    if deployment_env == "prod" and cluster_name not in allowedlist_prod_clusters:
        raise Exception("Access to production clusters is blocked.")

    os.environ['CLUSTER_NAME'] = cluster_name
    os.environ['REGION'] = region
    os.environ['DEPLOYMENT_ENV'] = deployment_env
    role = StsRole.assume_role()
    os.environ['AWS_ACCESS_KEY_ID'] = role.access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = role.secret_access_key
    os.environ['AWS_SESSION_TOKEN'] = role.session_token
