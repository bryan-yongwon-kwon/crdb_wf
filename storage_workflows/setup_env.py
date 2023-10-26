import os
from storage_workflows.crdb.aws.sts_role import StsRole

def setup_env(deployment_env, region, cluster_name=""):
    # handle cluster_name with hyphens instead of underscores
    cluster_names_to_modify = ['url_shortener', 'revenue_platform']

    if cluster_name in cluster_names_to_modify or cluster_name.startswith('revenue_workflow_'):
        cluster_name = cluster_name.strip().replace("_", "-")
    else:
        cluster_name = cluster_name
    os.environ['CLUSTER_NAME'] = cluster_name
    os.environ['REGION'] = region
    os.environ['DEPLOYMENT_ENV'] = deployment_env
    role = StsRole.assume_role()
    os.environ['AWS_ACCESS_KEY_ID'] = role.access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = role.secret_access_key
    os.environ['AWS_SESSION_TOKEN'] = role.session_token
