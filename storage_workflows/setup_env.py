import os
from storage_workflows.crdb.aws.sts_role import StsRole

def setup_env(deployment_env, region):
    os.environ['REGION'] = region
    os.environ['DEPLOYMENT_ENV'] = deployment_env
    role = StsRole.assume_role()
    print("AWS_ACCESS_KEY_ID:")
    print(role.access_key_id)
    print("AWS_SECRET_ACCESS_KEY")
    print(role.secret_access_key)
    print("AWS_SESSION_TOKEN")
    print(role.session_token)
    os.environ['AWS_ACCESS_KEY_ID'] = role.access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = role.secret_access_key
    os.environ['AWS_SESSION_TOKEN'] = role.session_token
    print("after set env")
    print(os.getenv('AWS_ACCESS_KEY_ID'))
    print(os.getenv('AWS_SECRET_ACCESS_KEY'))
    print(os.getenv('AWS_SESSION_TOKEN'))
