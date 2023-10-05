import os
import typer
from storage_workflows.crdb.api_gateway.s3_gateway import S3Gateway
from storage_workflows.metadata_db.storage_metadata.storage_metadata import StorageMetadata
from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.models.users.read_only_user import ReadOnlyUser
from storage_workflows.setup_env import setup_env


app = typer.Typer()
logger = Logger()

@app.command()
def create_users_from_s3_objects(deployment_env, region, bucket_name, aws_account):
    setup_env(deployment_env, region, "")
    objects, next_page_token = S3Gateway.read_objects_with_pagination(bucket_name)
    #Process the objects in the current page
    for obj in objects:
        content = S3Gateway.read_object_contents(bucket_name=bucket_name, key=obj['Key'])
        user_type, cluster_name, user_name = content.split(':')
        logger.info("Read user_type : {}, cluster_name : {}, user_name : {}".format(user_type, cluster_name, user_name))
        os.environ['CLUSTER_NAME'] = cluster_name
        create_user_if_not_exist(cluster_name, deployment_env, region, aws_account, cluster_name, user_name)

    while next_page_token:
        objects, next_page_token = S3Gateway.read_objects_with_pagination(bucket_name, page_token=next_page_token)
        for obj in objects:
            content = S3Gateway.read_object_contents(bucket_name=bucket_name, key=obj['Key'])
            user_type, cluster_name, user_name = content.split(':')
            cluster_name = cluster_name
            logger.info("Read user_type : {0}, cluster_name : {1}, user_name : {2}", user_type, cluster_name, user_name)
            os.environ['CLUSTER_NAME'] = cluster_name
            create_user_if_not_exist(cluster_name, deployment_env, region, aws_account, cluster_name, user_name)


def create_user_if_not_exist(cluster_name, deployment_env, region, aws_account, db_name, role_name):
    storage_metadata = StorageMetadata()
    existing_user = storage_metadata.get_user(cluster_name, region, aws_account, db_name, role_name, deployment_env)
    if existing_user is None:
        read_only_user = ReadOnlyUser(user_name, cluster_name=cluster_name)
        read_only_user.create_user()
        storage_metadata.insert_user(cluster_name=cluster_name, deployment_env=deployment_env, region=region,
                                     aws_account=aws_account, database_name=db_name, role_name=role_name,
                                     certificate_path="cert_path")
        logger.info("Successfully created user_name: {0}", user_name)
    else:
        logger.info("User {0} already exists", user_name)
