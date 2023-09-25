import os
import typer
from storage_workflows.crdb.api_gateway.s3_gateway import S3Gateway
from storage_workflows.metadata_db.storage_metadata.storage_metadata import StorageMetadata
from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.models.users.sql_user import SqlUser
from storage_workflows.setup_env import setup_env


app = typer.Typer()
logger = Logger()

@app.command()
def create_users_from_s3_objects(deployment_env, region, bucket_name, aws_account):
    setup_env(deployment_env, region, "")
    storage_metadata = StorageMetadata()
    #objects, next_page_token = S3Gateway.read_objects_with_pagination(bucket_name)
    #Process the objects in the current page
    #for obj in objects:
    #    content = obj['Key']
    user_type, db_name, user_name = ["sql", "crdb_benchmark", "meha.kaushik"]#content.split(':')
    logger.info("Read user_type : {}, db_name : {}, user_name : {}".format(user_type, db_name, user_name))
    os.environ['CLUSTER_NAME'] = db_name
    # check if the user already exists, if yes, then do not do this.
    storage_metadata.insert_user(cluster_name=db_name, deployment_env=deployment_env, region=region,
                                 aws_account=aws_account, database_name=db_name, role_name=user_name,
                                 certificate_path="cert_path")
    sql_user = SqlUser(user_name, cluster_name=db_name, db_name=db_name)
    sql_user.create_user()
    logger.info("Successfully created user_name: {0}".format(user_name))

    # while next_page_token:
    #     objects, next_page_token = S3Gateway.read_objects_with_pagination(bucket_name, page_token=next_page_token)
    #     for obj in objects:
    #         content = obj['Key']
    #         user_type, db_name, user_name = content.split(':')
    #         logger.info("Read user_type : {0}, db_name : {1}, user_name : {2}", user_type, db_name, user_name)
    #         os.environ['CLUSTER_NAME'] = db_name
    #         # store in metadata db
    #         storage_metadata.insert_user(cluster_name=db_name, deployment_env=deployment_env, region=region,
    #                                         aws_account=aws_account, database_name=db_name, role_name=user_name,
    #                                         certificate_path="cert_path")
    #         sql_user = SqlUser(user_name, cluster_name=db_name)
    #         sql_user.create_user()
    #         logger.info("Successfully created user_name: {0}", user_name)
