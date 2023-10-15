import typer
from storage_workflows.crdb.commands.health_check import get_cluster_names
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env

app = typer.Typer()
logger = Logger()

@app.command()
def get_cluster_names(deployment_env, region):
    setup_env(deployment_env, region)
    get_cluster_names(deployment_env, region)

@app.command()
def setup_schema_single_cluster(deployment_env, region, cluster_name):
    SQL_CREATE_DR_SCHEMA = "CREATE SCHEMA IF NOT EXISTS auto_discovery AUTHORIZATION root;"
    SQL_CREATE_NODES_EXCLUSTION_CONFIG_TABLE = "CREATE TABLE IF NOT EXISTS auto_discovery.nodes_exclusion_config (azs STRING[], nodes INT[]);"
    SQL_CREATE_NODES_TO_EXCLUDE_VIEW = '''CREATE VIEW IF NOT EXISTS auto_discovery.nodes_to_exclude 
                                          AS SELECT unnest(nodes) AS node_id FROM auto_discovery.nodes_exclusion_config;'''
    SQL_CREATE_AZS_TO_EXCLUDE_VIEW = '''CREATE VIEW IF NOT EXISTS auto_discovery.azs_to_exclude
                                        AS SELECT unnest(azs) AS az FROM auto_discovery.nodes_exclusion_config;'''
    SQL_CREATE_NODES_VIEW = '''
    CREATE VIEW IF NOT EXISTS auto_discovery.nodes AS (
        WITH nodes AS (
            SELECT 
                node_id, 
                GREATEST(0, (EXTRACT(epoch FROM now() - (updated_at + '3 sec'::interval))/10)::int) AS age,
                string_to_array(address, ':')[1] as ip_address,
                string_to_array(address, ':')[2] as port,
                SPLIT_PART(SPLIT_PART(locality, 'az=', 2), ',', 1) AS availability_zone
            FROM crdb_internal.kv_node_status 
            ORDER BY updated_at DESC
        )
        SELECT node_id, age, ip_address, port, availability_zone,
            (node_id IN (SELECT node_id FROM auto_discovery.nodes_to_exclude)
                OR availability_zone IN (SELECT az FROM auto_discovery.azs_to_exclude)) AS excluded
        FROM nodes 
        WHERE age = (SELECT MIN(age) FROM nodes)
    );
    '''
    setup_env(deployment_env, region, cluster_name)
    logger.info("Starting schema setup for cluster {}.".format(cluster_name))
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    connection.execute_sql(SQL_CREATE_DR_SCHEMA, auto_commit=True)
    logger.info("Schema auto_discovery created.")
    connection.execute_sql(SQL_CREATE_NODES_EXCLUSTION_CONFIG_TABLE, auto_commit=True)
    logger.info("Table auto_discovery.nodes_exclusion_config created.")
    connection.execute_sql(SQL_CREATE_NODES_TO_EXCLUDE_VIEW, auto_commit=True)
    logger.info("View auto_discovery.nodes_to_exclude created.")
    connection.execute_sql(SQL_CREATE_AZS_TO_EXCLUDE_VIEW, auto_commit=True)
    logger.info("View auto_discovery.azs_to_exclude created.")
    connection.execute_sql(SQL_CREATE_NODES_VIEW, auto_commit=True)
    logger.info("View auto_discovery.nodes created.")
    connection.close()
    logger.info("Schema setup done for cluster {}.".format(cluster_name))

    