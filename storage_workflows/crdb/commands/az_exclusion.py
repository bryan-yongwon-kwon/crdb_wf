import typer
from storage_workflows.crdb.commands.health_check import get_cluster_names as gen_names
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env

app = typer.Typer()
logger = Logger()

@app.command()
def get_cluster_names(deployment_env, region):
    setup_env(deployment_env, region)
    gen_names(deployment_env, region)

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


@app.command()
def update_schema_single_cluster(deployment_env, region, cluster_name):
    SQL_TRUNCATE_CONFIG_TABLE = "TRUNCATE auto_discovery.nodes_exclusion_config;"
    SQL_ADD_COLUMN = "ALTER TABLE auto_discovery.nodes_exclusion_config ADD COLUMN IF NOT EXISTS is_valid BOOL NOT NULL;"
    SQL_UPDATE_PRIMARY_KEY = "ALTER TABLE auto_discovery.nodes_exclusion_config ALTER PRIMARY KEY USING COLUMNS (is_valid, rowid);"
    SQL_INSERT_NODES_EXCLUSION_CONFIG = "INSERT INTO auto_discovery.nodes_exclusion_config (azs, nodes, is_valid) VALUES (Array[], Array[], true);"
    SQL_UPDATE_AZS_EXCLUSION_VIEW = "CREATE OR REPLACE VIEW auto_discovery.azs_to_exclude AS SELECT unnest(azs) AS az FROM auto_discovery.nodes_exclusion_config WHERE is_valid is true;"
    SQL_UPDATE_NODES_EXCLUSION_VIEW = "CREATE OR REPLACE VIEW auto_discovery.nodes_to_exclude AS SELECT unnest(nodes) AS node_id FROM auto_discovery.nodes_exclusion_config WHERE is_valid is true;"
    setup_env(deployment_env, region, cluster_name)
    logger.info("Starting schema update for cluster {}.".format(cluster_name))
    connection = CrdbConnection.get_crdb_connection(cluster_name)
    connection.connect()
    connection.execute_sql(SQL_TRUNCATE_CONFIG_TABLE, auto_commit=True)
    logger.info("Table auto_discovery.nodes_exclusion_config truncated.")
    connection.execute_sql(SQL_ADD_COLUMN, auto_commit=True)
    logger.info("Column is_valid added to table auto_discovery.nodes_exclusion_config.")
    connection.execute_sql(SQL_UPDATE_PRIMARY_KEY, auto_commit=True)
    logger.info("Primary key updated for table auto_discovery.nodes_exclusion_config.")
    connection.execute_sql(SQL_INSERT_NODES_EXCLUSION_CONFIG, auto_commit=True)
    logger.info("Default row inserted into table auto_discovery.nodes_exclusion_config.")
    connection.execute_sql(SQL_UPDATE_AZS_EXCLUSION_VIEW, auto_commit=True)
    logger.info("View auto_discovery.azs_to_exclude updated.")
    connection.execute_sql(SQL_UPDATE_NODES_EXCLUSION_VIEW, auto_commit=True)
    logger.info("View auto_discovery.nodes_to_exclude updated.")
    connection.close()
    logger.info("Schema update done for cluster {}.".format(cluster_name))
