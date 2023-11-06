import typer
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env

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

@app.command()
def reduce_rebalance_rate(deployment_env, region, cluster_name):
    SNAPSHOT_REBALANCE_RATE = 'kv.snapshot_rebalance.max_rate'
    SNAPSHOT_RECOVERY_RATE = 'kv.snapshot_recovery.max_rate'
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    rebalance_rate = cluster.get_cluster_setting(SNAPSHOT_REBALANCE_RATE)
    logger.info("Current rebalance rate: {}".format(rebalance_rate.value))
    recovery_rate = cluster.get_cluster_setting(SNAPSHOT_RECOVERY_RATE)
    logger.info("Current recovery rate: {}".format(recovery_rate.value))
    if rebalance_rate.value != recovery_rate.value:
        logger.error("Rebalance rate and recovery rate are not equal. Please check.")
        logger.info('Skip reducing rebalance rate.')
        return
    new_rate = "{} MiB".format(int(str(rebalance_rate.value).split(' ')[0])//2)
    logger.info("New rate will be: {}".format(new_rate))
    cluster.update_cluster_setting(SNAPSHOT_REBALANCE_RATE, new_rate)
    logger.info("New rebalance rate: {}".format(new_rate))
    cluster.update_cluster_setting(SNAPSHOT_RECOVERY_RATE, new_rate)
    logger.info("New recovery rate: {}".format(new_rate))
    logger.info("Reducing rebalance rate completed.")