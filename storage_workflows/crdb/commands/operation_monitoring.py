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