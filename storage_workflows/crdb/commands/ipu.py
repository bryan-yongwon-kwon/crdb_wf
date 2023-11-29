import typer
import os
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_global_env import setup_global_env
from storage_workflows.setup_env import setup_env
from apscheduler.schedulers.blocking import BlockingScheduler

app = typer.Typer()
logger = Logger()

deployment_env = os.getenv('DEPLOYMENT_ENV')
region = os.getenv('REGION')
cluster_name = os.getenv('CLUSTER_NAME')


@app.command()
def update_and_drain_nodes():
    setup_env(deployment_env, region, cluster_name)
    logger.info(f"Starting update and drain process for {cluster_name} cluster.")

    # Get the list of nodes from the cluster
    nodes = Node.get_nodes()

    # Get the name of the auto-scaling group associated with the cluster
    asg_name = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).name

    for node in nodes:
        try:
            # Connect to the node and run the download_and_setup_cockroachdb method
            node.ssh_client.connect_to_node()
            node.ssh_client.download_and_setup_crdb()
            node.ssh_client.close_connection()

            # Detach the node from its auto-scaling group
            AutoScalingGroupGateway.detach_instance_from_autoscaling_group([node.ip_address], asg_name)

            # Drain the node
            node.drain()

            logger.info(f"Successfully updated and drained node with IP {node.ip_address}.")

        except Exception as e:
            logger.error(f"Failed to update and drain node with IP {node.ip_address}: {str(e)}")

    logger.info("Update and drain process completed for all nodes in the cluster.")


@app.command()
def run_ipu_tasks():
    scheduler = BlockingScheduler()
    # using 'date' without 'run_date' will trigger immediate scheduler execution
    scheduler.add_job(lambda: update_and_drain_nodes(), 'date')
    # Start the scheduler and run the job(s) immediately
    scheduler.start()


if __name__ == "__main__":
    try:
        setup_global_env()
        run_ipu_tasks()
    except (KeyboardInterrupt, SystemExit):
        pass
