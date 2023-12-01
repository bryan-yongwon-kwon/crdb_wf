import typer
import os
from storage_workflows.crdb.api_gateway.auto_scaling_group_gateway import AutoScalingGroupGateway
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_global_env import setup_global_env
from storage_workflows.setup_env import setup_env
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

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

            # standby will reduce the node count by one
            # decrease min-capacity by one if min-capacity is equal to current capacity
            AutoScalingGroupGateway.decrease_min_capacity(asg_name)

            # enter standby mode
            AutoScalingGroupGateway.enter_instances_into_standby(asg_name, [node.instance_id])

            # Drain and restart
            node.drain()

            # exit standby mode
            AutoScalingGroupGateway.exit_instances_from_standby(asg_name, [node.instance_id])

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


@app.command()
def run_ipu_tasks():
    # Initialize a BlockingScheduler.
    # BlockingScheduler is a type of scheduler provided by APScheduler.
    # It runs in the foreground (blocking the main thread) and is useful for scripts that have no I/O loop.
    scheduler = BlockingScheduler()

    def job_executed_listener(event):
        # This function is called when a job is executed successfully.
        # It logs a message and then shuts down the scheduler.
        logger.info("Job executed successfully.")
        scheduler.shutdown(wait=False)

    def job_error_listener(event):
        # This function is called when a job encounters an error.
        # It logs an error message with the exception that occurred and then shuts down the scheduler.
        logger.error(f"Job encountered an error: {event.exception}")
        scheduler.shutdown(wait=False)

    # Attach event listeners to the scheduler.
    # EVENT_JOB_EXECUTED is triggered when a job is successfully executed.
    # EVENT_JOB_ERROR is triggered when a job execution fails with an error.
    scheduler.add_listener(job_executed_listener, EVENT_JOB_EXECUTED)
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)

    # The 'date' trigger is used here to schedule a job for a specific point in time.
    # Since 'run_date' is not specified, it defaults to now (i.e., immediate execution).
    scheduler.add_job(lambda: update_and_drain_nodes(), 'date')

    # Start the scheduler.
    # The scheduler will execute the scheduled job and then shut down (either on successful completion or error),
    # based on the event listeners defined above.
    scheduler.start()


if __name__ == "__main__":
    try:
        # Set up the global environment variables.
        setup_global_env()

        # Run the scheduled tasks.
        run_ipu_tasks()
    except (KeyboardInterrupt, SystemExit):
        # Handle keyboard interrupt or system exit (like Ctrl+C).
        # This block ensures graceful shutdown in case of such interruptions.
        logger.info("Scheduler stopped.")
