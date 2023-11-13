import time
import typer
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.slack.content_templates import ContentTemplate
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification

app = typer.Typer()
logger = Logger()

SNAPSHOT_REBALANCE_RATE = 'kv.snapshot_rebalance.max_rate'
SNAPSHOT_RECOVERY_RATE = 'kv.snapshot_recovery.max_rate'

@app.command()
def check_avg_cpu(deployment_env, region, cluster_name, namespace, workflow_id, is_test:bool):
    AVG_CPU_THRESHOLD = 0.5 # 50% CPU usage
    OFFSET_MINS = 5 # > threshold for OFFSET_MINS minutes to trigger action
    REBALANCE_RATE_THRESHOLD = '1 MiB' # if below this threshold, should not keep reducing
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    slack_notification = SlackNotification.config_notification(deployment_env, is_test)
    if cluster.is_avg_cpu_exceed_threshold(AVG_CPU_THRESHOLD, OFFSET_MINS):
        rebalance_rate = cluster.get_cluster_setting(SNAPSHOT_REBALANCE_RATE)
        recovery_rate = cluster.get_cluster_setting(SNAPSHOT_RECOVERY_RATE)
        logger.info("Average CPU usage is above threshold. Current rebalance rate: {}, recovery rate: {}".format(rebalance_rate.value, recovery_rate.value))
        if (rebalance_rate.compare_value_with(REBALANCE_RATE_THRESHOLD) <= 0):
            logger.info("Rebalance rate is at threshold, should not keep reducing.")
            logger.info("Average CPU usage is still above threshold. Sending Slack notification.")
            slack_notification.send_notification(ContentTemplate.get_average_cpu_high_alert_template(namespace,
                                                                                                     workflow_id,
                                                                                                     cluster_name,
                                                                                                     rebalance_rate.value,
                                                                                                     recovery_rate.value))
            return
        logger.info("Average CPU usage is above threshold. Start reducing balancing rate.")
        reduce_rebalance_rate(deployment_env, region, cluster_name, namespace, workflow_id, is_test)
        logger.info("Reducing rebalance rate completed. Wait for 5 minutes to check again.")
        time.sleep(300)
        if cluster.is_avg_cpu_exceed_threshold(AVG_CPU_THRESHOLD, OFFSET_MINS):
            logger.info("Average CPU usage is still above threshold. Sending Slack notification.")
            rebalance_rate.refresh()
            recovery_rate.refresh()
            slack_notification.send_notification(ContentTemplate.get_average_cpu_high_alert_template(namespace,
                                                                                                     workflow_id,
                                                                                                     cluster_name,
                                                                                                     rebalance_rate.value,
                                                                                                     recovery_rate.value))
            return
    logger.info("Average CPU usage is below threshold. No action needed.")

@app.command()
def reduce_rebalance_rate(deployment_env, region, cluster_name, namespace, workflow_id, is_test):
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    rebalance_rate = cluster.get_cluster_setting(SNAPSHOT_REBALANCE_RATE)
    logger.info("Current rebalance rate: {}".format(rebalance_rate.value))
    recovery_rate = cluster.get_cluster_setting(SNAPSHOT_RECOVERY_RATE)
    logger.info("Current recovery rate: {}".format(recovery_rate.value))
    if rebalance_rate.value != recovery_rate.value:
        logger.error("Rebalance rate and recovery rate are not equal. Please check.")
        logger.info('Skip reducing rebalance rate.')
        slack_notification = SlackNotification.config_notification(deployment_env, is_test)
        slack_notification.send_notification(ContentTemplate.get_rebalance_and_recovery_rates_not_match_alert_template(namespace,
                                                                                                                       workflow_id,
                                                                                                                       cluster_name,
                                                                                                                       rebalance_rate.value,
                                                                                                                       recovery_rate.value))
        return
    new_rate = "{} MiB".format(int(str(rebalance_rate.value).split(' ')[0])//2)
    logger.info("New rate will be: {}".format(new_rate))
    cluster.update_cluster_setting(SNAPSHOT_REBALANCE_RATE, new_rate)
    logger.info("New rebalance rate: {}".format(new_rate))
    cluster.update_cluster_setting(SNAPSHOT_RECOVERY_RATE, new_rate)
    logger.info("New recovery rate: {}".format(new_rate))
    logger.info("Reducing rebalance rate completed.")