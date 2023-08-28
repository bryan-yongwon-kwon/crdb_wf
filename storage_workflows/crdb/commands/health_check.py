import json
import os
import typer
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.logging.logger import Logger
from storage_workflows.crdb.models.cluster import Cluster
from storage_workflows.crdb.slack.content_templates import ContentTemplate
from storage_workflows.setup_env import setup_env
from storage_workflows.slack.slack_notification import SlackNotification
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance

app = typer.Typer()
logger = Logger()


@app.command()
def get_cluster_names(deployment_env, region):
    setup_env(deployment_env, region)
    asgs = AutoScalingGroup.find_all_auto_scaling_groups([AutoScalingGroup.build_filter_by_crdb_tag()])
    names = list(map(lambda asg: asg.name.split("_{}-".format(deployment_env))[0], asgs))
    names.sort()
    logger.info("Found {} clusters.".format(len(names)))
    logger.info(names)
    output_file = open("/tmp/cluster_names.json", "w")
    output_file.write(json.dumps(names))
    output_file.close()

# the deployment_env is either staging or prod
# the region is us-west-2 in most cases
# the cluster name is the one with underscore like ao_test
@app.command()
def asg_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    unhealthy_asg_instances = list(filter(lambda instance: not instance.in_service(), asg.instances))
    if unhealthy_asg_instances:
        unhealthy_asg_instance_ids = list(map(lambda instance: instance.instance_id, unhealthy_asg_instances))
        logger.warning("Displaying all unhealthy instances for the $cluster_name cluster:")
        logger.warning(unhealthy_asg_instance_ids)
        logger.warning("Auto Scaling Group name: {}".format(asg.name))
    # TODO: Write result into metadata DB


@app.command()
def changefeed_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()
    unhealthy_changefeed_jobs = list(filter(lambda job: job.status != "running" and job.status != "canceled", cluster.changefeed_jobs))
    if unhealthy_changefeed_jobs:
        logger.warning("Changefeeds Not Running:")
        for job in unhealthy_changefeed_jobs:
            logger.warning("Job id is {}. Status is {}.".format(job.id, job.status))
    # TODO: Write result into metadata DB 

@app.command()
def orphan_health_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    cluster = Cluster()

    # Get the IP count of AWS instances
    aws_cluster_instance_count = len(Ec2Instance.find_ec2_instances_by_cluster_tag(cluster_name))

    # Get the IP count of CRDB nodes
    crdb_cluster_instance_count = len(list(map(lambda response: response['Node.ipaddress'], cluster.nodes)))
    
    # Compare the IP count of AWS instances and CRDB nodes
    if aws_cluster_instance_count != crdb_cluster_instance_count:
        crdb_cluster_instance_ips = list(map(lambda response: response['Node.ipaddress'], cluster.nodes))
        orphan_instances = list(map(lambda instance: {"InstanceId": instance.instance_id, "PrivateIpAddress": instance.private_ip_address},
                                    filter(lambda instance: instance.private_ip_address not in crdb_cluster_instance_ips, Ec2Instance.find_ec2_instances_by_cluster_tag(cluster_name))))
        logger.warning("Orphan instances found.")
        logger.warning("AWS instance count is {} and CRDB instance count is {}.".format(aws_cluster_instance_count, crdb_cluster_instance_count))
        logger.warning("Orphan instances are:".format(orphan_instances))
    # TODO: Write result into metadata DB 


@app.command()
def send_slack_notification(deployment_env):
    #TODO: Read healthy check result from metadata DB
    results = ["text1", "text2"] # this is a place holder
    webhook_url = os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB') if deployment_env == 'prod' else os.getenv('SLACK_WEBHOOK_STORAGE_ALERTS_CRDB_STAGING')
    notification = SlackNotification(webhook_url)
    notification.send_notification(ContentTemplate.get_health_check_template(results))
