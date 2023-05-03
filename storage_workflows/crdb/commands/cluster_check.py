import typer
from storage_workflows.setup_env import setup_env
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.crdb.cluster.node import Node
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup

app = typer.Typer()

@app.command()
def check_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name)
    instances = asg.instances
    for instance in instances:
        print("LaunchTime: {}".format(instance.launch_time))
    node_ip_list = list(map(lambda node: node.ip_address, Node.get_nodes()))
    ssh_client = SSH()
    for ip in node_ip_list:
        print("ip: {}".format(ip))
        ssh_client.connect_to_node(ip)
        stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")
        print("stdout: {}".format(stdout.readlines()))
        print("stderr: {}".format(stderr.readlines()))


if __name__ == "__main__":
    app()