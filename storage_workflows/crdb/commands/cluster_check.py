import typer
from storage_workflows.setup_env import setup_env
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.crdb.cluster.node import Node

app = typer.Typer()

@app.command()
def check_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    node_ip_list = list(map(lambda node: node.ip_address, Node.get_nodes()))
    ssh_client = SSH()
    for ip in node_ip_list:
        ssh_client.connect_to_node(ip)
        stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")


if __name__ == "__main__":
    app()