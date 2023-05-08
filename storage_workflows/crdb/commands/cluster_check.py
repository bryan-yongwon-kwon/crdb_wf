import typer
from storage_workflows.setup_env import setup_env
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.crdb.cluster.node import Node
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup

app = typer.Typer()

CRONTAB_SCRIPTS_DIR = '/root/.cockroach-certs/'

@app.command()
def check_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    nodes = Node.get_nodes()
    nodes.sort(key=lambda node: node.started_at, reverse=True)
    node_ip_list = list(map(lambda node: node.ip_address, nodes))
    print("New node IP: {}".format(nodes[0].ip_address))
    new_node_ssh_client = SSH(nodes[0].ip_address)
    new_node_ssh_client.connect_to_node()
    for ip in node_ip_list:
        ssh_client = SSH(ip)
        print("ip: {}".format(ip))
        ssh_client.connect_to_node()
        stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")
        errors = stderr.readlines()
        print("stdout: {}".format(stdout.readlines()))
        print("stderr: {}".format(errors))
        if errors:
            continue
        copy_cron_scripts_to_new_node(ssh_client, new_node_ssh_client)


def copy_cron_scripts_to_new_node(old_node_ssh_client: SSH, new_node_ssh_client: SSH):
    old_node_script_names = filter(lambda file_name: '.sh' in file_name, 
                                   old_node_ssh_client.list_remote_dir_with_root(CRONTAB_SCRIPTS_DIR))
    for script_name in old_node_script_names:
        print("Moving script {} from {} to {}".format(script_name, old_node_ssh_client.ip, new_node_ssh_client.ip))
        file_path = CRONTAB_SCRIPTS_DIR + script_name
        lines = old_node_ssh_client.read_remote_file_with_root(file_path)
        new_node_ssh_client.write_remote_file_with_root(file_lines=lines, file_path=file_path)



if __name__ == "__main__":
    app()