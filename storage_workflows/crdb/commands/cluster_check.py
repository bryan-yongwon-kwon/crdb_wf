import typer
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.crdb.metadata_db.metadata_db_operations import MetadataDBOperations
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env

app = typer.Typer()
logger = Logger()

CRONTAB_SCRIPTS_DIR = '/root/.cockroach-certs/'

@app.command()
def check_crontab(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    metadata_db_operations = MetadataDBOperations()
    instance_ids = metadata_db_operations.get_old_nodes(cluster_name, deployment_env)
    old_instance_ips = set(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).private_ip_address, instance_ids))
    nodes = Node.get_nodes()
    new_nodes_list = list(filter(lambda node: node.ip_address not in old_instance_ips, nodes))
    new_node_ssh_client = SSH(new_nodes_list[0].ip_address)
    new_node_ssh_client.connect_to_node()
    for ip in old_instance_ips:
        ssh_client = SSH(ip)
        ssh_client.connect_to_node()
        stdin, stdout, stderr = ssh_client.execute_command("sudo crontab -l")
        lines = stdout.readlines()
        errors = stderr.readlines()
        logger.info("Listing cron jobs for {}: {}".format(ip, lines))
        if errors:
            continue
        copy_cron_scripts_to_new_node(ssh_client, new_node_ssh_client)
        schedule_cron_jobs(lines, new_node_ssh_client)
        ssh_client.close_connection()


def copy_cron_scripts_to_new_node(old_node_ssh_client: SSH, new_node_ssh_client: SSH):
    old_node_script_names = filter(lambda file_name: '.sh' in file_name, 
                                   old_node_ssh_client.list_remote_dir_with_root(CRONTAB_SCRIPTS_DIR))
    for script_name in old_node_script_names:
        logger.info("Moving script {} from {} to {}".format(script_name, old_node_ssh_client.ip, new_node_ssh_client.ip))
        file_path = CRONTAB_SCRIPTS_DIR + script_name
        lines = old_node_ssh_client.read_remote_file_with_root(file_path)
        new_node_ssh_client.write_remote_file_with_root(file_lines=lines, file_path=file_path)

def schedule_cron_jobs(crontab_file_lines:list, new_node_ssh_client:SSH):
    new_node_ssh_client.execute_command('sudo mkdir /var/log/crdb/export_logs && \
                                        sudo ln -s /var/log/crdb/export_logs /root/export_logs && \
                                        sudo ln -s /var/log/crdb/export_logs /root/.cockroach-certs/export_logs')
    for line in crontab_file_lines:
        line = str(line).rstrip()
        if line[0] == '#':
            continue
        logger.info("scheduling cron: {}".format(line))
        command = '(sudo crontab -l 2>/dev/null; echo "{}") | sudo crontab -'.format(line)
        logger.info("command: {}".format(command))
        stdin, stdout, stderr = new_node_ssh_client.execute_command(command)
        error = stderr.readlines()
        if error:
            raise Exception(error)
        logger.info("cron job scheduled.")



if __name__ == "__main__":
    app()