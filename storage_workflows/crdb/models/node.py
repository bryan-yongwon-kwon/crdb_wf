import os
import subprocess
import datetime
from functools import cached_property, reduce
from storage_workflows.crdb.api_gateway.crdb_api_gateway import CrdbApiGateway
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.logging.logger import Logger

logger = Logger()


class Node:
    CRONTAB_SCRIPTS_DIR = '/root/.cockroach-certs/'

    @staticmethod
    def get_nodes():
        session = CrdbApiGateway.login()
        return list(map(lambda node: Node(node), CrdbApiGateway.list_nodes(session)['nodes']))

    @staticmethod
    def stop_crdb(ip):
        ssh_client = SSH(ip)
        ssh_client.connect_to_node()
        logger.info("Stopping crdb on node {}...".format(ip))
        stdin, stdout, stderr = ssh_client.execute_command("sudo systemctl stop crdb")
        stdin.close()
        lines = stdout.readlines()
        errors = stderr.readlines()
        if errors:
            logger.error("Stopping crdb failed!")
            logger.error(errors)
        else:
            logger.info(lines)
            logger.info("Stopped crdb on node {}".format(ip))
        ssh_client.close_connection()

    @staticmethod
    def start_crdb(ip):
        ssh_client = SSH(ip)
        ssh_client.connect_to_node()
        logger.info("Starting crdb on node {}...".format(ip))
        stdin, stdout, stderr = ssh_client.execute_command("sudo systemctl start crdb")
        stdin.close()
        lines = stdout.readlines()
        errors = stderr.readlines()
        if errors:
            logger.error("Starting crdb failed!")
            logger.error(errors)
        else:
            logger.info(lines)
            logger.info("Started crdb on node {}".format(ip))
        ssh_client.close_connection()

    def __init__(self, api_response):
        self.api_response = api_response

    @cached_property
    def instance_id(self):
        return self.get_instance_id()

    @property
    def cluster_name(self):
        return os.getenv('CLUSTER_NAME')

    @property
    def deployment_env(self):
        return os.getenv('DEPLOYMENT_ENV')

    @property
    def id(self):
        return self.api_response['node_id']

    @property
    def major_version(self):
        return self.api_response['ServerVersion']['major']

    @property
    def minor_version(self):
        return self.api_response['ServerVersion']['major']['minor']

    @property
    def ip_address(self):
        return str(self.api_response['address']['address_field']).split(":")[0]

    @property
    def started_at(self):
        return self.api_response['started_at']

    @property
    def sql_conns(self):
        return int(self.api_response['metrics']['sql.conns'])

    @property
    def replicas(self):
        stores = CrdbApiGateway.get_node_details_from_endpoint(CrdbApiGateway.login(), self.id)['storeStatuses']
        replicas_list = map(lambda store: int(store['metrics']['replicas']), stores)
        return reduce(lambda replica_count_1, replica_count_2: replica_count_1 + replica_count_2, replicas_list)

    @property
    def overreplicated_ranges(self):
        stores = CrdbApiGateway.get_node_details_from_endpoint(CrdbApiGateway.login(), self.id)['storeStatuses']
        ranges_list = map(lambda store: int(store['metrics']['ranges.overreplicated']), stores)
        return reduce(lambda range_count_1, range_count_2: range_count_1 + range_count_2, ranges_list)

    @property
    def unavailable_ranges(self):
        stores = CrdbApiGateway.get_node_details_from_endpoint(CrdbApiGateway.login(), self.id)['storeStatuses']
        ranges_list = map(lambda store: int(store['metrics']['ranges.unavailable']), stores)
        return reduce(lambda range_count_1, range_count_2: range_count_1 + range_count_2, ranges_list)

    @property
    def underreplicated_ranges(self):
        stores = CrdbApiGateway.get_node_details_from_endpoint(CrdbApiGateway.login(), self.id)['storeStatuses']
        ranges_list = map(lambda store: int(store['metrics']['ranges.underreplicated']), stores)
        return reduce(lambda range_count_1, range_count_2: range_count_1 + range_count_2, ranges_list)

    @property
    def applied_initial_snapshots(self):
        stores = CrdbApiGateway.get_node_details_from_endpoint(CrdbApiGateway.login(), self.id)['storeStatuses']
        snapshots_list = map(lambda store: int(store['metrics']['range.snapshots.applied-initial']), stores)
        return reduce(lambda range_count_1, range_count_2: range_count_1 + range_count_2, snapshots_list)

    @cached_property
    def ssh_client(self):
        return SSH(self.ip_address)

    def reload(self):
        self.api_response = list(filter(lambda node: node.id == self.id, Node.get_nodes()))[0].api_response

    def drain(self):
        certs_dir = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + self.cluster_name + "/"
        CrdbConnection.get_crdb_connection(self.cluster_name)
        formatted_cluster_name = "{}-{}".format(self.cluster_name.replace('_', '-'), os.getenv('DEPLOYMENT_ENV'))
        node_drain_command = "crdb{} node drain {} --host={}:26256 --certs-dir={} --cluster-name={}".format(
            self.major_version,
            self.id,
            self.ip_address,
            certs_dir,
            formatted_cluster_name)
        result = subprocess.run(node_drain_command, capture_output=True, shell=True)
        logger.error(result.stderr)
        result.check_returncode()
        logger.info(result.stdout)
        # need restart the service so that the node won't be marked as dead in crdb console
        logger.info("Restarting the service...")
        self.ssh_client.connect_to_node()
        stdin, stdout, stderr = self.ssh_client.execute_command("sudo systemctl restart crdb")
        error = stderr.readline()
        if error:
            logger.error(error)
        self.ssh_client.close_connection()
        logger.info("Service restarted.")

    def get_instance_id(self):
        """Fetches the instance ID using ec2metadata command."""
        try:
            self.ssh_client.connect_to_node()
            stdin, stdout, stderr = self.ssh_client.execute_command("ec2metadata --instance-id")
            instance_id = stdout.read().decode().strip()
            logger.info(f"instance_id from ec2metadata: {instance_id}")
            return instance_id
        except Exception as e:
            logger.error(f"Error fetching instance ID : {e}")
            return None
        finally:
            self.ssh_client.close_connection()

    def schedule_cron_jobs(self, crontab_file_lines: list):
        def cron_job_already_exists(ssh_client: SSH, job: str) -> bool:
            command = 'sudo crontab -l'.format(job)
            stdin, stdout, stderr = ssh_client.execute_command(command)
            jobs = set(map(lambda job: str(job).rstrip(), stdout.readlines()))
            return job in jobs

        new_node_ssh_client = self.ssh_client
        new_node_ssh_client.connect_to_node()
        new_node_ssh_client.execute_command('sudo mkdir /var/log/crdb/export_logs && \
                                            sudo ln -s /var/log/crdb/export_logs /root/export_logs && \
                                            sudo ln -s /var/log/crdb/export_logs /root/.cockroach-certs/export_logs')
        for line in crontab_file_lines:
            line = str(line).rstrip()
            if line[0] == '#' or cron_job_already_exists(new_node_ssh_client, line):
                continue
            logger.info("scheduling cron: {}".format(line))
            command = '(sudo crontab -l 2>/dev/null; echo "{}") | sudo crontab -'.format(line)
            logger.info("command: {}".format(command))
            stdin, stdout, stderr = new_node_ssh_client.execute_command(command)
            error = stderr.readlines()
            if error:
                raise Exception(error)
            logger.info("cron job scheduled.")
        new_node_ssh_client.close_connection()

    def copy_cron_scripts_from_old_node(self, old_node_ssh_client: SSH):
        new_node_ssh_client = self.ssh_client
        new_node_ssh_client.connect_to_node()
        old_node_ssh_client.connect_to_node()
        old_node_script_names = filter(lambda file_name: '.sh' in file_name,
                                       old_node_ssh_client.list_remote_dir_with_root(Node.CRONTAB_SCRIPTS_DIR))
        for script_name in old_node_script_names:
            logger.info(
                "Moving script {} from {} to {}".format(script_name, old_node_ssh_client.ip, new_node_ssh_client.ip))
            file_path = Node.CRONTAB_SCRIPTS_DIR + script_name
            lines = old_node_ssh_client.read_remote_file_with_root(file_path)
            new_node_ssh_client.write_remote_file_with_root(file_lines=lines, file_path=file_path)
        new_node_ssh_client.close_connection()
        old_node_ssh_client.close_connection()

    def check_table_descriptor_corruption(self):
        """
        Checks for table descriptor corruption in this node.
        """
        date = datetime.datetime.now().strftime("%Y-%m-%d")

        self.ssh_client.connect_to_node()
        try:
            self.ssh_client.download_debug_zip(self.ip_address, self.cluster_name, date, self.deployment_env)
            self.ssh_client.analyze_debug_zip(self.ip_address, self.cluster_name, date, self.deployment_env)
            self.ssh_client.cleanup_debug_zip(self.ip_address, self.cluster_name, date, self.deployment_env)

        finally:
            self.ssh_client.close_connection()

    def restart_crdb(self):
        logger.info(f"Restarting CockroachDB on node {self.ip_address}...")

        # First, stop the CockroachDB service
        Node.stop_crdb(self.ip_address)

        # Then, start the CockroachDB service
        Node.start_crdb(self.ip_address)

        logger.info(f"CockroachDB restarted on node {self.ip_address}")

    def download_and_setup_crdb(self):
        version = os.environ['CRDB_VERSION']
        ip = self.ip_address  # Retrieve the IP address of the current node

        ssh_client = SSH(ip)
        ssh_client.connect_to_node()  # Establish SSH connection

        try:
            # Function to execute SSH command and log output
            def execute_ssh_command(command):
                stdin, stdout, stderr = ssh_client.execute_command(command)
                output = stdout.readlines()
                error = stderr.readlines()
                if output:
                    logger.info(f'Output for command "{command}": {output}')
                if error:
                    logger.error(f'Error for command "{command}": {error}')
                return output, error

            # Determine instance type for selecting the correct binary
            instance_type_command = "ec2metadata | grep 'instance-type:' | awk '{print $2}'"
            instance_type_output, _ = execute_ssh_command(instance_type_command)
            instance_type = instance_type_output[0].strip() if instance_type_output else ""

            # Determine the correct binary URL
            architecture = 'linux-amd64' if 'm6i.' in instance_type or 'm5.' in instance_type or 'r5.' in instance_type else 'linux-arm64'
            binary_url = f"https://binaries.cockroachdb.com/cockroach-v{version}.{architecture}.tgz"

            # Download the binary
            download_binary_command = f"wget -q {binary_url} -O /tmp/cockroachdb.tgz"
            execute_ssh_command(download_binary_command)

            # Extract the binary
            extract_command = "tar xvf /tmp/cockroachdb.tgz -C /tmp"
            execute_ssh_command(extract_command)

            # Rename 'cockroach' to 'crdb'
            rename_command = f"mv /tmp/cockroach-v{version}.{architecture}/cockroach /tmp/cockroach-v{version}.{architecture}/crdb"
            execute_ssh_command(rename_command.format(version=version))

            # Install the files
            install_commands = [
                f"sudo install /tmp/cockroach-v{version}.{architecture}/crdb /usr/local/bin/crdb",
                f"sudo install /tmp/cockroach-v{version}.{architecture}/lib/libgeos_c.so /usr/local/lib/cockroach/libgeos_c.so",
                f"sudo install /tmp/cockroach-v{version}.{architecture}/lib/libgeos.so /usr/local/lib/cockroach/libgeos.so"
            ]
            for command in install_commands:
                execute_ssh_command(command)

            # Clean up temporary files
            cleanup_command = f"rm -rf /tmp/cockroach-v{version}.{architecture} /tmp/cockroachdb.tgz"
            execute_ssh_command(cleanup_command.format(version=version))

            logger.info(f"CockroachDB version {version} installed successfully.")
        finally:
            ssh_client.close_connection()
