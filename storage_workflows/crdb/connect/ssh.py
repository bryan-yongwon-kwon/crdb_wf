import os
import stat
import datetime
import hashlib
from io import StringIO
from functools import cached_property
from paramiko import SSHClient,RSAKey
from paramiko import AutoAddPolicy
from storage_workflows.logging.logger import Logger

logger = Logger()

class SSH:

    def __init__(self, ip):
        self.ip = ip

    @cached_property
    def ssh_private_key(self):
        return RSAKey.from_private_key(StringIO(os.getenv('SSH_PRIVATE_KEY')), password=os.getenv('SSH_KEY_PASSPHRASE'))

    @cached_property
    def client(self):
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        return ssh_client
    
    @cached_property
    def sftp_client(self):
        return self.client.open_sftp()
    
    def connect_to_node(self):
        self.client.connect(self.ip, username='ubuntu', pkey=self.ssh_private_key)

    def close_connection(self):
        self.client.close()

    def execute_command(self, command):
        return self.client.exec_command(command)
    
    def write_remote_file(self, file_lines, file_path, chmod=stat.S_IRWXO):
        sftp_file = self.sftp_client.open(file_path, 'w')
        sftp_file.chmod(chmod)
        sftp_file.writelines(file_lines)
        sftp_file.close()

    def write_remote_file_with_root(self, file_lines, file_path, chmod=stat.S_IRWXO):
        temp_file_dir = '/home/ubuntu/temp_files/'
        file_name = file_path.split('/')[-1]
        temp_file_path = temp_file_dir + file_name
        self.create_remote_dir(temp_file_dir)
        self.write_remote_file(file_lines=file_lines, file_path=temp_file_path, chmod=chmod)
        stdin, stdout, stderr = self.execute_command('sudo mv {} {}'.format(temp_file_path, file_path))
        error = stderr.readlines()
        if error:
            raise Exception(error)
    
    def create_remote_dir(self, dir_path):
        try:
            self.sftp_client.stat(dir_path)
        except FileNotFoundError:
            self.sftp_client.mkdir(dir_path)

    def create_remote_dir_with_root(self, dir_path):
        stdin, stdout, stderr = self.execute_command('sudo mkdir {}'.format(dir_path))
        logger.info(stdout.readlines())
        error = stderr.readlines()
        if error:
            raise Exception(error)

    def list_remote_dir(self, dir_path) -> list:
        return self.sftp_client.listdir(dir_path)
    
    def list_remote_dir_with_root(self, dir_path) -> list:
        stdin, stdout, stderr = self.execute_command('sudo ls {}'.format(dir_path))
        lines = list(map(lambda line: str(line).rstrip(), stdout.readlines()))
        logger.info(lines)
        error = stderr.readlines()
        if error:
            raise Exception(error)
        return lines
    
    def read_remote_file(self, file_path):
        sftp_file = self.sftp_client.open(file_path, 'r')
        lines = sftp_file.readlines()
        sftp_file.close()
        return lines
    
    def read_remote_file_with_root(self, file_path):
        stdin, stdout, stderr = self.execute_command('sudo cat {}'.format(file_path))
        lines = stdout.readlines()
        logger.info(lines)
        error = stderr.readlines()
        if error:
            raise Exception(error)
        return lines

    def download_debug_zip(self, first_node: str, cluster_name: str, date: str, deployment_env: str):
        logger.info("Downloading and creating debug zip for cluster: {}".format(cluster_name))

        # Format cluster_name for the debug zip command
        formatted_cluster_name = cluster_name.replace('_', '-') + '-' + deployment_env

        # Define date_from and date_until (modify as needed for your use case)
        date_from = (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        date_until = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Determine instance type for selecting the correct binary
        instance_type_command = "ec2metadata | grep 'instance-type:' | awk '{print $2}'"
        stdin, stdout, stderr = self.execute_command(instance_type_command)
        instance_type_output = stdout.readlines()
        instance_type = instance_type_output[0].strip() if instance_type_output else ""

        # Select the binary URL based on instance type
        binary_url = ""
        if "m6i." in instance_type or "m5." in instance_type or "r5." in instance_type:
            binary_url = "https://binaries.cockroachdb.com/cockroach-v23.2.0-alpha.6.linux-amd64.tgz"
        elif "m6g." in instance_type or "m7g." in instance_type:
            binary_url = "https://binaries.cockroachdb.com/cockroach-v23.2.0-alpha.6.linux-arm64.tgz"

        if binary_url:
            download_command = f"""
            sudo bash -c '
            cd /root/.cockroach-certs && 
            wget -q {binary_url} && 
            tar xvf $(basename {binary_url}) && 
            ./$(basename {binary_url} .tgz)/cockroach debug zip /data/crdb/crdb_support/{formatted_cluster_name}-{date}.zip --host :26256 --exclude-files=* --files-from="{date_from}" --files-until="{date_until}" --cluster-name {formatted_cluster_name};
            '
            """
            stdin, stdout, stderr = self.execute_command(download_command)
            errors = stderr.readlines()
            if errors:
                raise Exception(f"Error downloading debug zip: {errors}")
            logger.info("Downloaded and created debug zip successfully.")
        else:
            logger.error("Instance type not supported for binary download.")

    def analyze_debug_zip(self, first_node: str, cluster_name: str, date: str, deployment_env: str):
        formatted_cluster_name = cluster_name.replace('_', '-') + '-' + deployment_env
        logger.info("Extracting and analyzing debug zip for cluster: {}".format(cluster_name))
        remote_debug_path = f"/data/crdb/crdb_support/{formatted_cluster_name}-{date}"
        analyze_command = f"""
        mkdir -p {remote_debug_path}; 
        unzip /data/crdb/crdb_support/{formatted_cluster_name}-{date}.zip -d {remote_debug_path};
        crdb debug doctor examine zipdir {remote_debug_path}/debug;
        """
        stdin, stdout, stderr = self.execute_command(analyze_command)
        output = stdout.readlines()
        errors = stderr.readlines()
        if errors:
            raise Exception(f"Error analyzing debug zip: {errors}")

        logger.info(f"checking output: {output}")
        # Check if 'No problems found' is in the output
        if not any("No problems found" in line for line in output):
            raise Exception("Analysis of debug zip indicates problems.")

        logger.info("Debug zip analysis completed successfully.")
        return output

    def cleanup_debug_zip(self, first_node: str, cluster_name: str, date: str, deployment_env: str):
        logger.info("Cleaning up debug zip for cluster: {}".format(cluster_name))
        formatted_cluster_name = cluster_name.replace('_', '-') + '-' + deployment_env
        cleanup_command = f"rm -rf /data/crdb/crdb_support/{formatted_cluster_name}-{date} /data/crdb/crdb_support/{formatted_cluster_name}-{date}.zip"
        stdin, stdout, stderr = self.execute_command(cleanup_command)
        errors = stderr.readlines()
        if errors:
            raise Exception(f"Error cleaning up debug zip: {errors}")
        logger.info("Cleaned up debug zip successfully.")

    @staticmethod
    def compute_checksum(self, file_path):
        """Compute the SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256.update(block)
        return sha256.hexdigest()

    @staticmethod
    def scp_to_node(self, local_path, remote_path):
        """Securely copy a file and its checksum to the remote node and verify the checksum."""
        try:
            # Compute the checksum of the local file
            local_checksum = self.compute_checksum(local_path)
            checksum_file = local_path + '.checksum'
            with open(checksum_file, 'w') as f:
                f.write(local_checksum)

            # Initialize SFTP client
            sftp = self.client.open_sftp()

            # Copy the file and its checksum
            sftp.put(local_path, remote_path)
            sftp.put(checksum_file, remote_path + '.checksum')

            # Verify checksum on the remote node
            verification_command = f"echo '{local_checksum} {remote_path}' | sha256sum -c"
            stdin, stdout, stderr = self.execute_command(verification_command)
            verification_output = stdout.read().decode().strip()
            if "OK" not in verification_output:
                raise Exception(f"Checksum verification failed for {remote_path}")

            logger.info(f"File {local_path} and its checksum copied and verified on node {self.ip}")

            # Close the SFTP client and remove local checksum file
            sftp.close()
            os.remove(checksum_file)

        except Exception as e:
            logger.error(f"Error while copying file to node {self.ip}: {e}")
            raise
