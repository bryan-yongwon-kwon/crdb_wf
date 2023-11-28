import os
import subprocess
import hashlib
from storage_workflows.logging.logger import Logger
from storage_workflows.setup_env import setup_env

logger = Logger()


class CRDBBinary:
    def __init__(self, version, deployment_env, region, cluster_name):
        self.version = version
        self.deployment_env = deployment_env
        self.region = region
        self.cluster_name = cluster_name
        setup_env(deployment_env, region, cluster_name)

    @staticmethod
    def determine_instance_architecture():
        try:
            # Querying EC2 metadata to get the instance type
            instance_type = subprocess.check_output(
                ["ec2metadata", "--instance-type"], text=True
            ).strip()

            # Logic to determine if it's Intel or Graviton
            if "m6g" in instance_type or "m7g" in instance_type:
                return "linux-arm64"
            else:
                return "linux-amd64"
        except subprocess.CalledProcessError as e:
            logger.error(f"Error determining instance architecture: {e}")
            return "linux-amd64"  # Defaulting to Intel if unable to determine

    @staticmethod
    def compute_checksum(self, file_path):
        """Compute the SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256.update(block)
        return sha256.hexdigest()

    def download_binary(self):
        architecture = self.determine_instance_architecture()
        binary_url = f"https://binaries.cockroachdb.com/cockroach-{self.version}.{architecture}.tgz"
        local_binary_dir = f"cockroach-{self.version}.{architecture}"

        logger.info(f"Downloading CockroachDB binary from {binary_url}")
        subprocess.run(['wget', '-q', binary_url], check=True)
        logger.info("Download completed. Extracting the binary.")
        subprocess.run(['tar', '-xvf', f"{local_binary_dir}.tgz"], check=True)
        logger.info("Extraction completed.")
        return os.path.join(os.getcwd(), local_binary_dir)  # Return the directory where the binary is located

    def deploy_binary_to_node(self, node):
        binary_dir = self.download_binary()
        cockroach_binary_path = os.path.join(binary_dir, 'cockroach')
        remote_path = '/tmp/cockroach'

        node.ssh_client.connect_to_node()
        try:
            node.ssh_client.scp_to_node(cockroach_binary_path, remote_path)
            # Here you can add more logic if you need to copy more files or perform additional setup
        finally:
            node.ssh_client.close_connection()
