import os
import subprocess
from functools import cached_property, reduce
from storage_workflows.crdb.api_gateway.crdb_api_gateway import CrdbApiGateway
from storage_workflows.crdb.connect.ssh import SSH
from storage_workflows.logging.logger import Logger

logger = Logger()
class Node:

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

    @property
    def cluster_name(self):
        return os.getenv('CLUSTER_NAME')

    @property
    def id(self):
        return self.api_response['node_id']
    
    @property
    def major_version(self):
        return self.api_response['ServerVersion']['major']

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
        return reduce(lambda replica_count_1, replica_count_2: replica_count_1+replica_count_2, replicas_list)
    
    @cached_property
    def ssh_client(self):
        return SSH(self.ip_address)
    
    def reload(self):
        self.api_response = list(filter(lambda node: node.id == self.id, Node.get_nodes()))[0].api_response

    def drain(self):
        certs_dir = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + self.cluster_name + "/"
        cluster_name = "{}-{}".format(self.cluster_name.replace('_', '-'), os.getenv('DEPLOYMENT_ENV'))
        node_drain_command = "crdb{} node drain {} --host={}:26256 --certs-dir={} --cluster-name={}".format(self.major_version, 
                                                                                                            self.id, 
                                                                                                            self.ip_address, 
                                                                                                            certs_dir, 
                                                                                                            cluster_name)
        result = subprocess.run(node_drain_command, capture_output=True, shell=True)
        logger.error(result.stderr)
        result.check_returncode()
        logger.info(result.stdout)
