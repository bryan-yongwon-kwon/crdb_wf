import os
from functools import cached_property
from paramiko import SSHClient,RSAKey
from paramiko import AutoAddPolicy

class SSH:

    @cached_property
    def ssh_private_key(self):
        return RSAKey.from_private_key(os.getenv('DEPLOYMENT_ENV'), os.getenv('PASSPHRASE'))

    @cached_property
    def client(self):
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        return ssh_client
    
    def connect_to_node(self, ip):
        self.client.connect(ip, username='ubuntu', pkey=self.ssh_private_key)

    def execute_command(self, command):
        return self.client.exec_command(command)
