import os
from functools import cached_property
from paramiko import SSHClient,RSAKey

class SSHClient:

    @cached_property
    def ssh_private_key(self):
        return RSAKey.from_private_key(os.getenv('DEPLOYMENT_ENV'), os.getenv('PASSPHRASE'))

    @cached_property
    def client(self):
        return SSHClient()
    
    def connect_to_node(self, ip):
        self.client.connect(ip, username='ubuntu')

    def execute_command(self, command):
        return self.client.exec_command(command)
