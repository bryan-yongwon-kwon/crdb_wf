import os
from io import StringIO
from functools import cached_property
from paramiko import SSHClient,RSAKey
from paramiko import AutoAddPolicy

class SSH:

    @cached_property
    def ssh_private_key(self):
        return RSAKey.from_private_key(StringIO(os.getenv('SSH_PRIVATE_KEY')), password=os.getenv('SSH_KEY_PASSPHRASE'))

    @cached_property
    def client(self):
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        return ssh_client
    
    def connect_to_node(self, ip):
        self.client.connect(ip, username='ubuntu', pkey=self.ssh_private_key)

    def execute_command(self, command):
        return self.client.exec_command(command)
