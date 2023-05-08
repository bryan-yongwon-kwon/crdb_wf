import os
import stat
from io import StringIO
from functools import cached_property
from paramiko import SSHClient,RSAKey
from paramiko import AutoAddPolicy

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

    def execute_command(self, command):
        return self.client.exec_command(command)
    
    def write_remote_file(self, file_lines, file_path, chmod=stat.S_IROTH):
        sftp_file = self.sftp_client.open(file_path, 'w')
        sftp_file.chmod(chmod)
        sftp_file.writelines(file_lines)
        sftp_file.close()

    def write_remote_file_with_root(self, file_lines, file_path, chmod=stat.S_IROTH):
        temp_file_dir = '/temp_file/'
        file_name = file_lines.split('/')[-1]
        temp_file_path = temp_file_dir + file_name
        self.write_remote_file(file_lines, temp_file_path)
        stdin, stdout, stderr = self.execute_command('sudo su mv {} {}'.format(temp_file_path, file_path))
        print(stdout.readlines())
        print(stderr.readlines())
    
    def create_remote_dir(self, dir_path):
        self.sftp_client.mkdir(dir_path)

    def create_remote_dir_with_root(self, dir_path):
        stdin, stdout, stderr = self.execute_command('sudo mkdir {}'.format(dir_path))
        print(stdout.readlines())
        print(stderr.readlines())

    def list_remote_dir(self, dir_path) -> list:
        return self.sftp_client.listdir(dir_path)
    
    def list_remote_dir_with_root(self, dir_path) -> list:
        self.execute_command('sudo su -')
        stdin, stdout, stderr = self.execute_command('ls {}'.format(dir_path))
        print("List remote dir with root:")
        print(stdout.readlines())
        print(stderr.readlines())
        return stdout.readlines()
    
    def read_remote_file(self, file_path):
        sftp_file = self.sftp_client.open(file_path, 'r')
        lines = sftp_file.readlines()
        sftp_file.close()
        return lines
    
    def read_remote_file_with_root(self, file_path):
        stdin, stdout, stderr = self.execute_command('sudo cat file_path')
        print("Read remote file with root:")
        print(stdout.readlines())
        print(stderr.readlines())
        return stdout.readlines()

