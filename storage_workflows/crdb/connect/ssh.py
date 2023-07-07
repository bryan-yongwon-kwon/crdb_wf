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
        print(stdout.readlines())
        error = stderr.readlines()
        if error:
            raise Exception(error)

    def list_remote_dir(self, dir_path) -> list:
        return self.sftp_client.listdir(dir_path)
    
    def list_remote_dir_with_root(self, dir_path) -> list:
        stdin, stdout, stderr = self.execute_command('sudo ls {}'.format(dir_path))
        lines = list(map(lambda line: str(line).rstrip(), stdout.readlines()))
        print(lines)
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
        print(lines)
        error = stderr.readlines()
        if error:
            raise Exception(error)
        return lines

