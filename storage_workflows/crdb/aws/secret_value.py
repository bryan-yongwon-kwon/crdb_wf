from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway
import os
import stat

class SecretValue:
    
    @staticmethod
    def find_secret_value(secret_manager_aws_client, arn:str):
        return SecretValue(SecretManagerGateway.find_secret(secret_manager_aws_client, arn))

    def __init__(self, api_response):
        self._api_response = api_response

    def secret_string(self):
        return self._api_response['SecretString']
    
    def print_response(self):
        print(self._api_response)
    
    def write_to_file(self, dir_path, file_name):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = os.path.join(dir_path, file_name)
        file = open(file_path, "w")
        file.write(self.secret_string())
        file.close()
        os.chmod(file_path, stat.S_IREAD|stat.S_IWRITE)