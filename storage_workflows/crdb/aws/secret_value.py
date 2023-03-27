from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway
import os
import stat

class SecretValue:
    
    @staticmethod
    def find_secret_value(arn:str):
        return SecretValue(SecretManagerGateway.find_secret(arn))

    def __init__(self, api_response):
        self._api_response = api_response

    @property
    def secret_string(self):
        return self._api_response['SecretString']
    
    def write_to_file(self, dir_path, file_name):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = os.path.join(dir_path, file_name)
        file = open(file_path, "w")
        file.write(self.secret_string())
        file.close()
        os.chmod(file_path, stat.S_IREAD|stat.S_IWRITE)