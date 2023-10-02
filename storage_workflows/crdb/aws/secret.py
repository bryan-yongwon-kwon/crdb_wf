from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway


class Secret:

    @staticmethod
    def find_all_secrets(filters: list) -> list:
        return list(map(lambda secret: Secret(secret), 
                        SecretManagerGateway.list_secrets(filters)))

    def __init__(self, api_response):
        self._api_response = api_response

    @property
    def arn(self):
        return self._api_response['ARN']

    @property
    def name(self):
        return self._api_response.get('Name', 'N/A')

    @property
    def description(self):
        return self._api_response.get('Description', 'N/A')

    def get_secret_value(self):
        secret_data = SecretManagerGateway.find_secret(self.arn)
        print(f"Keys in secret_data: {list(secret_data.keys())}")  # Debugging line
        # Assuming the key is 'SecretString', which is common for AWS Secrets Manager
        return secret_data.get('SecretString', 'N/A')
