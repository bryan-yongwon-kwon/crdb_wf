from storage_workflows.crdb.api_gateway.secret_manager_gateway import SecretManagerGateway


class Secret:

    @staticmethod
    def find_all_secrets(secret_manager_aws_client, filters: list) -> list:
        return list(map(lambda secret: Secret(secret), 
                        SecretManagerGateway.list_secrets(secret_manager_aws_client, filters)))

    def __init__(self, api_response):
        self._api_response = api_response

    def secret_arn(self):
        return self._api_response['ARN']
