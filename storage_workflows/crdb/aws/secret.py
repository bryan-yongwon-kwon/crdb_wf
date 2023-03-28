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
