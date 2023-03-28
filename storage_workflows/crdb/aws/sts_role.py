from storage_workflows.crdb.api_gateway.sts_gateway import StsGateway


class StsRole:

    @staticmethod
    def assume_role():
        return StsRole(StsGateway.assume_role())
    
    def __init__(self, api_response: dict):
        self._api_response = api_response

    @property
    def access_key_id(self):
        return self._api_response['Credentials']['AccessKeyId']
    
    @property
    def secret_access_key(self):
        return self._api_response['Credentials']['SecretAccessKey']
    
    @property
    def session_token(self):
        return self._api_response['Credentials']['SessionToken']