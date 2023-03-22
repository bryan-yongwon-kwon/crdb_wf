class SecretManagerGateway:

    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def list_secrets(secret_manager_aws_client, filters=[], next_token=''):
        if not next_token:
            response = secret_manager_aws_client.list_secrets(Filters=filters, MaxResults=SecretManagerGateway.PAGINATOR_MAX_RESULT_PER_PAGE, IncludePlannedDeletion=False)
        else:
            response = secret_manager_aws_client.list_secrets(Filters=filters, 
                                                MaxResults=SecretManagerGateway.PAGINATOR_MAX_RESULT_PER_PAGE,
                                                IncludePlannedDeletion=False,
                                                NextToken=next_token)
        if 'NextToken' in response:
            response['SecretList'].extend(
                SecretManagerGateway.list_secrets(secret_manager_aws_client, filters, response['NextToken']))
        return response['SecretList']
    
    @staticmethod
    def find_secret(secret_manager_aws_client, secret_arn):
        return secret_manager_aws_client.get_secret_value(SecretId=secret_arn)