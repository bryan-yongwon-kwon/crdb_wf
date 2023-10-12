from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
import botocore.exceptions


class SecretManagerGateway:
    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def _get_secret_manager_client():
        """Retrieve the AWS Secrets Manager client."""
        return AwsSessionFactory.secret_manager()

    @staticmethod
    def list_secrets(filters=None, next_token=None):
        """
        List secrets based on filters.

        Args:
        - filters (list): List of filter conditions.
        - next_token (str): Token for pagination.

        Returns:
        - list: List of secrets.
        """
        if filters is None:
            filters = []

        if len(filters) > 10:
            raise ValueError("Number of filters exceeds the allowed limit of 10.")

        secret_manager_aws_client = SecretManagerGateway._get_secret_manager_client()

        request_params = {
            'Filters': filters,
            'MaxResults': SecretManagerGateway.PAGINATOR_MAX_RESULT_PER_PAGE,
            'IncludePlannedDeletion': False
        }

        if next_token:
            request_params['NextToken'] = next_token

        try:
            response = secret_manager_aws_client.list_secrets(**request_params)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ValidationException':
                # Generate a string representation of the filters for the error message
                filters_str = ', '.join([f"{f.get('Key')}={f.get('Values')}" for f in filters])
                raise ValueError(f"Validation error with filters: {filters_str}") from e
            raise

        secrets = response.get('SecretList', [])

        # Check for more secrets (pagination)
        if 'NextToken' in response:
            secrets.extend(
                SecretManagerGateway.list_secrets(filters, response['NextToken'])
            )

        return secrets

    @staticmethod
    def find_secret(secret_arn):
        """Retrieve the value of a specific secret based on its ARN.

        Args:
        - secret_arn (str): ARN of the secret.

        Returns:
        - dict: Secret value data.
        """
        secret_manager_aws_client = SecretManagerGateway._get_secret_manager_client()

        try:
            return secret_manager_aws_client.get_secret_value(SecretId=secret_arn)
        except botocore.exceptions.ClientError as e:
            # Handle specific errors or re-raise the exception
            raise
