import pytest
import boto3
import uuid
from botocore.stub import Stubber, ANY
from datetime import datetime
from storage_workflows.crdb.aws.aws_base import AwsBase
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.aws.secret_manager import SecretManager
from storage_workflows.crdb.aws.account_type import AccountType
from storage_workflows.crdb.authentication.cred_type import CredType


def make_secrets(count:int) -> dict:
    secrets = {
        'SecretList': []
    }
    for secret_index in range(0,count):
        secrets['SecretList'].append(
            {
                'ARN': uuid.uuid4().hex
            }
        )
    return secrets

@pytest.mark.parametrize(
    ["secrets", "count"],
    [
        (make_secrets(0), 0),
        (make_secrets(1), 1),
        (make_secrets(2), 2),
    ]
)
def test_get_secret(mocker, secrets, count):
    client = boto3.client('secretsmanager', 'us-west-2')
    def mock_get_client(account_type: AccountType, service_name:str, region:str):
        return client
    mocker.patch('storage_workflows.crdb.aws.aws_base.AwsBase.get_aws_client', mock_get_client)
    stubber = Stubber(client)
    list_secrets_expected_params = {
        'IncludePlannedDeletion': False,
        'MaxResults': AwsBase.PAGINATOR_MAX_RESULT_PER_PAGE,
        'Filters': ANY
    }
    get_secret_value_expected_params = {
        'SecretId': ANY
    }
    secret_string = 'secret'
    mock_content = {
        'SecretString': secret_string
    }
    stubber.add_response('list_secrets', secrets, list_secrets_expected_params)
    stubber.add_response('get_secret_value', mock_content, get_secret_value_expected_params)
    stubber.activate()
    error_message = "Should get exact 1 {} for {}. Got {} now.".format(CredType.CA_CERT_CRED_TYPE.value, 'test_cluster', count)
    secret_manager = SecretManager(AccountType.STAGING, "test_cluster", "us-west-2")
    if count == 1:
        assert secret_manager.get_crdb_ca_cert() == secret_string
    else:
        with pytest.raises(Exception, match=error_message):
            secret_manager.get_crdb_ca_cert()