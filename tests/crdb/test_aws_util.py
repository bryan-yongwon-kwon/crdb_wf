import pytest
import boto3
import uuid
from botocore.stub import Stubber, ANY
from datetime import datetime
from storage_workflows.crdb.aws_util import suspicious_instances_exist, get_secret, get_asg
from storage_workflows.crdb.aws_util import MAX_RESULT
from storage_workflows.crdb.common_enums import Env
from storage_workflows.crdb.common_enums import CredType

@pytest.mark.parametrize(
    ["lifecycle_state", "health_status", "result"],
    [
        ('StandBy', 'Healthy', True),
        ('InService', 'Unhealthy', True),
        ('InService', 'Healthy', False),
    ],
    ids=["standby_exist", "unhealthy_instance_exist", "all_good"],
)
def test_suspicious_instances_exist(lifecycle_state, health_status, result):
    asg = {
        'Instances': [
            {
                'LifecycleState': 'InService',
                'HealthStatus': 'Healthy'
            },
            {
                'LifecycleState': lifecycle_state,
                'HealthStatus': health_status
            }
        ]
    }
    assert suspicious_instances_exist(asg) == result

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
def test_get_secret(secrets, count):
    client = boto3.client('secretsmanager', 'us-west-2')
    stubber = Stubber(client)
    list_secrets_expected_params = {
        'IncludePlannedDeletion': False,
        'MaxResults': MAX_RESULT,
        'Filters': ANY
    }
    get_secret_value_expected_params = {
        'SecretId': ANY
    }
    mock_content = {
        'SecretString': 'secret'
    }
    stubber.add_response('list_secrets', secrets, list_secrets_expected_params)
    stubber.add_response('get_secret_value', mock_content, get_secret_value_expected_params)
    stubber.activate()
    error_message = "Should get exact 1 {} for {}. Got {} now.".format(CredType.CA_CERT_CRED_TYPE.value, 'crdb_cluster', count)
    with stubber:
        if count == 1:
            get_secret(client, 'crdb_cluster', Env.STAGING, CredType.CA_CERT_CRED_TYPE)
        else:
            with pytest.raises(Exception, match=error_message):
                get_secret(client, 'crdb_cluster', Env.STAGING, CredType.CA_CERT_CRED_TYPE)

def make_asgs(count:int) -> dict:
    asgs = {
        'AutoScalingGroups': []
    }
    for asg_index in range(0,count):
        asgs['AutoScalingGroups'].append(
            {
                'AutoScalingGroupName': uuid.uuid4().hex,
                'MinSize': 5,
                'MaxSize': 10,
                'DesiredCapacity': 8,
                'DefaultCooldown': 0,
                'AvailabilityZones': ['az'],
                'HealthCheckType': 'test',
                'CreatedTime': datetime.now(),
            }
        )
    return asgs

@pytest.mark.parametrize(
    ["asgs", "count"],
    [
        (make_asgs(0), 0),
        (make_asgs(1), 1),
        (make_asgs(2), 2),
    ]
)
def test_get_asg(asgs, count):
    client = boto3.client('autoscaling', 'us-west-2')
    stubber = Stubber(client)
    describe_auto_scaling_groups_expected_params = {
        'Filters': ANY,
        'MaxRecords': MAX_RESULT,
    }
    stubber.add_response('describe_auto_scaling_groups', asgs, describe_auto_scaling_groups_expected_params)
    stubber.activate()
    error_message = "Should get exact 1 AutoScaling Group. Got {} now.".format(count)
    with stubber:
        if count == 1:
            get_asg(client, 'crdb_cluster', Env.STAGING)
        else:
            with pytest.raises(Exception, match=error_message):
                get_asg(client, 'crdb_cluster', Env.STAGING)

