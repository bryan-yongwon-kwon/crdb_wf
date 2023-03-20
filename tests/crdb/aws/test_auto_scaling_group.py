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
                'Instances':[]
            }
        )
    return asgs

@pytest.mark.parametrize(
    ["lifecycle_state", "health_status", "result"],
    [
        ('StandBy', 'Healthy', True),
        ('InService', 'Unhealthy', True),
        ('InService', 'Healthy', False),
    ],
    ids=["standby_exist", "unhealthy_instance_exist", "all_good"],
)
def test_suspicious_instances_exist(mocker, lifecycle_state, health_status, result):
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
    def mock_get_asg(self):
        return asg
    auto_scaling_group = AutoScalingGroup(AccountType.STAGING, "test_cluster", "us-west-2")
    mocker.patch('storage_workflows.crdb.aws.auto_scaling_group.AutoScalingGroup._get_asg', mock_get_asg)
    assert auto_scaling_group.suspicious_instances_exist() == result

@pytest.mark.parametrize(
    ["asgs", "count"],
    [
        (make_asgs(0), 0),
        (make_asgs(1), 1),
        (make_asgs(2), 2),
    ]
)
def test_get_asg(mocker, asgs, count):
    client = boto3.client('autoscaling', 'us-west-2')
    def mock_get_client(account_type: AccountType, service_name:str, region:str):
        return client
    mocker.patch('storage_workflows.crdb.aws.aws_base.AwsBase.get_aws_client', mock_get_client)
    stubber = Stubber(client)
    describe_auto_scaling_groups_expected_params = {
        'Filters': ANY,
        'MaxRecords': AwsBase.PAGINATOR_MAX_RESULT_PER_PAGE,
    }
    stubber.add_response('describe_auto_scaling_groups', asgs, describe_auto_scaling_groups_expected_params)
    stubber.activate()
    error_message = "Should get exact 1 AutoScaling Group. Got {} now.".format(count)
    auto_scaling_group = AutoScalingGroup(AccountType.STAGING, "test_cluster", "us-west-2")
    if count == 1:
        auto_scaling_group.suspicious_instances_exist()
    else:
        with pytest.raises(Exception, match=error_message):
            auto_scaling_group.suspicious_instances_exist()
