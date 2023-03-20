from storage_workflows.crdb.aws.aws_base import AwsBase
from storage_workflows.crdb.aws.account_type import AccountType

class AutoScalingGroup(AwsBase):
    _SERVICE_NAME = "autoscaling"

    def __init__(self, account_type:AccountType, cluster_name:str, region:str):
        self._account_type = account_type
        self._cluster_name = cluster_name
        self._region = region
        self._aws_client = AwsBase.get_aws_client(account_type, self._SERVICE_NAME, region)

    def _get_asg(self) -> dict:
        filters = [
            {
                'Name': 'tag:crdb_cluster_name',
                'Values': [
                    self._cluster_name+"_"+self._account_type.value,
                ]
            }
        ]
        asg_list = self._aws_client.describe_auto_scaling_groups(
            Filters=filters, 
            MaxRecords=AwsBase.PAGINATOR_MAX_RESULT_PER_PAGE)['AutoScalingGroups']
        result_count = len(asg_list)
        assert result_count == 1, "Should get exact 1 AutoScaling Group. Got {} now.".format(result_count)
        return asg_list[0] 
    
    def suspicious_instances_exist(self) -> bool:
        instance_list = self._get_asg()['Instances']
        contain_suspicious_instances = False
        for instance in instance_list:
            if instance['LifecycleState'] != 'InService' or instance['HealthStatus'] != 'Healthy':
                contain_suspicious_instances = True
                break
        return contain_suspicious_instances