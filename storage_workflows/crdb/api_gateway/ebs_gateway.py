from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from storage_workflows.crdb.aws.ebs_volume import EBSVolume


class EBSGateway:
    @staticmethod
    def get_ebs_volumes_for_instance(instance_id):
        ec2 = AwsSessionFactory.ec2()

        # Describe volumes attached to the instance. We'll exclude 8GB root volumes.
        volumes = ec2.describe_volumes(
            Filters=[
                {
                    'Name': 'attachment.instance-id',
                    'Values': [instance_id]
                },
                {
                    'Name': 'size',
                    'Values': ['8'],
                    'Comparison': 'NotEquals'
                },
                {
                    'Name': 'volume-type',
                    'Values': ['gp2', 'io1', 'io2', 'st1', 'sc1']
                }
            ]
        )['Volumes']

        ebs_objects = []
        for volume in volumes:
            ebs_objects.append(EBSVolume.from_aws_response(volume))

        return ebs_objects
