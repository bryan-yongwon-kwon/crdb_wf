from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from botocore.exceptions import ClientError
from storage_workflows.logging.logger import Logger

logger = Logger()


class ElasticLoadBalancerGateway:

    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    def describe_load_balancers(names:list, marker=''):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        if not marker:
            response = elastic_load_balancer_client.describe_load_balancers(LoadBalancerNames=names, 
                                                                            PageSize=ElasticLoadBalancerGateway.PAGINATOR_MAX_RESULT_PER_PAGE)
        else:
            response = elastic_load_balancer_client.describe_load_balancers(LoadBalancerNames=names, 
                                                                            Marker=marker, 
                                                                            PageSize=ElasticLoadBalancerGateway.PAGINATOR_MAX_RESULT_PER_PAGE)
        if 'NextMarker' in response:
            return response['LoadBalancerDescriptions'].extend(
                ElasticLoadBalancerGateway.describe_load_balancers(LoadBalancerNames=names,
                                                                   Marker=response['NextMarker'],
                                                                   PageSize=ElasticLoadBalancerGateway.PAGINATOR_MAX_RESULT_PER_PAGE))
        return response['LoadBalancerDescriptions']

    @staticmethod
    def register_instances_with_load_balancer(load_balancer_name, instances):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        try:
            response = elastic_load_balancer_client.register_instances_with_load_balancer(LoadBalancerName=load_balancer_name,
                                                                                      Instances=instances)
            return response['Instances']
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidInstance':
                # Extract instance ID from the error message
                invalid_id = str(instances).split("'")[1]
                print(f"Instance {invalid_id} does not exist. Removing from the list.")
                # Remove the invalid instance from the list
                new_instances = [instance for instance in instances if instance['InstanceId'] != invalid_id]
                logger.error(f"new_instances: {new_instances}")
                new_response = elastic_load_balancer_client.register_instances_with_load_balancer(
                    LoadBalancerName=load_balancer_name,
                    Instances=new_instances)
                return new_response
            else:
                logger.error(f"e: {e}")
                # Handle other possible exceptions or re-raise
                logger.error("Unhandled client exception occurred while registering new instance(s) with etl")
                return None

    
    @staticmethod
    def deregister_instances_from_load_balancer(load_balancer_name, instances):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        response = elastic_load_balancer_client.deregister_instances_from_load_balancer(LoadBalancerName=load_balancer_name, 
                                                                                        Instances=instances)
        return response['Instances']