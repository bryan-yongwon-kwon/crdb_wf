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
            if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
                logger.error("Instance does not exist or is not in a valid state.")
                return None
            else:
                logger.error(f"error: {e}")
                # Handle other possible exceptions or re-raise
                logger.error("Unhandled client exception occurred while registering new instance(s) with etl")
                return None

    
    @staticmethod
    def deregister_instances_from_load_balancer(load_balancer_name, instances):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        response = elastic_load_balancer_client.deregister_instances_from_load_balancer(LoadBalancerName=load_balancer_name, 
                                                                                        Instances=instances)
        return response['Instances']