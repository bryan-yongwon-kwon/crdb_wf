from storage_workflows.crdb.factory.aws_session_factory import AwsSessionFactory
from storage_workflows.logging.logger import Logger
from botocore.exceptions import ClientError, WaiterError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = Logger()


class ElasticLoadBalancerGateway:
    PAGINATOR_MAX_RESULT_PER_PAGE = 100

    @staticmethod
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(ClientError),
        reraise=True
    )
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
            next_page = ElasticLoadBalancerGateway.describe_load_balancers(LoadBalancerNames=names,
                                                                           Marker=response['NextMarker'],
                                                                           PageSize=ElasticLoadBalancerGateway.PAGINATOR_MAX_RESULT_PER_PAGE)
            return response['LoadBalancerDescriptions'] + next_page

        return response['LoadBalancerDescriptions']

    @staticmethod
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(ClientError),
        reraise=True
    )
    def register_instances_with_load_balancer(load_balancer_name, instances):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        response = elastic_load_balancer_client.register_instances_with_load_balancer(
            LoadBalancerName=load_balancer_name,
            Instances=instances)
        return response['Instances']

    @staticmethod
    def deregister_instances_from_load_balancer(load_balancer_name, instances):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        response = elastic_load_balancer_client.deregister_instances_from_load_balancer(LoadBalancerName=load_balancer_name,
                                                                                        Instances=instances)
        return response['Instances']

    @staticmethod
    def get_out_of_service_instances(load_balancer_name: str):
        elastic_load_balancer_client = AwsSessionFactory.elb()
        instances_out_of_service = []

        # Get the description of the instances registered with the ELB
        response = elastic_load_balancer_client.describe_instance_health(
            LoadBalancerName=load_balancer_name
        )

        for instance in response['InstanceStates']:
            if instance['State'] == 'OutOfService':
                instances_out_of_service.append(instance['InstanceId'])

        return instances_out_of_service
