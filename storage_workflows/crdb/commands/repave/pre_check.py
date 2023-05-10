import typer
from storage_workflows.crdb.operations.workflow_pre_run_check import WorkflowPreRunCheck
from storage_workflows.crdb.aws.elastic_load_balancer import ElasticLoadBalancer
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.api_gateway.elastic_load_balancer_gateway import ElasticLoadBalancerGateway
from storage_workflows.setup_env import setup_env

app = typer.Typer()

@app.command()
def pre_check(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    if (WorkflowPreRunCheck.backup_job_is_running(cluster_name)
        or WorkflowPreRunCheck.restore_job_is_running(cluster_name)
        or WorkflowPreRunCheck.schema_change_job_is_running(cluster_name)
        or WorkflowPreRunCheck.row_level_ttl_job_is_running(cluster_name)
        or WorkflowPreRunCheck.unhealthy_ranges_exist(cluster_name)
        or WorkflowPreRunCheck.instances_not_in_service_exist(cluster_name)):
        raise Exception("Pre run check failed")
    else:
        print("Check passed")

@app.command()
def refresh_etl_load_balancer(deployment_env, region, cluster_name):
    setup_env(deployment_env, region, cluster_name)
    etl_load_balancer_name = (cluster_name.replace("_", "-") + "-crdb-etl")[:32]
    load_balancer = ElasticLoadBalancer.find_elastic_load_balancers([etl_load_balancer_name])[0]
    old_instances = load_balancer.instances
    new_instances = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(cluster_name).instances
    new_instances = list(map(lambda instance: {'InstanceId': instance.instance_id}, new_instances))
    ElasticLoadBalancerGateway.register_instances_with_load_balancer(etl_load_balancer_name, new_instances)
    ElasticLoadBalancerGateway.deregister_instances_from_load_balancer(etl_load_balancer_name, old_instances)


if __name__ == "__main__":
    app()