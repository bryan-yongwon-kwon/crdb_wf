import os
import subprocess
import time
from typing import Any
from functools import reduce
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from storage_workflows.crdb.connect.crdb_connection import CrdbConnection
from storage_workflows.crdb.aws.auto_scaling_group import AutoScalingGroup
from storage_workflows.crdb.aws.ec2_instance import Ec2Instance
from storage_workflows.crdb.metadata_db.metadata_db_operations import MetadataDBOperations
from storage_workflows.crdb.models.jobs.backup_job import BackupJob
from storage_workflows.crdb.models.jobs.changefeed_job import ChangefeedJob
from storage_workflows.crdb.models.jobs.restore_job import RestorelJob
from storage_workflows.crdb.models.jobs.row_level_ttl_job import RowLevelTtlJob
from storage_workflows.crdb.models.jobs.schema_change_job import SchemaChangelJob
from storage_workflows.crdb.models.node import Node
from storage_workflows.logging.logger import Logger


logger = Logger()
class Cluster:

    def __init__(self):
        self.cluster_name = os.getenv('CLUSTER_NAME')

    @property
    def deployment_env(self):
        return os.getenv('DEPLOYMENT_ENV')

    @property
    def region(self):
        return os.getenv('REGION')

    @property
    def nodes(self):
        return Node.get_nodes()
    
    @property
    def changefeed_jobs(self) -> list[ChangefeedJob]:
        logger.info("retrieving changefeed_jobs")
        return ChangefeedJob.find_all_changefeed_jobs(self.cluster_name)
    
    def is_avg_cpu_exceed_threshold(self, threshold:float, offest_mins) -> bool:
        query = 'min_over_time(avg(sys_cpu_combined_percent_normalized{{job="crdb", cluster="{}_{}", region="{}"}})[{}:10s]) > bool {}'.format(self.cluster_name, 
                                                                                                                                             self.deployment_env,
                                                                                                                                             self.region, 
                                                                                                                                             offest_mins, 
                                                                                                                                             threshold)
        return ChronosphereApiGateway.query_promql_instant(query)['data']['result'][0]['value'][1] == '1'
    
    def backup_job_is_running(self) -> bool:
        logger.info("checking for running backups")
        running_backup_jobs = list(filter(lambda job: job.status == 'running',
                                          BackupJob.find_all_backup_jobs(self.cluster_name)))
        contains_running_backup_job = any(running_backup_jobs)
        if contains_running_backup_job:
            logger.warning("Running backup job(s) found!")
        return contains_running_backup_job
    
    def restore_job_is_running(self) -> bool:
        logger.info("checking for restore running restore job")
        running_restore_jobs = list(filter(lambda job: job.status == 'running',
                                           RestorelJob.find_all_restore_jobs(self.cluster_name)))
        contains_running_restore_job = any(running_restore_jobs)
        if contains_running_restore_job:
            logger.warning("Running restore job(s) found!")
        return contains_running_restore_job
    
    def schema_change_job_is_running(self) -> bool:
        logger.info("checking for running schema change job")
        running_schema_change_jobs = list(filter(lambda job: job.status == 'running',
                                                 SchemaChangelJob.find_all_schema_change_jobs(self.cluster_name)))
        contains_schema_change_job = any(running_schema_change_jobs)
        if contains_schema_change_job:
            logger.warning("Running schema change job found!")
        return contains_schema_change_job
    
    def row_level_ttl_job_is_running(self) -> bool:
        logger.info("checking for running ttl job")
        running_row_level_ttl_jobs = list(filter(lambda job: job.status == 'running',
                                                 RowLevelTtlJob.find_all_row_level_ttl_jobs(self.cluster_name)))
        contains_row_level_ttl_job = any(running_row_level_ttl_jobs)
        if contains_row_level_ttl_job:
            logger.warning("Running row level ttl job(s) found!")
        return contains_row_level_ttl_job
    
    def paused_changefeed_jobs_exist(self) -> bool:
        logger.info("checking for paused changefeed jobs")
        paused_changefeed_jobs = list(filter(lambda job: job.status == 'paused',
                                             ChangefeedJob.find_all_changefeed_jobs(self.cluster_name)))
        contains_paused_changefeed_jobs = any(paused_changefeed_jobs)
        if contains_paused_changefeed_jobs:
            logger.warning("Paused changefeed job(s) found!")
        return contains_paused_changefeed_jobs
    
    def unhealthy_ranges_exist(self) -> bool:
        logger.info("checking for unhealthy ranges")
        nodes = self.nodes
        unhealthy_ranges_list = map(lambda node: node.overreplicated_ranges+node.unavailable_ranges+node.underreplicated_ranges, nodes)
        total_unhealthy_ranges = reduce(lambda range_count_1, range_count_2: range_count_1+range_count_2, unhealthy_ranges_list)
        return total_unhealthy_ranges > 0
    
    def instances_not_in_service_exist(self) -> bool:
        logger.info(f"Checking for NotInService instances in ASG associated with cluster: {self.cluster_name}")
        try:
            asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(self.cluster_name)

            if not asg:
                logger.error(f"No ASG found for cluster name: {self.cluster_name}")
                return False

            not_in_service_instances = [instance for instance in asg.instances if not instance.in_service()]

            if not_in_service_instances:
                logger.warning(
                    f"Instances not in service for cluster {self.cluster_name}: "
                    f"{[instance.instance_id for instance in not_in_service_instances]}")
                return True

            return False

        except Exception as e:
            logger.error(
                f"Error while checking for NotInService instances in ASG for cluster {self.cluster_name}: {str(e)}")
            raise e
    
    def decommission_nodes(self, nodes:list[Node]):
        certs_dir = os.getenv('CRDB_CERTS_DIR_PATH_PREFIX') + "/" + self.cluster_name + "/"
        CrdbConnection.get_crdb_connection(self.cluster_name)
        formatted_cluster_name = "{}-{}".format(self.cluster_name.replace('_', '-'), os.getenv('DEPLOYMENT_ENV'))
        major_version_dict = dict()
        for node in nodes:
            major_version = node.major_version
            node_id = str(node.id)
            if major_version in major_version_dict:
                major_version_dict[major_version].append(node_id)
            else:
                major_version_dict[major_version] = [node_id]
        for major_version in major_version_dict:
            nodes_str = ' '.join(major_version_dict[major_version])
            node_decommission_command = "crdb{} node decommission {} --host={}:26256 --certs-dir={} --cluster-name={}".format(major_version,
                                                                                                                              nodes_str, 
                                                                                                                              nodes[-1].ip_address,
                                                                                                                              certs_dir,
                                                                                                                              formatted_cluster_name)
            logger.info("Decommissioning nodes with major version {}...".format(major_version))
            result = subprocess.run(node_decommission_command, capture_output=True, shell=True)
            logger.error(result.stderr.decode('ascii'))
            result.check_returncode()
            logger.info(result.stdout.decode('ascii'))
            logger.info("Completed decommissioning nodes with major version {}.".format(major_version))

    @staticmethod
    def get_nodes_from_asg_instances(asg_instances):
        instance_ids = []
        for instance in asg_instances:
            instance_ids.append(instance.instance_id)
        nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, instance_ids))
        return nodes

    def wait_for_hydration(self, timeout_mins:int):
        def is_node_hydrated(old_applied_initial_snapshots, new_applied_initial_snapshots):
            return (new_applied_initial_snapshots - old_applied_initial_snapshots)/60 < 0.75
        def refresh_snapshots_dict(nodes:list[Node], applied_initial_snapshots_dict:dict):
            for node in nodes:
                applied_initial_snapshots_dict[node.id] = node.applied_initial_snapshots
        asg = AutoScalingGroup.find_auto_scaling_group_by_cluster_name(self.cluster_name)
        metadata_db_operations = MetadataDBOperations()
        old_instance_ids = metadata_db_operations.get_old_instance_ids(self.cluster_name, os.getenv('DEPLOYMENT_ENV'))
        new_instances = list(filter(lambda instance: instance.instance_id not in old_instance_ids, asg.instances))
        new_nodes = list(map(lambda instance: Ec2Instance.find_ec2_instance(instance.instance_id).crdb_node, new_instances))
        applied_initial_snapshots_dict = {}
        for node in new_nodes:
            applied_initial_snapshots_dict[node.id] = 0
        logger.info("Checking nodes for hydration with {} mins timeout.".format(timeout_mins))
        is_snapshots_change_rate_below_threshold_for_last_check = False
        for minute in range(timeout_mins):
            nodes_pending_hydration = list(filter(lambda node: not is_node_hydrated(applied_initial_snapshots_dict[node.id], node.applied_initial_snapshots), new_nodes))
            if not nodes_pending_hydration:
                if is_snapshots_change_rate_below_threshold_for_last_check:
                    logger.info("Hydration complete.")
                    return
                else:
                    is_snapshots_change_rate_below_threshold_for_last_check = True
                    refresh_snapshots_dict(new_nodes, applied_initial_snapshots_dict)
                    time.sleep(60)
                    continue
            node_ids = list(map(lambda node: node.id, nodes_pending_hydration))
            logger.info("Following nodes not hydrated: {}.".format(node_ids))
            refresh_snapshots_dict(new_nodes, applied_initial_snapshots_dict)
            is_snapshots_change_rate_below_threshold_for_last_check = False
            time.sleep(60)
        logger.info("Hydration timeout!")
    
    def wait_for_connections_drain_on_old_nodes(self, timeout_mins:int):
        metadata_db_operations = MetadataDBOperations()
        old_instance_ids = metadata_db_operations.get_old_instance_ids(self.cluster_name, os.getenv('DEPLOYMENT_ENV'))
        old_nodes = list(map(lambda instance_id: Ec2Instance.find_ec2_instance(instance_id).crdb_node, old_instance_ids))
        logger.info("Waiting for connections drain...")
        for count in range(timeout_mins):
            logger.info("Checking for connections...")
            for node in old_nodes:
                node.reload()
            nodes_not_drained = list(filter(lambda node: node.sql_conns > 1, old_nodes))
            if nodes_not_drained:
                ids = list(map(lambda node: node.id, nodes_not_drained))
                logger.info("Waiting for connections on following nodes to drain: {}".format(ids))
                for node in nodes_not_drained:
                    logger.info("Node {} still has {} active SQL connections.".format(node.id, node.sql_conns))
                logger.info("Sleep for 1 mins...")
                logger.info("{} mins left till timeout.".format(timeout_mins-count))
                time.sleep(60)
            else:
                logger.info("All the connections on old nodes are disconnected.")
                return
        logger.info("Timeout, proceed with decommission.")
