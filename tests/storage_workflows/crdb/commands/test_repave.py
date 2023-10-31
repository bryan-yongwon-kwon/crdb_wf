import os
from parameterized import parameterized
from storage_workflows.crdb.commands.repave import pre_check
from unittest import TestCase
from unittest.mock import patch, DEFAULT


class TestRepave(TestCase):

    @patch.multiple('storage_workflows.crdb.commands.repave',
                    setup_env=DEFAULT,
                    logger=DEFAULT)
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    backup_job_is_running=DEFAULT,
                    restore_job_is_running=DEFAULT,
                    schema_change_job_is_running=DEFAULT,
                    row_level_ttl_job_is_running=DEFAULT,
                    instances_not_in_service_exist=DEFAULT,
                    paused_changefeed_jobs_exist=DEFAULT,
                    unhealthy_ranges_exist=DEFAULT)
    def test_pre_check_all_pass(self, 
                                setup_env, 
                                logger,
                                backup_job_is_running,
                                restore_job_is_running,
                                schema_change_job_is_running,
                                row_level_ttl_job_is_running,
                                instances_not_in_service_exist,
                                paused_changefeed_jobs_exist,
                                unhealthy_ranges_exist):
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        os.environ['CLUSTER_NAME'] = test_cluster_name
        backup_job_is_running.return_value = False
        restore_job_is_running.return_value = False
        schema_change_job_is_running.return_value = False
        row_level_ttl_job_is_running.return_value = False
        instances_not_in_service_exist.return_value = False
        paused_changefeed_jobs_exist.return_value = False
        unhealthy_ranges_exist.return_value = False
        pre_check('prod', 'us-west-2', test_cluster_name)
        backup_job_is_running.assert_called_once_with()
        restore_job_is_running.assert_called_once_with()
        schema_change_job_is_running.assert_called_once_with()
        row_level_ttl_job_is_running.assert_called_once_with()
        instances_not_in_service_exist.assert_called_once_with()
        paused_changefeed_jobs_exist.assert_called_once_with()
        unhealthy_ranges_exist.assert_called_once_with()
        logger.info.assert_called_once_with('{} Check passed'.format(test_cluster_name))
    
    @parameterized.expand(["backup_job_is_running", 
                           "restore_job_is_running", 
                           "schema_change_job_is_running", 
                           "row_level_ttl_job_is_running", 
                           "instances_not_in_service_exist", 
                           "paused_changefeed_jobs_exist", 
                           "unhealthy_ranges_exist"])
    @patch('storage_workflows.crdb.commands.repave.setup_env')
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    backup_job_is_running=DEFAULT,
                    restore_job_is_running=DEFAULT,
                    schema_change_job_is_running=DEFAULT,
                    row_level_ttl_job_is_running=DEFAULT,
                    instances_not_in_service_exist=DEFAULT,
                    paused_changefeed_jobs_exist=DEFAULT,
                    unhealthy_ranges_exist=DEFAULT)
    def test_pre_check_failure_cases(self,
                                     check_type,
                                     setup_env,
                                     backup_job_is_running,
                                     restore_job_is_running,
                                     schema_change_job_is_running,
                                     row_level_ttl_job_is_running,
                                     instances_not_in_service_exist,
                                     paused_changefeed_jobs_exist,
                                     unhealthy_ranges_exist):
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        os.environ['CLUSTER_NAME'] = test_cluster_name
        backup_job_is_running.return_value = True if check_type == 'backup_job_is_running' else False
        restore_job_is_running.return_value = True if check_type == 'restore_job_is_running' else False
        schema_change_job_is_running.return_value = True if check_type == 'schema_change_job_is_running' else False
        row_level_ttl_job_is_running.return_value = True if check_type == 'row_level_ttl_job_is_running' else False
        instances_not_in_service_exist.return_value = True if check_type == 'instances_not_in_service_exist' else False
        paused_changefeed_jobs_exist.return_value = True if check_type == 'paused_changefeed_jobs_exist' else False
        unhealthy_ranges_exist.return_value = True if check_type == 'unhealthy_ranges_exist' else False
        with self.assertRaises(Exception) as context:
            pre_check('prod', 'us-west-2', test_cluster_name)
        self.assertEqual(str(context.exception), 'Pre run check failed')
        