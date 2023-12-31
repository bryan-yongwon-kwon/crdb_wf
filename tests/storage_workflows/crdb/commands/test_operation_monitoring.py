import os
from storage_workflows.crdb.commands.operation_monitoring import check_avg_cpu, reduce_rebalance_rate
from storage_workflows.crdb.models.cluster_setting import ClusterSetting
from storage_workflows.slack.slack_notification import SlackNotification
from unittest import TestCase
from unittest.mock import patch, DEFAULT, MagicMock

class TestOperationMonitoring(TestCase):
    
    @patch.multiple('storage_workflows.crdb.commands.operation_monitoring',
                    setup_env=DEFAULT,
                    logger=DEFAULT)
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    get_cluster_setting=DEFAULT)
    @patch.multiple('storage_workflows.slack.slack_notification.SlackNotification',
                    config_notification=DEFAULT,
                    send_notification=DEFAULT)
    def test_reduce_rebalance_rate_rates_not_match(self,
                                                   setup_env,
                                                   logger,
                                                   get_cluster_setting,
                                                   config_notification,
                                                   send_notification):
        SNAPSHOT_REBALANCE_RATE = 'kv.snapshot_rebalance.max_rate'
        SNAPSHOT_RECOVERY_RATE = 'kv.snapshot_recovery.max_rate'
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        test_namespace = 'test_namespace'
        test_workflow_id = 'test_workflow_id'
        is_test = False
        os.environ['CLUSTER_NAME'] = test_cluster_name
        test_rebalance_rate = ClusterSetting([SNAPSHOT_REBALANCE_RATE, '64 MiB', 'z', 'Maximum rate at which the rebalance process will copy data from one node to another.'], test_cluster_name)
        test_recovery_rate = ClusterSetting([SNAPSHOT_RECOVERY_RATE, '32 MiB', 'z', 'Maximum rate at which the recovery process will copy data from one node to another.'], test_cluster_name)
        return_values = {SNAPSHOT_REBALANCE_RATE: test_rebalance_rate, SNAPSHOT_RECOVERY_RATE: test_recovery_rate}
        get_cluster_setting.side_effect = return_values.get
        test_notification = SlackNotification('test_webhook_url')
        config_notification.return_value = test_notification
        reduce_rebalance_rate('prod', 'us-west-2', test_cluster_name, test_namespace, test_workflow_id, is_test)
        config_notification.assert_called_with('prod', False)
        send_notification.assert_called_once()
        self.assertEqual(logger.error.call_count, 1)
        logger.error.assert_called_with('Rebalance rate and recovery rate are not equal. Please check.')
        self.assertEqual(logger.info.call_count, 3)
        logger.info.assert_called_with('Skip reducing rebalance rate.')


    @patch.multiple('storage_workflows.crdb.commands.operation_monitoring',
                    setup_env=DEFAULT,
                    logger=DEFAULT)
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    get_cluster_setting=DEFAULT,
                    update_cluster_setting=DEFAULT)
    def test_redeuce_rebalance_rate_success(self,
                                            setup_env,
                                            logger,
                                            get_cluster_setting,
                                            update_cluster_setting):
        SNAPSHOT_REBALANCE_RATE = 'kv.snapshot_rebalance.max_rate'
        SNAPSHOT_RECOVERY_RATE = 'kv.snapshot_recovery.max_rate'
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        test_namespace = 'test_namespace'
        test_workflow_id = 'test_workflow_id'
        is_test = False
        os.environ['CLUSTER_NAME'] = test_cluster_name
        test_rebalance_rate = ClusterSetting([SNAPSHOT_REBALANCE_RATE, '64 MiB', 'z', 'Maximum rate at which the rebalance process will copy data from one node to another.'], test_cluster_name)
        test_recovery_rate = ClusterSetting([SNAPSHOT_RECOVERY_RATE, '64 MiB', 'z', 'Maximum rate at which the recovery process will copy data from one node to another.'], test_cluster_name)
        return_values = {SNAPSHOT_REBALANCE_RATE: test_rebalance_rate, SNAPSHOT_RECOVERY_RATE: test_recovery_rate}
        get_cluster_setting.side_effect = return_values.get
        reduce_rebalance_rate('prod', 'us-west-2', test_cluster_name, test_namespace, test_workflow_id, is_test)
        update_cluster_setting.assert_any_call(SNAPSHOT_REBALANCE_RATE, '32 MiB')
        update_cluster_setting.assert_any_call(SNAPSHOT_RECOVERY_RATE, '32 MiB')
        self.assertEqual(logger.info.call_count, 6)
        logger.info.assert_called_with('Reducing rebalance rate completed.')


    @patch.multiple('storage_workflows.crdb.commands.operation_monitoring',
                    setup_env=DEFAULT,
                    logger=DEFAULT)
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    is_avg_cpu_exceed_threshold=DEFAULT)
    @patch.multiple('storage_workflows.slack.slack_notification.SlackNotification',
                    config_notification=DEFAULT)
    def test_check_avg_cpu_below_threshold(self,
                                           setup_env,
                                           logger,
                                           is_avg_cpu_exceed_threshold,
                                           config_notification,):
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        test_namespace = 'test_namespace'
        test_workflow_id = 'test_workflow_id'
        is_test = False
        os.environ['CLUSTER_NAME'] = test_cluster_name
        is_avg_cpu_exceed_threshold.return_value = False
        test_notification = SlackNotification('test_webhook_url')
        config_notification.return_value = test_notification
        check_avg_cpu('prod', 'us-west-2', test_cluster_name, test_namespace, test_workflow_id, is_test)
        config_notification.assert_called_with('prod', False)
        self.assertEqual(logger.info.call_count, 1)
        logger.info.assert_called_with('Average CPU usage is below threshold. No action needed.')


    @patch.multiple('storage_workflows.crdb.commands.operation_monitoring',
                    setup_env=DEFAULT,
                    logger=DEFAULT)
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    get_cluster_setting=DEFAULT,
                    is_avg_cpu_exceed_threshold=DEFAULT)
    @patch.multiple('storage_workflows.slack.slack_notification.SlackNotification',
                    config_notification=DEFAULT,
                    send_notification=DEFAULT)
    def test_check_avg_cpu_above_threshold_rate_at_lower_threshold(self,
                                                                   setup_env,
                                                                   logger,
                                                                   get_cluster_setting,
                                                                   is_avg_cpu_exceed_threshold,
                                                                   config_notification,
                                                                   send_notification):
        SNAPSHOT_REBALANCE_RATE = 'kv.snapshot_rebalance.max_rate'
        SNAPSHOT_RECOVERY_RATE = 'kv.snapshot_recovery.max_rate'
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        test_namespace = 'test_namespace'
        test_workflow_id = 'test_workflow_id'
        is_test = False
        os.environ['CLUSTER_NAME'] = test_cluster_name
        test_rebalance_rate = ClusterSetting([SNAPSHOT_REBALANCE_RATE, '1 MiB', 'z', 'Maximum rate at which the rebalance process will copy data from one node to another.'], test_cluster_name)
        test_recovery_rate = ClusterSetting([SNAPSHOT_RECOVERY_RATE, '1 MiB', 'z', 'Maximum rate at which the recovery process will copy data from one node to another.'], test_cluster_name)
        return_values = {SNAPSHOT_REBALANCE_RATE: test_rebalance_rate, SNAPSHOT_RECOVERY_RATE: test_recovery_rate}
        get_cluster_setting.side_effect = return_values.get
        test_notification = SlackNotification('test_webhook_url')
        config_notification.return_value = test_notification
        is_avg_cpu_exceed_threshold.return_value = True
        check_avg_cpu('prod', 'us-west-2', test_cluster_name, test_namespace, test_workflow_id, is_test)
        config_notification.assert_called_with('prod', False)
        self.assertEqual(logger.info.call_count, 3)
        logger.info.assert_called_with('Average CPU usage is still above threshold. Sending Slack notification.')
        send_notification.assert_called_once()
