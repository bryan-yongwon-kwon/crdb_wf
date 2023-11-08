import os
from storage_workflows.crdb.commands.operation_monitoring import reduce_rebalance_rate
from storage_workflows.crdb.models.cluster_setting import ClusterSetting
from unittest import TestCase
from unittest.mock import patch, DEFAULT

class TestOperationMonitoring(TestCase):
    
    @patch.multiple('storage_workflows.crdb.commands.operation_monitoring',
                    setup_env=DEFAULT,
                    logger=DEFAULT)
    @patch.multiple('storage_workflows.crdb.models.cluster.Cluster',
                    get_cluster_setting=DEFAULT)
    def test_reduce_rebalance_rate_rates_not_match(self,
                                                   setup_env,
                                                   logger,
                                                   get_cluster_setting):
        SNAPSHOT_REBALANCE_RATE = 'kv.snapshot_rebalance.max_rate'
        SNAPSHOT_RECOVERY_RATE = 'kv.snapshot_recovery.max_rate'
        setup_env.return_value = None
        test_cluster_name = 'test_cluster'
        os.environ['CLUSTER_NAME'] = test_cluster_name
        test_rebalance_rate = ClusterSetting([SNAPSHOT_REBALANCE_RATE, '64 MiB', 'z', 'Maximum rate at which the rebalance process will copy data from one node to another.'], test_cluster_name)
        test_recovery_rate = ClusterSetting([SNAPSHOT_RECOVERY_RATE, '32 MiB', 'z', 'Maximum rate at which the recovery process will copy data from one node to another.'], test_cluster_name)
        return_values = {SNAPSHOT_REBALANCE_RATE: test_rebalance_rate, SNAPSHOT_RECOVERY_RATE: test_recovery_rate}
        get_cluster_setting.side_effect = return_values.get
        reduce_rebalance_rate('prod', 'us-west-2', test_cluster_name)
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
        os.environ['CLUSTER_NAME'] = test_cluster_name
        test_rebalance_rate = ClusterSetting([SNAPSHOT_REBALANCE_RATE, '64 MiB', 'z', 'Maximum rate at which the rebalance process will copy data from one node to another.'], test_cluster_name)
        test_recovery_rate = ClusterSetting([SNAPSHOT_RECOVERY_RATE, '64 MiB', 'z', 'Maximum rate at which the recovery process will copy data from one node to another.'], test_cluster_name)
        return_values = {SNAPSHOT_REBALANCE_RATE: test_rebalance_rate, SNAPSHOT_RECOVERY_RATE: test_recovery_rate}
        get_cluster_setting.side_effect = return_values.get
        reduce_rebalance_rate('prod', 'us-west-2', test_cluster_name)
        update_cluster_setting.assert_any_call(SNAPSHOT_REBALANCE_RATE, '32 MiB')
        update_cluster_setting.assert_any_call(SNAPSHOT_RECOVERY_RATE, '32 MiB')
        self.assertEqual(logger.info.call_count, 6)
        logger.info.assert_called_with('Reducing rebalance rate completed.')