import os
from parameterized import parameterized
from storage_workflows.chronosphere.chronosphere_api_gateway import ChronosphereApiGateway
from unittest import TestCase
from unittest.mock import patch, DEFAULT

class TestChronosphereApiGateway(TestCase):

    def test_query_promql_instant_success(self):
        test_promql_query = 'test_promql_query'
        test_response = {'status': 'success'}
        with patch('storage_workflows.chronosphere.chronosphere_api_gateway.get') as mock_get:
            mock_get.return_value.json.return_value = test_response
            response = ChronosphereApiGateway.query_promql_instant(test_promql_query)
            mock_get.assert_called_once_with(ChronosphereApiGateway.CHRONOSPHERE_PROMETHEUS_URL, 
                                             headers={'Authorization': 'Bearer {}'.format(ChronosphereApiGateway.CHRONOSPHERE_API_TOKEN)}, 
                                             params={'query': test_promql_query})
            self.assertEqual(response, test_response)

    def test_query_promql_instant_failure(self):
        test_promql_query = 'test_promql_query'
        test_response = {'status': 'failure'}
        with patch('storage_workflows.chronosphere.chronosphere_api_gateway.get') as mock_get:
            mock_get.return_value.json.return_value = test_response
            with self.assertRaises(Exception) as context:
                ChronosphereApiGateway.query_promql_instant(test_promql_query)
            self.assertTrue('Prometheus query failed' in str(context.exception))