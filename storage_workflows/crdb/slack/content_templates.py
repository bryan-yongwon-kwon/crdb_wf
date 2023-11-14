import json

class ContentTemplate:

    @staticmethod
    def get_workflow_failure_content(namespace:str, 
                                     workflow_name:str,
                                     cluster_name:str,
                                     deployment_env:str,
                                     region:str) -> dict:
        workflow_url = "https://argo-workflows.infra-control-plane.doordash.red/workflows/{}/{}?tab=workflow".format(namespace, workflow_name)
        return {
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": ":exclamation: Operator Service workflow `{}` failed! \n\n<{}|View workflow>".format(workflow_name, 
                                                                                                                                 workflow_url)
                                }
                            },
                            {
                                "type": "section",
                                "fields": [
                                    {
                                        "type": "mrkdwn",
                                        "text": "*Cluster Name*\n{}".format(cluster_name)
                                    }
                                ]
                            },
                            {
                                "type": "section",
                                "fields": [
                                    {
                                        "type": "mrkdwn",
                                        "text": "*Env*\n{}".format(deployment_env)
                                    }
                                ]
                            },
                            {
                                "type": "section",
                                "fields": [
                                    {
                                        "type": "mrkdwn",
                                        "text": "*Region*\n{}".format(region)
                                    }
                                ]
                            }
                        ]
                    }
    
    @staticmethod
    def get_health_check_template(results):
        def build_block(result):
            return {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": result
                        }
                    }
        results = list(map(lambda result: build_block(result), results))
        return {
                    "blocks": results
                }
    
    @staticmethod
    def get_rebalance_and_recovery_rates_not_match_alert_template(namespace:str,
                                                                  workflow_id:str,
                                                                  cluster_name: str,
                                                                  rebalance_rate: str,
                                                                  recovery_rate: str) -> dict:
        workflow_url = "https://argo-workflows.infra-control-plane.doordash.red/workflows/{}/{}?tab=workflow".format(namespace, workflow_id)
        return {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":exclamation: Rebalance rate and recovery rate are not equal. Please check. \n\n<{}|*Workflow*> \n\n*Cluster Name*: {}\n\n*Current rebalance rate*: {}\n\n*Current recovery rate*: {}".format(workflow_url,
                                                                                                                                                                                                                                       cluster_name, 
                                                                                                                                                                                                                                       rebalance_rate, 
                                                                                                                                                                                                                                       recovery_rate)
                            }
                        }
                    ]
                }
    
    @staticmethod
    def get_average_cpu_high_alert_template(namespace:str,
                                            workflow_id:str,
                                            cluster_name: str,
                                            rebalance_rate: str,
                                            recovery_rate: str) -> dict:
        workflow_url = "https://argo-workflows.infra-control-plane.doordash.red/workflows/{}/{}?tab=workflow".format(namespace, workflow_id)
        return {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":exclamation: CPU still high after reducing the rebalance rate. Please check. \n\n<{}|*Workflow*> \n\n*Cluster Name*: {}\n\n*Current rebalance rate*: {}\n\n*Current recovery rate*: {}".format(workflow_url,
                                                                                                                                                                                                                                         cluster_name, 
                                                                                                                                                                                                                                         rebalance_rate, 
                                                                                                                                                                                                                                         recovery_rate)
                            }
                        }
                    ]
                }