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