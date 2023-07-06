from requests import post
from time import time
from storage_workflows.logging.logger import Logger
from storage_workflows.global_change_log.service_name import ServiceName

class GlobalChangeLogGateway:

    PROD_ENDPOINT = "http://global-change-log-consumer.svc.ddnw.net:80/global-change-log"
    STAGING_ENDPOINT = "http://global-change-log.doorcrawl-int.com:80/global-change-log"

    @staticmethod
    def post_event(deployment_env,
                   service_name: ServiceName,
                   owner_id='storage@doordash.com',
                   tag='INFRASTRUCTURE_CHANGE', # https://github.com/doordash/services-protobuf/blob/master/protos/global_change_log/global_change_log_request.proto#L27
                   value_before='', 
                   value_after='', 
                   message='',
                   reference_url=''):
        endpoint = GlobalChangeLogGateway.PROD_ENDPOINT if deployment_env == 'prod' else GlobalChangeLogGateway.STAGING_ENDPOINT
        logger = Logger()
        headers = {'Content-type': 'application/json'}
        data = {
            "service_name": service_name.value,
            "tag": tag,
            "owner_id": owner_id,
            "updated_at_ms": int(time() * 1000)
        }
        if value_before:
            data["value_before"] = value_before
        if value_after:
            data["value_after"] = value_after
        if message:
            data["message"] = message
        if reference_url:
            data["reference_url"] = reference_url
        response = post(url=endpoint, headers=headers, json=data)
        if response.ok:
            logger.info("Successfully posted global change log.")
        else:
            logger.error("Failed to post global change log.")
        logger.info("Global Change Log response: {}".format(response.text))
