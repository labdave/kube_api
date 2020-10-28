import os
import logging
import json
from kubernetes import config, client
logger = logging.getLogger(__name__)


configuration = None
batch_v1_api = None
core_v1_api = None


def __parse_identity_json(data):
    api_key_prefix = 'Bearer'

    return data['cert'], data['host'], data['api_key'], api_key_prefix


def load_configuration(config_file_path):
    """Loads the Kubernetes configurations from a file.

    configuration and batch_v1_api will be set after loading the config.

    """
    logger.debug("Loading Kubernetes config from %s ..." % config_file_path)
    config_data = None
    try:
        config_data = json.load(open(config_file))
        cert_path, host, api_token, api_prefix = __parse_identity_json(config_data)
        cert_file = os.environ.get("KUBERNETES_CERT")
        if cert_file:
            cert_path = cert_file
    except Exception:
        config.load_kube_config(config_file=config_file_path)

    global configuration
    global batch_v1_api
    global core_v1_api
    configuration = client.Configuration()

    # build out configuration if provided with json config file
    if config_data:
        configuration.api_key["authorization"] = api_token
        configuration.api_key_prefix['authorization'] = api_prefix
        configuration.host = host
        configuration.ssl_ca_cert = cert_path
    batch_v1_api = client.BatchV1Api(client.ApiClient(configuration))
    core_v1_api = client.CoreV1Api(client.ApiClient(configuration))


config_file = os.environ.get("KUBERNETES_CONFIG")
if config_file:
    load_configuration(config_file)
