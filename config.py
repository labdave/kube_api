import os
import logging
from kubernetes import config, client
logger = logging.getLogger(__name__)


def load_configuration(config_file_path):
    """Loads the Kubernetes configurations from a file.

    configuration and batch_v1_api will be set after loading the config.

    """
    logger.debug("Loading Kubernetes config from %s ..." % config_file_path)
    config.load_kube_config(config_file=config_file_path)
    client_config = client.Configuration()
    batch_v1 = client.BatchV1Api(client.ApiClient(client_config))
    core_v1 = client.CoreV1Api(client.ApiClient(client_config))
    return client_config, batch_v1, core_v1


config_file = os.environ.get("KUBERNETES_CONFIG")
if config_file:
    configuration, batch_v1_api, core_v1_api = load_configuration(config_file)
