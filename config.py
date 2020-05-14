import os
import logging
from kubernetes import config, client
logger = logging.getLogger(__name__)


configuration = None
batch_v1_api = None
core_v1_api = None


def load_configuration(config_file_path):
    """Loads the Kubernetes configurations from a file.

    configuration and batch_v1_api will be set after loading the config.

    """
    logger.debug("Loading Kubernetes config from %s ..." % config_file_path)
    config.load_kube_config(config_file=config_file_path)
    global configuration
    global batch_v1_api
    global core_v1_api
    configuration = client.Configuration()
    batch_v1_api = client.BatchV1Api(client.ApiClient(configuration))
    core_v1_api = client.CoreV1Api(client.ApiClient(configuration))


config_file = os.environ.get("KUBERNETES_CONFIG")
if config_file:
    load_configuration(config_file)
