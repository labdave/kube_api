import logging
import base64
from kubernetes import client
from kube_api.config import core_v1_api
from .utils import api_request
logger = logging.getLogger(__name__)


def create(name):
    """Creates a new namespace
    """
    body = client.V1Namespace()
    body.metadata = client.V1ObjectMeta(name=name)
    response = api_request(core_v1_api.create_namespace, body)
    return response


def list_all():
    """Lists all existing namespaces
    """
    return api_request(core_v1_api.list_namespace)


class Namespace:
    def __init__(self, namespace):
        self.namespace = namespace

    def list_secrets(self):
        """

        Returns (list):

        """
        response = api_request(core_v1_api.list_namespaced_secret, self.namespace)
        secrets = response.get("items", [])
        return secrets

    def secret_names(self):
        secrets = self.list_secrets
        if not secrets:
            return []
        return [s.get("metadata", {}).get("name") for s in secrets]

    def get_secret(self, name):
        secrets = self.list_secrets()
        if not secrets:
            return None
        for s in secrets:
            if s.get("metadata", {}).get("name") == name:
                return s
        return None

    def setup_secret_file(self, secret_name, file_path, filename):
        """Configure a file as a cluster namespaced secret

        Args:
            file_path (str): The source file path to be saved as a secret on the cluster.
            secret_name (str): The secret name.
            filename (str): The filename to be used in the secret volume.

        Returns:

        """
        with open(file_path, 'rb') as f:
            secret_content = base64.b64encode(f.read()).decode()
        secret = self.get_secret(secret_name)
        if isinstance(secret, dict):
            if secret.get("data", {}).get(filename) == secret_content:
                logger.debug("%s already in namespace." % secret_name)
                return client.V1SecretVolumeSource(secret_name=secret_name)
            # Remove secret
            logger.debug("Replacing existing %s..." % secret_name)
            res = api_request(core_v1_api.delete_namespaced_secret, name=secret_name, namespace=self.namespace)
            logger.debug(res)

        # See https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Secret.md
        metadata = {
            'name': secret_name,
            'namespace': self.namespace
        }
        secret = client.V1Secret(data={
            filename: secret_content
        }, metadata=metadata)
        logger.debug("Creating %s..." % secret_name)
        res = api_request(core_v1_api.create_namespaced_secret, self.namespace, secret)
        if res.get("error"):
            logger.debug("Failed to create %s" % secret_name)
            return None
        return client.V1SecretVolumeSource(secret_name=secret_name)
