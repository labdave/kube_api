from kubernetes import client
from kube_api.config import core_v1_api
from .utils import api_request


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
