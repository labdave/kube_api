from kubernetes import client
from kube.config import configuration
from .utils import api_request


# create an instance of the API class
core_v1_api = client.CoreV1Api(client.ApiClient(configuration))


def create(name):
    body = client.V1Namespace()
    body.metadata = client.V1ObjectMeta(name=name)
    response = api_request(core_v1_api.create_namespace, body)
    return response


def list_all():
    return api_request(core_v1_api.list_namespace)
