import logging
from kubernetes import client
from .utils import api_request
logger = logging.getLogger(__name__)


class Pod:
    """Represents a pod
    """
    def __init__(self, pod_name, namespace='default'):
        self.name = pod_name
        self.namespace = namespace
        self.api = client.CoreV1Api()

    def info(self):
        """Gets the pod information as a dictionary.

        Returns: A dictionary containing pod information.

        """
        logger.debug("Getting info from pod: %s in %s" % (self.name, self.namespace))
        response = api_request(self.api.read_namespaced_pod, self.name, self.namespace)
        if response.get("error"):
            metadata = response.get("metadata", {})
            metadata.update({
                "name": self.name,
                "namespace": self.namespace
            })
            response["metadata"] = metadata
        response["logs"] = self.logs()
        return response

    def logs(self):
        """Gets the logs of pod

        Returns: A string containing the logs. None if there is an error.

        """
        logger.debug("Getting logs from pod: %s in %s" % (self.name, self.namespace))
        pod_logs = api_request(self.api.read_namespaced_pod_log, self.name, self.namespace)
        # There is an error if the pod_logs is a dictionary
        if isinstance(pod_logs, dict):
            return None
        logger.debug("Logs have %s lines" % len(pod_logs.split("\n")))
        return pod_logs


def pod_template(containers, volumes=None, **kwargs):
    if volumes is not None and not volumes:
        volumes = None
    template = client.V1PodTemplate()
    template.template = client.V1PodTemplateSpec()
    post_start_command = ""
    post_start_handler = client.V1Handler(_exec=client.V1ExecAction(command=post_start_command.split(" ")))
    template.template.spec = client.V1PodSpec(
        containers=containers,
        volumes=volumes,
        restart_policy='Never',
        **kwargs
    )
    return template
