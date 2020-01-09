import logging
from kubernetes import client
from .utils import api_request
logger = logging.getLogger(__name__)


class Pod:
    def __init__(self, pod_name, namespace='default'):
        self.name = pod_name
        self.namespace = namespace
        self.api = client.CoreV1Api()

    def info(self):
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
        logger.debug("Getting logs from pod: %s in %s" % (self.name, self.namespace))
        try:

            pod_log = self.api.read_namespaced_pod_log(self.name, self.namespace)
            logger.debug("Logs have %s lines" % len(pod_log.split("\n")))
        except Exception as e:
            pod_log = None
            logger.debug(e)
        return pod_log


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
