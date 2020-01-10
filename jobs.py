import logging
from kubernetes import client
from kube_api.config import batch_v1_api as api
from .utils import api_request
from .pods import Pod, pod_template
logger = logging.getLogger(__name__)


def list_all(namespace=None):
    """Lists all jobs on the cluster
    If namespace is specified, lists only jobs in the namespace.
    """
    if namespace:
        logger.debug("Getting jobs for namespace %s..." % namespace)
        response = api_request(api.list_namespaced_job, namespace)
    else:
        logger.debug("Getting jobs for all namespaces...")
        response = api_request(api.list_job_for_all_namespaces)
    return response


class Job:
    """Represents a kubernetes job.
    """
    def __init__(self, job_name, namespace='default'):
        """Initialize a Job object, which can be an existing job or used to create a new job.
        """
        self.job_name = job_name
        self.namespace = namespace
        self.containers = []
        self.volumes = []

    def status(self):
        """Job status
        """
        return api_request(api.read_namespaced_job_status, self.job_name, self.namespace)

    def logs(self):
        """Gets the logs of the job from the last pod running the job.
        This method will try to the logs from last succeeded pod.
        The logs of the last pod will be returned if there is no succeeded pod.

        Caution: Only logs from ONE pod will be returned.

        Returns: A string containing the logs. None if logs are not available.

        """
        job_logs = None
        for pod_name in self.pod_names():
            pod = Pod(pod_name, self.namespace)
            pod_info = pod.info()
            # TODO: sort the pods by time
            # Use the logs from succeeded pod if there is one
            phase = pod_info.get("status", {}).get("phase")
            # Save pod logs as the best available log
            job_logs = pod.logs()
            # Use pod logs as job logs if pod finished successfully.
            if phase == "Succeeded":
                break
        return job_logs

    def pods(self):
        """Gets a list of pods for running the job.

        Returns: A list of pods, each is a pods.Pod object.

        """
        v1 = client.CoreV1Api()
        response = api_request(v1.list_pod_for_all_namespaces, watch=False, pretty='true')
        # logger.debug(response)
        if response.get("error"):
            return []
        pods = []
        # Loop through all the pods to find the pods for the job
        for pod in response.get("items"):
            if pod.get("metadata", {}).get("labels", {}).get("job-name") == self.job_name:
                # Create and append a Pod object.
                pods.append(Pod(
                    pod.get("metadata", {}).get("name", "N/A"),
                    pod.get("metadata", {}).get("namespace", "N/A"),
                ))
        logger.debug("%s pods for job %s" % (len(pods), self.job_name))
        return pods

    def pod_names(self):
        """Gets the names of the pods for running the job

        Returns: A list of strings.

        """
        return [pod.name for pod in self.pods()]

    def add_container(
            self, container_image, command,
            # Optional Arguments
            command_args=None, container_name=None, volume_mounts=None, resources=None, lifecycle=None,
            **envs
    ):
        if not container_name:
            container_name = self.job_name
        if not isinstance(command, list):
            command = [command]
        if command_args and not isinstance(command_args, list):
            command_args = [command_args]

        env_list = []
        if envs:
            for env_name, env_value in envs.items():
                env_list.append(client.V1EnvVar(name=env_name, value=env_value))
        container = client.V1Container(
            # lifecycle=client.V1Lifecycle(post_start=post_start_handler),
            command=command,
            args=command_args,
            name=container_name,
            image=container_image,
            env=env_list,
            volume_mounts=volume_mounts,
            # volume_mounts=[client.V1VolumeMount("/data")]
            lifecycle=lifecycle,
            resources=resources
        )
        self.containers.append(container)
        return self

    def add_volume(self):
        pass

    def create(self, **kwargs):
        """Creates and runs the job on the cluster.

        Args:
            kwargs: a dictionary of keyword arguments that will be passed to the pod_template()
                This corresponds to the keyword arguments for V1PodSpec()

        Returns: A dictionary containing the results of creating the job on the cluster.

        """
        if not self.containers:
            raise ValueError(
                "Containers not found. "
                "Use add_containers() to specify containers before creating the job."
            )
        # TODO: Set the backoff limit to 1. There will be no retry if the job fails.
        # Convert job name to lower case
        job_name = str(self.job_name).lower()
        job_body = client.V1Job(kind="Job")
        job_body.metadata = client.V1ObjectMeta(namespace=self.namespace, name=job_name)
        job_body.status = client.V1JobStatus()
        template = pod_template(self.containers, self.volumes, **kwargs)
        job_body.spec = client.V1JobSpec(ttl_seconds_after_finished=600, template=template.template)
        return api_request(api.create_namespaced_job, self.namespace, job_body)
