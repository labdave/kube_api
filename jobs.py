import logging
import re
import random
import string
import time
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
        self.creation_response = None
        self.__status = None
        self.__logs = None

    @staticmethod
    def run_shell_commands(image, commands, name_prefix="", namespace='default', **kwargs):
        """Runs commands on a docker image using "/bin/sh -c".

        Args:
            image: Docker image for running the job.
            commands (string or list): Job commands.
                commands can be a string with line breaks (each line is a command), or
                a list of strings, each is a command.
                Empty lines will be removed.
            name_prefix: A string prefix for the job name.
                A random string will be used as job name if name_prefix is empty or None.
            namespace: Namespace for the job.
            **kwargs: keyword arguments to be passed into Job.create(), which can be:
                job_spec: a dictionary of keyword arguments for initializing V1PJobSpec()
                pod_spec: a dictionary of keyword arguments for initializing V1PodSpec()

        Returns:

        """
        if not isinstance(commands, list):
            # Parse text string and Remove empty lines from commands.
            # Each line is a command, the commands will be connected with "&&" instead of line break.
            commands = [c.strip() for c in str(commands).split("\n") if c.strip()]
        args = " && ".join(commands)
        # Replace non alpha numeric and convert to lower case.
        if name_prefix:
            job_name = re.sub('[^0-9a-zA-Z]+', '-', str(name_prefix).strip()).lower() + "-"
        else:
            job_name = ""
        # Append a random string
        job_name += ''.join(random.choice(string.ascii_lowercase) for _ in range(6))
        logger.debug("Job:%s, Image: %s" % (job_name, image))
        logger.debug("%d commands." % len(args))
        job = Job(job_name, namespace)
        job.add_container(
            image,
            ["/bin/sh", "-c"],
            args
        ).create(job_spec=dict(backoff_limit=0), **kwargs)
        return job

    @staticmethod
    def env_var(**kwargs):
        env_list = []
        if kwargs:
            for env_name, env_value in kwargs.items():
                env_list.append(client.V1EnvVar(name=env_name, value=env_value))
        return env_list

    @property
    def server_status(self):
        job_status = self.info().get("status", dict())
        if not isinstance(job_status, dict):
            return dict()
        return job_status

    @property
    def is_active(self):
        return self.server_status.get("active")

    @property
    def succeeded(self):
        return self.server_status.get("succeeded")

    def wait(self, interval=20, timeout=7200):
        """Waits for the job to finish
        """
        counter = 0
        # Stop checking the results if the job is running for more than 3 hours.
        while counter < timeout:
            # Check job status every interval
            time.sleep(interval)
            status = self.status().get("status", {})
            if status.get("succeeded"):
                logger.debug("Job %s succeeded." % self.job_name)
                return self
            elif status.get("failed"):
                logger.error("Job %s failed" % self.job_name)
                return self
            elif not status.get("active"):
                logger.debug("Job %s is no longer active." % self.job_name)
                return self
            counter += interval
        logger.error("Timeout: Job %s has been running for more than %s seconds" % (self.job_name, timeout))
        return self

    def info(self, use_cache=True):
        """Job info from the cluster
        """
        if self.__status and use_cache:
            return self.__status
        s = api_request(api.read_namespaced_job_status, self.job_name, self.namespace)
        # Save the status if the job is no longer active
        job_status = s.get("status", dict())
        logger.debug(job_status)
        if isinstance(job_status, dict) and not job_status.get("active"):
            self.__status = s
        return s

    def status(self):
        return self.info()

    def logs(self, use_cache=True):
        """Gets the logs of the job from the last pod running the job.
        This method will try to the logs from last succeeded pod.
        The logs of the last pod will be returned if there is no succeeded pod.

        Caution: Only logs from ONE pod will be returned.

        Returns: A string containing the logs. None if logs are not available.

        """
        if self.__logs and use_cache:
            return self.__logs
        job_logs = None
        for pod_name in self.pod_names():
            pod = Pod(pod_name, self.namespace)
            pod_info = pod.info()
            # TODO: sort the pods by time
            # Use the logs from succeeded pod if there is one
            pod_status = pod_info.get("status", {})
            if isinstance(pod_status, dict):
                phase = pod_status.get("phase")
            else:
                phase = ""
            # Save pod logs as the best available log
            job_logs = pod.logs()
            # Use pod logs as job logs if pod finished successfully.
            if phase == "Succeeded":
                break
        if not self.is_active:
            self.__logs = job_logs
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

    def add_container(self, container_image, command, command_args=None, container_name=None, **kwargs):
        # Use job name as the default container name
        if not container_name:
            container_name = self.job_name
        # Make command and command_args as lists
        if not isinstance(command, list):
            command = [command]
        if command_args and not isinstance(command_args, list):
            command_args = [command_args]

        container = client.V1Container(
            # lifecycle=client.V1Lifecycle(post_start=post_start_handler),
            command=command,
            args=command_args,
            name=container_name,
            image=container_image,
            **kwargs
        )
        self.containers.append(container)
        return self

    def add_volume(self, **kwargs):
        self.volumes.append(client.V1Volume(**kwargs))
        return self

    def create(self, job_spec=None, pod_spec=None):
        """Creates and runs the job on the cluster.

        Args:
            job_spec: A dictionary of keyword arguments that will be passed to V1JobSpec()
            pod_spec: A dictionary of keyword arguments that will be passed to V1PodSpec()

        Returns: A dictionary containing the results of creating the job on the cluster.

        """
        if job_spec is None:
            job_spec = {}
        if pod_spec is None:
            pod_spec = {}
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
        template = pod_template(self.containers, self.volumes, **pod_spec)
        job_body.spec = client.V1JobSpec(template=template.template, **job_spec)
        self.creation_response = api_request(api.create_namespaced_job, self.namespace, job_body)
        return self.creation_response

    def delete(self):
        body = client.V1DeleteOptions(propagation_policy='Foreground')
        return api_request(api.delete_namespaced_job, name=self.job_name, namespace=self.namespace, body=body)
