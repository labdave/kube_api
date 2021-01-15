import logging
import re
import random
import string
import time
import copy
from kubernetes import client
from kube_api.config import batch_v1_api as api, core_v1_api as core_api
from .utils import api_request
from .pods import Pod, pod_template
from .volumeclaims import VolumeClaim
logger = logging.getLogger(__name__)


def list_all(namespace=None, prefix=""):
    """Lists all jobs on the cluster
    If namespace is specified, lists only jobs in the namespace.
    """
    if namespace:
        logger.debug("Getting jobs for namespace %s..." % namespace)
        response = api_request(api.list_namespaced_job, namespace)
    else:
        logger.debug("Getting jobs for all namespaces...")
        response = api_request(api.list_job_for_all_namespaces)
    if prefix:
        filtered_items = []
        items = response.get("items")
        for item in items:
            job_name = item.get("metadata", {}).get("name")
            if job_name and str(job_name).startswith(prefix):
                filtered_items.append(item)
        response["items"] = filtered_items
    return response


class Job:
    """Represents a kubernetes job.
    """
    def __init__(self, job_name, namespace='default'):
        """Initialize a Job object, which can be an existing job or used to create a new job.
        """
        self.job_name = job_name
        self.namespace = namespace
        self.job_spec = dict()
        self.pod_spec = dict()

        self._containers = []
        self._volumes = []
        self._volume_claims = []

        self.creation_response = dict()
        self.__status = None
        self.__logs = None

    @staticmethod
    def parse_commands(commands):
        """Parses shell commands from string or list of strings.

        Args:
            commands (str or list): Commands as a string with line breaks or a list of commands
            If commands is a string, the commands must be separated by line breaks ("\n").
            If commands is a list, each string in the list should be a command.
            Empty lines will be removed.

        Returns: A string of commands connect by "&&"

        """
        if not isinstance(commands, list):
            # Parse text string and Remove empty lines from commands.
            # Each line is a command, the commands will be connected with "&&" instead of line break.
            commands = [c.strip() for c in str(commands).strip().split("\n") if c.strip()]
            logger.debug("%d commands." % len(commands))
        args = " && ".join(commands)
        return args

    @staticmethod
    def generate_job_name(prefix, n=6):
        """Generates a job name by concatenating a prefix and a string of random lower case letters.
        The prefix will be converted to lower case string.
        Non-alphanumeric characters in the prefix will be replaced by "-".

        Args:
            prefix (string): Prefix of the job name.
            n (int): The number of random lower case letters to be appended to the prefix.

        Returns: A string like "prefix-abcdef"

        """
        # Replace non alpha numeric and convert to lower case.
        if prefix:
            job_name = re.sub('[^0-9a-zA-Z]+', '-', str(prefix).strip()).lower() + "-"
        else:
            job_name = ""
        if len(job_name) > (48 - n):
            job_name = job_name[:48 - n]
        # Append a random string
        job_name += ''.join(random.choice(string.ascii_lowercase) for _ in range(n))
        return job_name

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
        job_name = Job.generate_job_name(name_prefix)
        logger.debug("Job:%s, Image: %s" % (job_name, image))
        job = Job(job_name, namespace)
        job.add_shell_commands(image, commands).create(job_spec=dict(backoff_limit=0), **kwargs)
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
        """Waits for the job to finish.
        This method works by checking the job status periodically (every interval, e.g. 20 seconds).
        This method will return if:
            The job is succeeded, failed or no longer active due to some other reason.
            Or timeout is reached.

        Args:
            interval (int): The time interval in seconds between checking the job status.
            timeout (int): Timeout in seconds.
                This method will return after timeout, regardless of the job status.

        Returns: self.

        """
        counter = 0
        # Stop checking the results if the job is running for more than 3 hours.
        while counter < timeout:
            # Check job status every interval
            time.sleep(interval)
            status = self.status().get("status", {})
            if status.get("succeeded"):
                logger.debug("Job %s succeeded." % self.job_name)
                self.cleanup()
                return self
            elif status.get("failed"):
                logger.error("Job %s failed" % self.job_name)
                self.cleanup()
                return self
            elif not status.get("active"):
                logger.debug("Job %s is no longer active." % self.job_name)
                self.cleanup()
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
        # logger.debug(job_status)
        if isinstance(job_status, dict) and not job_status.get("active"):
            self.__status = s
        return s

    def status(self):
        """Same as info().
        The actual status information get be obtained by self.status().get("status").
        The method may be modified to return just the status in the future.
        """
        # TODO: return self.info().get("status") instead.
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
            job_logs = pod_info.get("logs")
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
        response = api_request(core_api.list_pod_for_all_namespaces, watch=False, pretty='true')
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

    def add_shell_commands(self, image, commands, **kwargs):
        """Adds an image running shell commands
        """
        args = Job.parse_commands(commands)
        return self.add_container(
            image,
            ["/bin/sh", "-c"],
            args,
            **kwargs
        )

    def add_container(self, container_image, command, command_args=None, container_name=None, **kwargs):
        """Adds a container to the job

        This is a general purpose method for adding a docker container.
        Use add_shell_commands() to run shell/bash commands.

        Args:
            container_image: The docker image.
            command: The command to be executed. This will replace the ENTRYPOINT of the docker image.
            command_args: Additional arguments for the command.
            container_name: Optional container name.
                If container_name is not specified, a random name will be generated based on the job name.
            **kwargs: keyword arguments to be passed into client.V1Container()

        Returns: self

        """
        # Use job name as the default container name
        if not container_name:
            container_name = self.job_name + '-' + ''.join(random.choice(string.ascii_lowercase) for _ in range(3))
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
        self._containers.append(container)
        return self

    def clear_containers(self):
        self._containers = []
        return self

    def run_container(self, container_image, command, command_args=None, container_name=None, **kwargs):
        """Creates a job and run the container with a command.
        """
        self.add_container(container_image, command, command_args, container_name, **kwargs)
        response = self.create()
        self.clear_containers()
        return response

    def add_volume(self, **kwargs):
        self._volumes.append(client.V1Volume(**kwargs))
        return self

    def cleanup(self):
        if self._volumes:
            for vol in self._volumes:
                if vol.persistent_volume_claim:
                    response = api_request(core_api.delete_namespaced_persistent_volume_claim,
                                            namespace=self.namespace,
                                            name=vol.persistent_volume_claim.claim_name)

    def create(self, job_spec=None, pod_spec=None):
        """Creates and runs the job on the cluster.

        Args:
            job_spec: A dictionary of keyword arguments that will be passed to V1JobSpec()
            pod_spec: A dictionary of keyword arguments that will be passed to V1PodSpec()

        Returns: A dictionary containing the results of creating the job on the cluster.

        """
        if job_spec is None:
            job_spec = self.job_spec
        if pod_spec is None:
            pod_spec = self.pod_spec
        if not self._containers:
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
        template = pod_template(self._containers, self._volumes, **pod_spec)
        job_body.spec = client.V1JobSpec(template=template.template, **job_spec)
        self.creation_response = api_request(api.create_namespaced_job, self.namespace, job_body)
        return self.creation_response

    def delete(self):
        body = client.V1DeleteOptions(propagation_policy='Foreground')
        return api_request(api.delete_namespaced_job, name=self.job_name, namespace=self.namespace, body=body)


class WorkspaceJob(Job):
    """Represents a job running bash commands using a list of containers with shared workspace.

    "workspace" and "outputs" volumes are shared by all containers.

    Attributes:
        workspace_path: The mount path for workspace volume.
        outputs_path: The mount path for outputs volume
    """
    def __init__(self, job_name, namespace='default', workspace_path="/workspace/", output_path=None, workspace_size="1", output_size="1"):
        super().__init__(job_name, namespace)
        self.workspace_path = workspace_path
        if not self.workspace_path.endswith("/"):
            self.workspace_path += "/"
        self.outputs_path = output_path
        if isinstance(self.outputs_path, str) and not self.outputs_path.endswith("/"):
            self.outputs_path += "/"

        self.workspace_size = workspace_size
        self.output_size = output_size

        self.job_spec = dict(backoff_limit=0)
        self.workspace_vc = None
        self.outputs_vc = None
        # envs will be shared by all jobs.
        self.envs = dict()
        if self.outputs_path:
            self.envs["OUTPUT_PATH"] = self.outputs_path

    def add_envs(self, envs):
        self.envs.update(envs)
        return self.envs

    def _add_volumes(self):
        # See https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Volume.md
        self.workspace_vc = VolumeClaim(self.job_name+"-wvc", self.workspace_size, self.namespace)
        wrkspc_response = self.workspace_vc.create()
        self.add_volume(
            name="workspace",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=self.job_name+"-wvc"
            )
        )

        # See https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1VolumeMount.md
        volume_mounts = [
            client.V1VolumeMount(mount_path=self.workspace_path, name="workspace"),
        ]
        # Add output volume, if outputs_path is not the same as the workspace_path.
        if self.outputs_path and self.outputs_path != self.workspace_path:
            self.outputs_vc = VolumeClaim(self.job_name+"-ovc", self.output_size, self.namespace)
            out_response = self.outputs_vc.create()
            self.add_volume(
                name="outputs",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=self.job_name+"-ovc"
                )
            )
            volume_mounts.append(
                client.V1VolumeMount(mount_path=self.outputs_path, name="outputs"),
            )

        return volume_mounts

    def run_containers(self, containers, composition='sequential'):
        """Runs a list of containers one after another.

        Args:
            containers: A list of dictionaries, each defines the following key value pairs:
                name: the docker image name.
                args: the commands to be executed using "/bin/sh -c".
                envs: Optional, a dictionary of environment variables.

                cpu: Optional, the number of CPU requested.
                memory: Optional, the size of memory (GB) requested.
                storage: Optional, the disk storage (GB) requested.

            composition: Indicates whether the containers should be executed sequentially or in parallel.
                Containers will be executed sequentially if composition is "sequential".
                Otherwise, the containers will be executed in parallel.

        Returns:

        """
        workspace_mounts = self._add_volumes()
        for i, container in enumerate(containers):
            name = container.get("name")
            args = container.get("args")
            vols = container.get("volumes", [])
            # Additional volumes for this container
            volume_mounts = workspace_mounts + [
                client.V1VolumeMount(name=v.get("name"), mount_path=v.get("path")) for v in vols if isinstance(v, dict)
            ]
            # Additional environment variables for this container
            container_envs = copy.deepcopy(self.envs)
            container_envs.update(self.parse_env(container.get("env")))
            if container_envs:
                env = Job.env_var(**container_envs)
            else:
                env = None

            # CPU and memory
            cpu = container.get("cpu", 0.2)
            memory = container.get("memory", 0.5)
            if not isinstance(memory, str):
                memory = "%sG" % memory
            resources = client.V1ResourceRequirements(
                requests={'cpu': cpu, 'memory': memory}
            )

            additional_keys = [
                "image_pull_policy",
                "termination_message_path",
                "termination_message_policy",
                "tty",
                "stdin",
                "stdin_once"
            ]

            kwargs = {}
            for key, value in container.items():
                if key in additional_keys:
                    kwargs[key] = value
            logger.debug("Container has additional arguments: %s" % kwargs)
            # Add container to run shell command
            self.add_shell_commands(
                name, args, volume_mounts=volume_mounts, env=env, resources=resources, **kwargs
            )
        # logger.debug(self._containers)
        job_containers = self._containers
        if composition == "sequential":
            # Run jobs in order using init_containers
            # See https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
            if len(job_containers) > 1:
                self.pod_spec["init_containers"] = job_containers[:-1]
                self._containers = [job_containers[-1]]

        self.create()
        return self

    @staticmethod
    def parse_env(envs):
        if isinstance(envs, dict):
            return envs
        if isinstance(envs, list):
            return WorkspaceJob.parse_env_strings(envs)
        return dict()

    @staticmethod
    def parse_env_strings(env_list):
        """

        Args:
            env_list: A list of environment variable defined in "KEY=VALUE" strings

        Returns: A dictionary of environment variable as key-value pairs.

        """
        env_dict = dict()
        for env_str in env_list:
            env_arr = str(env_str).split("=", 1)
            if len(env_arr) < 2:
                raise ValueError("Environment Variable must be in the format of KEY=VALUE")
            env_dict[env_arr[0]] = env_arr[1]
        return env_dict

    @classmethod
    def run_config(cls, config, **kwargs):
        """Runs a job defined in a configuration file.

        Args:
            config: Job configuration as a dictionary.
            kwargs: Additional keyword arguments for initializing the job.
                The kwargs will be passed as cls(**kwargs)

        The configuration file should have a format similar to the Google Cloud Build configuration.
        See https://cloud.google.com/cloud-build/docs/build-config

        The following keys are implemented:
        steps: name, args, env, volumes
        options: env

        In addition, the config can have the following keys:
        prefix: A string prefix for the job name.
            A job name will be generated with a random string appended to the prefix.
        output_path: A directory for storing output data.
            This volume can be the same as workspace.
            User is responsible for copying the files output of output directory
            output_path will also be stored as a global environment variable OUTPUT_PATH
        workspace_size: Number of GB of storage needed for the workspace directory.
        output_size: Number of GB of storage needed for the output directory.

        In each step, the following additional keys are accepted:
        cpu: The number of cpu requested.
        memory: The memory size (GB) requested.

        Examples:
            If the config is stored in a json file, a job can be launched by:

            with open("path/to/json", 'r') as f:
                WorkspaceJob.run_config(json.load(f))

        """
        # prefix
        job_name = cls.generate_job_name(config.get("prefix", ""))
        # output_path
        # output_path from kwargs will have higher priority
        if "output_path" not in kwargs and config.get("output_path"):
            kwargs["output_path"] = config.get("output_path")
        if "workspace_size" not in kwargs and config.get("workspace_size"):
            kwargs["workspace_size"] = config.get("workspace_size")
        if "output_size" not in kwargs and config.get("output_size"):
            kwargs["output_size"] = config.get("output_size")
        if "preemptible" not in kwargs and config.get("preemptible"):
            kwargs["preemptible"] = config.get("preemptible")

        env_dict = dict()

        # Initialize job object
        # cls can be a subclass if run_config is called by a subclass.
        job = cls(job_name, **kwargs)

        # options - env
        if isinstance(config.get("options"), dict):
            env_dict.update(cls.parse_env(config.get("options").get("env")))
            job.add_envs(env_dict)

        # steps
        steps = config.get("steps")
        if not steps:
            raise ValueError("Config must have \"steps\" as key")
        # Run job
        job.run_containers(steps)
        return job
