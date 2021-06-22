import logging
from kubernetes import client
from kube_api.config import core_v1_api as api
from .utils import api_request, get_dict_value
logger = logging.getLogger(__name__)


class VolumeClaim:
    """Represents a volume claim.
    """
    def __init__(self, claim_name, disk_space, namespace='default', storage_class="standard-fc"):
        """ Initialize a VolumeClaim object
        """
        self.name = claim_name
        self.namespace = namespace
        self.vc_spec = dict(access_modes=['ReadWriteOnce'], storage_class_name=storage_class)
        self.disk_space = disk_space

    @staticmethod
    def generate_claim_name(prefix, n=6):
        """Generates a volume claim name by concatenating a prefix and a string of random lower case letters.
        The prefix will be converted to lower case string.
        Non-alphanumeric characters in the prefix will be replaced by "-".

        Args:
            prefix (string): Prefix of the claim name.
            n (int): The number of random lower case letters to be appended to the prefix.

        Returns: A string like "prefix-abcdef"

        """
        # Replace non alpha numeric and convert to lower case.
        if prefix:
            claim_name = re.sub('[^0-9a-zA-Z]+', '-', str(prefix).strip()).lower() + "-"
        else:
            claim_name = ""
        if len(claim_name) > (48 - n):
            claim_name = claim_name[:48 - n]
        # Append a random string
        claim_name += ''.join(random.choice(string.ascii_lowercase) for _ in range(n))
        return claim_name

    def create(self, vc_spec=None):
        """Creates a volume claim on the cluster.

        Args:
            vc_spec: A dictionary of keyword arguments that will be passed to V1PersistentVolumeClaimSpec()

        Returns: A dictionary containing the results of creating the volume claim on the cluster.

        """
        if vc_spec is None:
            vc_spec = self.vc_spec

        # Convert claim name to lower case
        claim_name = str(self.name).lower()
        vc_body = client.V1PersistentVolumeClaim()
        vc_body.metadata = client.V1ObjectMeta(namespace=self.namespace, name=claim_name)
        resources = client.V1ResourceRequirements(requests={'storage': str(self.disk_space)+'Gi'})
        vc_body.spec = client.V1PersistentVolumeClaimSpec(resources=resources, **vc_spec)
        self.creation_response = api_request(api.create_namespaced_persistent_volume_claim, self.namespace, vc_body)
        return self.creation_response

    def delete(self):
        body = client.V1DeleteOptions(propagation_policy='Foreground')
        return api_request(api.delete_namespaced_persistent_volume_claim,
                            name=self.name,
                            namespace=self.namespace,
                            body=body)
