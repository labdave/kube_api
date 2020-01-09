import logging
import json
from commons.Aries.strings import stringify
from kubernetes.client.rest import ApiException
logger = logging.getLogger(__name__)


def api_request(api_func, *args, **kwargs):
    try:
        response = api_func(*args, **kwargs).to_dict()
    except ApiException as e:
        logger.debug("Exception when calling %s: %s" % (api_func.__name__, e))
        response = {
            "status": e.status,
            "error": e.reason,
            "headers": stringify(e.headers),
        }
        response.update(json.loads(e.body))
    return response
