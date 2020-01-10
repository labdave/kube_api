# Kubernetes Python API
This repository contains a simple Kubernetes Python API (Kube API). This API is built using the [Official Python client library](https://github.com/kubernetes-client/python) (Official API) for [kubernetes](http://kubernetes.io/)

## Handling Exceptions
The official API raises exceptions when there is an error. Kube API will catch the ApiException and return an error message instead. This is implemented by wrapping the function calls to the official API using the `api_request()` function in `utils.py`.

When there is an error calling the official API, Kube API will return a JSON response containing the body of the ApiException (which is a dictionary-like object), plus three (3) additional fields:
* status, the status returned in the ApiException
* error, the reason of the error
* headers, the header of the ApiException
