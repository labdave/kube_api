"""Build Script for setuptools

This build script must be executed outside of the source code directory.
The version number will be generated using the most recent tag and the number of commits on the master branch.
[TAG].[COMMIT_COUNT]

See Also: https://packaging.python.org/tutorials/packaging-projects/
"""
import setuptools
import os

package_name = "kube_api"
package_description = "A simple Kubernetes Python API"
package_url = "https://github.com/labdave/kube_api"

with open(os.path.join(package_name, "README.md"), "r") as fh:
    long_description = fh.read()

with open(os.path.join(package_name, "requirements.txt"), "r") as f:
    requirements = f.read().split("\n")
    requirements = [r.strip() for r in requirements if r.strip()]

release_version = str(os.popen("cd %s && git tag | tail -1" % package_name).read()).strip()
if not release_version:
    raise ValueError("Release version not found.")
commit_version = str(os.popen("cd %s && git rev-list --count master" % package_name).read()).strip()

setuptools.setup(
    name=package_name,
    version="%s.%s" % (release_version, commit_version),
    author="Qiu Qin",
    author_email="qiuosier@gmail.com",
    description=package_description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=package_url,
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
