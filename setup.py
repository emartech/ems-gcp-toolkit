from setuptools import setup, find_packages

setup(
    name="ems-gcp-toolkit",
    version="0.1.53",
    packages=find_packages(exclude="tests"),
    url="https://github.com/emartech/ems-gcp-toolkit",
    license="MIT",
    author="Emarsys",
    author_email="",
    description="",
    install_requires=[
        "google-cloud-storage==1.14.0",
        "google-cloud-pubsub==0.44.0",
        "google-api-core==1.14.2",
        "googleapis-common-protos==1.6.0",
        "google-cloud-pubsub==0.44.0",
        "grpc-google-iam-v1==0.12.3"
    ]
)
