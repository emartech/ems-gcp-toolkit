from setuptools import setup, find_packages

setup(
    name="ems-gcp-toolkit",
    version="0.2.1",
    packages=find_packages(exclude="tests"),
    url="https://github.com/emartech/ems-gcp-toolkit",
    license="MIT",
    author="Emarsys",
    author_email="",
    description="",
    install_requires=[
        "google-cloud-storage>=2,<3",
        "google-cloud-spanner>=3,<4",
        "google-cloud-pubsub>=2,<3",
        "google-cloud-bigquery>=3,<4",
        "google-cloud-core>=2,<3",
        "google-api-core>=2,<3",
        "googleapis-common-protos>=1.63.0,<2",
        "grpc-google-iam-v1==0.13.0"
    ]
)
