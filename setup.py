from setuptools import setup, find_packages

setup(
    name="ems-gcp-toolkit",
    version="0.1.84",
    packages=find_packages(exclude="tests"),
    url="https://github.com/emartech/ems-gcp-toolkit",
    license="MIT",
    author="Emarsys",
    author_email="",
    description="",
    install_requires=[
        "google-cloud-storage>=1.19.0,<2",
        "google-cloud-spanner>=1.17.0,<2",
        "google-cloud-pubsub>=1.0.0,<2",
        "google-cloud-bigquery>=1.19.0,<2",
        "google-cloud-core>=1.0.3,<2",
        "google-api-core>=1.14.2,<2",
        "googleapis-common-protos>=1.6.0,<2",
        "grpc-google-iam-v1==0.12.3",
        "grpcio-gcp==0.2.2"
    ]
)
