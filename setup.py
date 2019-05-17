from setuptools import setup, find_packages

setup(
    name="ems-gcp-toolkit",
    version="0.1.49",
    packages=find_packages(exclude="tests"),
    url="https://github.com/emartech/ems-gcp-toolkit",
    license="MIT",
    author="Emarsys",
    author_email="",
    description="",
    install_requires=[
        "google-cloud-pubsub<=0.39.1"
    ]
)
