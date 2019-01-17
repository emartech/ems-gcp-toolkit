from setuptools import setup, find_packages

setup(
    name="ems-gcp-toolkit",
    version="0.1.9",
    packages=find_packages(exclude="tests"),
    url="https://github.com/emartech/ems-gcp-toolkit",
    license="MIT",
    author="Emarsys",
    author_email="",
    description="",
    install_requires=[
        "google-cloud-bigquery==1.5.0"
    ],
)