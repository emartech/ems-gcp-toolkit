from setuptools import setup

setup(
    name="ems-gcp-toolkit",
    version="0.1.6",
    packages=["bigquery", "job", "config"],
    url="https://github.com/emartech/ems-gcp-toolkit",
    license="MIT",
    author="Emarsys",
    author_email="",
    description="",
    install_requires=[
        "google-cloud-bigquery==1.5.0"
    ],
)
