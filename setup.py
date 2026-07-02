from setuptools import find_packages, setup


setup(
    name="pronabec-cloud-bi-platform",
    version="0.1.0",
    description="PRONABEC Cloud BI Platform pipelines",
    packages=find_packages(include=["pipelines", "pipelines.*"]),
    include_package_data=True,
)
