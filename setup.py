from setuptools import find_packages
from setuptools import setup

setup(
    name="cardano-sync-tests",
    packages=find_packages(),
    use_scm_version=True,
)
