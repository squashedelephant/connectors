
from setuptools import setup
from os import walk
from os.path import exists, join

packages = [d for d,_,__ in walk('connectors') if exists(join(d,'__init__.py'))]

setup(
    version='0.1.0',
    name='connectors',
    description='libraries shared by micro services to access remote services',
    maintainer='squashedelephant',
    maintainer_email='squashedelephant@gmail.com',
    url=None,
    packages=packages,
    include_package_data=True,
    install_requires=[
        'boto3==1.2.2',
        'botocore==1.3.30',
        'cassandra-driver==3.4.1',
        'elasticsearch==2.3.0',
        'ujson==1.33',
        'PyJWT==1.4.0',
        'PyYAML==3.11'
    ]
)
