from setuptools import setup

setup(
   name='kadena_indexer',
   version='1.2.0',
   description='A Kadena chainweb Index',
   author='CryptoPascal',
   packages=['kadena_indexer'],
   install_requires=['aiohttp', 'cachetools', 'easydict', 'orjson', 'portion', 'pymongo', 'PyYAML']
)
