from setuptools import setup, find_packages

setup(
    name='pyspark-chispa',
    version='1.0',
    extras_require=dict(tests=['pytest']),
    packages=find_packages(where="src"),
    package_dir={'': 'src'}
)
