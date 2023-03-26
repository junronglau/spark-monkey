from setuptools import setup, find_packages

setup(
    name='SparkMonkey',
    version='1.01',
    packages=['spark_monkey'],
    url='',
    license='',
    author='junrong.lau',
    author_email='junronglau@gmai.com',
    description='Python library to analyse and troubleshoot spark jobs performance',
    packages=find_packages(include=['spark_monkey', 'spark_monkey.*']),
    install_requires=['pandas', 'tqdm', 'scipy', 'numpy']
)
