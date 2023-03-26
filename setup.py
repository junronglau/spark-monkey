from setuptools import setup, find_packages

setup(
    name='SparkMonkey',
    version='1.01',
    packages=find_packages(['spark_monkey']),
    url='',
    license='',
    author='junrong.lau',
    author_email='junronglau@gmai.com',
    description='Python library to analyse and troubleshoot spark jobs performance',
    install_requires=['pandas', 'tqdm', 'scipy', 'numpy']
)
