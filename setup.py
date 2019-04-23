from setuptools import setup, find_packages
from setuptools.command.install import install
from distutils.command.build import build
import subprocess
import os


# https://stackoverflow.com/questions/1754966/how-can-i-run-a-makefile-in-setup-py
class MyInstall(install):
    def run(self):
        install.run(self)


class MyBuild(build):
    def run(self):
        # Need to install it like this to avoid conflict with other packages using trading_communication_protocol
        subprocess.run(['pip3', 'install', '.'], cwd=os.path.abspath(
            './trading_communication_protocol'), check=True)
        build.run(self)

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'pycapnp', 'pyyaml', 'pyzmq'
]

test_requirements = [
    'pytest', 'pylint', 'pylint-json2html', 'pytest-cov', 'pytest-runner'
]

setup(
    name='python_omega_client',
    version='1.1.0',
    description='api wrapper for connecting to Fund3 Omega',
    long_description=readme,
    author="dev-fund3",
    author_email='dev@fund3.co',
    url='https://github.com/fund3/python_omega_client',
    packages=find_packages(),
    install_requires=requirements,
    zip_safe=False,
    keywords='omega',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    cmdclass={'install': MyInstall, 'build': MyBuild},
    include_package_data=True,
    package_data={'': ['*.capnp']}
)
