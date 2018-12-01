import os
from setuptools import setup, find_packages
from setuptools.command.install import install
from distutils.command.build import build

capnp_dir = './communication_protocol/'

with open(capnp_dir + 'README.md') as readme_file:
    readme = readme_file.read()

if not os.path.exists(capnp_dir + '__init__.py'):
    with open(capnp_dir + '__init__.py', 'w'):
        pass

setup(
    name='communication_protocol',
    version='1.0',
    description='fund3 communication protocol message definitions',
    long_description=readme,
    author='dev-fund3',
    author_email='dev@fund3.co',
    url='https://github.com/fund3/CommunicationProtocol',
    packages=['communication_protocol'],
    install_requires=['Cython', 'pycapnp'],
    zip_safe=True,
    keywords='capnp',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    # test_suite='tests',
    #tests_require=test_requirements,
    #setup_requires=setup_requirements,
    #cmdclass={'install': MyInstall, 'build': MyBuild},
    include_package_data=True,
    package_data={'': ['*.capnp']}
)
