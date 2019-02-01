# This Dockerfile does not work on its own. This is just to illustrate
# installation and setup on AWS Linux 1 instances. You will have to install
# gcc 4.8+ for this to work.
FROM centos:6

USER root

RUN yum -y update && yum -y install yum-utils && yum -y groupinstall development
RUN yum install -y https://centos6.iuscommunity.org/ius-release.rpm && \
    yum install -y python36u python36u-pip python36u-devel

RUN ln -s /usr/bin/python3.6 /usr/bin/python3
RUN ln -s /usr/bin/pip3.6 /usr/bin/pip3

# Alternatively, if you have ssh keys on the machine, you can do
# pip3 install git+ssh://git@github.com/fund3/python_tes_client.git
# and skip COPY . /app; RUN pip3 install /app
COPY . /app
RUN pip3 install /app

WORKDIR /app/examples
