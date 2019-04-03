FROM centos:7

USER root
RUN yum -y update && yum -y install yum-utils && yum -y groupinstall development
RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm && \
    yum install -y python36u python36u-pip python36u-devel

RUN ln -s /usr/bin/python3.6 /usr/bin/python3
RUN ln -s /usr/bin/pip3.6 /usr/bin/pip3

# Alternatively, if you have ssh keys on the machine, you can do
# pip3 install git+ssh://git@github.com/fund3/python_omega_client.git
# and skip COPY . /app; RUN pip3 install /app
COPY . /app
RUN pip3 install /app

WORKDIR /app/examples
