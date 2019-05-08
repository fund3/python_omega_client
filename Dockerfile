FROM python:3.6.7

ENV PYTHONPATH /app/
USER root

RUN apt-get update && \
    apt-get install -y build-essential curl python3 python3-pip && \
    pip3 install --upgrade pip==9.0.3

# Setting up Python3 packages
WORKDIR /app
COPY requirements.txt /app/requirements.txt
COPY ./omega_protocol /app/omega_protocol
RUN pip3 install -r requirements.txt

COPY . /app

# Install omega_client as local package
RUN pip install .

#ENTRYPOINT ["/sbin/my_init", "python3"]
#CMD ["python3", "-u", "market_data_aggregator.py"]
