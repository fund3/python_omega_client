FROM python:3.6.7

# Alternatively, if you have ssh keys on the machine, you can do
# pip3 install git+ssh://git@github.com/fund3/python_tes_client.git
# and skip COPY . /app; RUN pip3 install /app
COPY . /app
RUN pip3 install /app

WORKDIR /app/examples
