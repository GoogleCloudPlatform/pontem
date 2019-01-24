FROM ubuntu
# bring system up-to-date
RUN apt-get update -qq && \
    apt-get upgrade -qqy

RUN apt-get update

RUN apt-get install -y libpq-dev build-essential python3.6 python3.6-dev python3-pip python3.6-venv
RUN apt-get install -y git

# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

RUN apt-get install -y wget

RUN apt-get install bzip2
RUN apt-get -y update && apt-get install -y mysql-client-5.7
COPY ./pontem/CloudSQLReplicator /replicator
WORKDIR /replicator
RUN python3 setup.py install