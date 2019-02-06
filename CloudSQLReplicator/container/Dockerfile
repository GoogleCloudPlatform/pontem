FROM ubuntu
ARG CLOUD_SDK_VERSION=229.0.0
ENV CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION

ARG INSTALL_COMPONENTS
RUN apt-get update -qqy && apt-get install -qqy \
        gcc \
        apt-transport-https \
        lsb-release \
        openssh-client \
        gnupg

RUN apt-get install -y libpq-dev build-essential python3.6 python3.6-dev python3-pip python3.6-venv

# update pip
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel

RUN apt-get install -y curl

RUN apt-get install bzip2
RUN apt-get -y update && apt-get install -y mysql-client-5.7
RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && apt-get install -y google-cloud-sdk=${CLOUD_SDK_VERSION}-0 $INSTALL_COMPONENTS && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image && \
    gcloud --version

COPY ./CloudSQLReplicator /replicator
WORKDIR /replicator
RUN python3 setup.py install
VOLUME ["/root/.config"]
