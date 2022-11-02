FROM python:3.8.5

RUN apt-get update

RUN apt-get install --assume-yes apt-utils

RUN apt-get install -y alien

COPY . /root

RUN /bin/bash -c "source /etc/profile"

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get install -y vim locales

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV LANG=en_US.UTF-8

WORKDIR /usr/orca/app

COPY pip.conf /etc

COPY requirements.txt .

RUN pip install --upgrade pip

ENV DOCKER_SCAN_SUGGEST=false

RUN pip install -r requirements.txt
