FROM ubuntu:latest
RUN apt-get update
RUN apt-get install git libmysqlclient-dev locales openjdk-8-jre-headless python3-pip -y
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen
RUN locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV PYSPARK_PYTHON /usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON python3
RUN pip3 install pipenv
ADD . /css
WORKDIR /css
RUN pipenv install --system --deploy --ignore-pipfile