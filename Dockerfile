FROM centos:centos7

RUN yum -y update; yum clean all
RUN yum -y install epel-release
RUN yum -y install python
RUN yum -y install python-devel
RUN yum -y install python-pip
RUN pip install -U pip;

RUN mkdir -p /opt/local/wds_ingester
COPY . /opt/local/wds_ingester

RUN pip install -r /opt/local/wds_ingester/requirements.txt

# Set default pythong environment variables
ENV PYTHONIOENCODING=UTF-8

WORKDIR "/opt/local/wds_ingester"
CMD ["/usr/bin/python", "/opt/local/wds_ingester/wds/ingester.py"]