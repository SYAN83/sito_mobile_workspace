FROM jupyter/pyspark-notebook
MAINTAINER Shu Yan <yanshu.usc@gmail.com>

RUN conda install -y boto3 

