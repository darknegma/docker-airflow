FROM puckel/docker-airflow:latest
LABEL name="airflow-dev.sdc" 

USER root
RUN groupadd --gid 999 docker
ENV AIRFLOW_HOME=/usr/local/airflow


EXPOSE 8080

COPY requirements.txt /
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN mkdir /usr/local/libs
#RUN apt-get update -y
#RUN apt-get install -y libpq-dev python-dev
RUN pip install --upgrade pip
RUN pip install  -r /requirements.txt
COPY libs/ /usr/local/libs/
COPY dags/ /usr/local/airflow/dags/
COPY airflow.cfg /usr/local/airflow/airflow.cfg
RUN ln -s /usr/local/libs/ /usr/local/lib/python3.7/site-packages/libs
#RUN cd /usr/local/lib/python3.7/site-packages/libs
WORKDIR /usr/local/lib/python3.7/site-packages/libs
#RUN python setup.py install
#RUN airflow variables 

