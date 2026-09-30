FROM apache/airflow:2.9.3-python3.11

# PySpark needs a JDK. Unlike Windows, Linux needs no winutils.exe /
# HADOOP_HOME hack at all — just Java. python3-venv is needed for the
# isolated virtualenv created below.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless python3-venv procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# A completely separate virtualenv for THIS PROJECT's own dependencies
# (pyspark, dbt, boto3, etc.) — deliberately NOT pip-installed into
# Airflow's own environment.
#
# dbt and pyspark both carry heavy, opinionated dependency trees that
# routinely conflict with Airflow's own pinned versions of shared
# packages (boto3 and SQLAlchemy especially — Airflow uses both
# internally too). Fighting that with an Airflow constraints file is a
# losing game once dbt's own transitive dependencies get involved.
# Isolating everything in its own venv avoids the conflict entirely:
# DAG tasks call this venv's python/dbt binaries explicitly instead of
# whatever's on Airflow's own PATH, so the two dependency trees never
# have to agree with each other.
RUN python3 -m venv /home/airflow/venvs/retailpulse

COPY airflow/requirements-airflow.txt /requirements-airflow.txt
RUN /home/airflow/venvs/retailpulse/bin/pip install --no-cache-dir -r /requirements-airflow.txt