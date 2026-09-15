FROM apache/airflow:3.3.1

USER root

# JDK 설치 (apt)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME 환경변수 설정
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# requirements.txt 복사 및 설치
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
