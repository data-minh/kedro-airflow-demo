FROM apache/airflow:2.10.4-python3.11


# Cài đặt bổ sung một số thư viện hệ thống

USER root

# Using Java 8
RUN apt-get update && apt-get install -y --no-install-recommends \
    net-tools \
    krb5-user \
    libkrb5-dev \
    gcc \
    # add
    graphviz \
    graphviz-dev \ 
    # Cài đặt java 8
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y temurin-8-jre \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Using java 11
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     net-tools \
#     krb5-user \
#     libkrb5-dev \
#     gcc \
#     graphviz \
#     graphviz-dev \
#     && mkdir -p /etc/apt/keyrings \
#     && curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg \
#     && echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list \
#     && apt-get update \
#     && apt-get install -y temurin-11-jre \
#     && apt-get autoremove -yqq --purge \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# RUN groupadd hadoop && \
#     useradd -m -s /bin/bash -g hadoop ams && \
#     useradd -m -s /bin/bash -g hadoop ambari-qa && \
#     useradd -m -s /bin/bash -g hadoop hdfs && \
#     useradd -m -s /bin/bash -g hadoop httpfs && \
#     useradd -m -s /bin/bash -g hadoop mapred && \
#     useradd -m -s /bin/bash -g hadoop yarn-ats && \
#     useradd -m -s /bin/bash -g hadoop yarn && \
#     useradd -m -s /bin/bash -g hadoop zookeeper 

COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt

USER airflow


# Setup environment for java 8
# ENV AIRFLOW_HOME=/opt/airflow \
#     HADOOP_HOME=/usr/odp/1.2.4.0-102/hadoop \
#     JAVA_HOME=/usr/lib/jvm/temurin-8-jre-amd64
# RUN export PATH=$JAVA_HOME/bin:$PATH \
#     export ARROW_LIBHDFS_DIR=${HADOOP_HOME}/lib/native/ && \
#     export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
#     # export CLASSPATH=`${HADOOP_HOME}/bin/hdfs classpath --glob`

# Setup environment for java 11
ENV AIRFLOW_HOME=/opt/airflow \
    HADOOP_HOME=/usr/odp/1.2.4.0-102/hadoop \
    JAVA_HOME=/usr/lib/jvm/temurin-11-jre-amd64 \
    ARROW_LIBHDFS_DIR=${HADOOP_HOME}/lib/native/ \
    HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# Cài đặt các thư viện bổ sung
RUN pip install --no-cache-dir \
    apache-airflow[apache-hdfs,apache-webhdfs,apache-spark,apache-iceberg,trino]==2.10.4 \
    -r /tmp/requirements.txt 
    # --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt"


