FROM python:3.9

ARG SPARK_VERSION=3.2.3
ARG HADOOP_VERSION=3.2
ARG HADOOP_AWS_VERSION=3.3.1
ARG AWS_SDK_VERSION=1.11.655
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

ENV SPARK_HOME=/opt/spark

# Install Java
RUN apt-get update \
    && apt-get install openjdk-11-jdk -y

# Install Spark
RUN wget -qO- https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/$SPARK_PACKAGE.tgz \
  | tar xz -C /tmp/ \
  && mv /tmp/$SPARK_PACKAGE $SPARK_HOME \
  && chown -R root:root $SPARK_HOME
ENV PYTHONPATH "${SPARK_HOME}/python/:${PYTHONPATH}"
ENV PYTHONPATH "${SPARK_HOME}/python/lib/py4j-0.10.9.2-src.zip:${PYTHONPATH}"
RUN echo "alias pyspark=${SPARK_HOME}/bin/pyspark" >> ~/.bashrc
RUN echo "alias spark-submit=${SPARK_HOME}/bin/spark-submit" >> ~/.bashrc

# Fetch AWS jars
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar \
  && mv hadoop-aws-$HADOOP_AWS_VERSION.jar $SPARK_HOME/jars \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar \
  && mv aws-java-sdk-bundle-$AWS_SDK_VERSION.jar $SPARK_HOME/jars

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
ENV PATH="${PATH}:/root/.local/bin"

# Copy project Poetry config files
WORKDIR /opt/interview-challenge-app
COPY ./docker/spark/Dockerfile ./interview-challenge-app/pyproject.toml* ./interview-challenge-app/poetry.lock* ./

# Turn off virtualenv creation
# RUN poetry config virtualenvs.create false
# Put .venv inside project
RUN poetry config virtualenvs.in-project true

# Install app dependencies
RUN poetry install --no-root
