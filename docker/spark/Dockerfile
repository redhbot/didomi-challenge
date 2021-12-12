FROM python:3.9

ARG SPARK_VERSION=3.2.0
ARG HADOOP_VERSION=3.2
ARG HADOOP_AWS_VERSION=3.3.1
ARG AWS_SDK_VERSION=1.11.655
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_HOME=/opt/spark

# Install Java
RUN apt-get update \
    && apt-get install openjdk-11-jdk -y

# Install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK_PACKAGE.tgz \
  | tar xz -C /tmp/ \
  && mv /tmp/$SPARK_PACKAGE $SPARK_HOME \
  && chown -R root:root $SPARK_HOME
RUN echo "alias pyspark=${SPARK_HOME}/bin/pyspark" >> ~/.bashrc

# Fetch AWS jars
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar \
  && mv hadoop-aws-$HADOOP_AWS_VERSION.jar $SPARK_HOME/jars \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar \
  && mv aws-java-sdk-bundle-$AWS_SDK_VERSION.jar $SPARK_HOME/jars

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
ENV PATH="${PATH}:/root/.local/bin"

# Copy project Poetry config files
WORKDIR /opt/didomi_challenge
COPY ./docker/spark/Dockerfile ./didomi_challenge/pyproject.toml* ./didomi_challenge/poetry.lock* ./

# Turn off virtualenv creation
# We do not need it in docker since it is already isolated
# RUN poetry config virtualenvs.create false
RUN poetry config virtualenvs.in-project true

# Install app dependencies
# RUN poetry install
