# Spark job image for the lakehouse loaders: Java 21, Python 3.12, Spark 4.1.2 and Iceberg 1.11.0.
# Every download is pinned by digest or checksum, so a rebuild from a clean checkout gives the same image contents.
FROM eclipse-temurin:21-jre-noble@sha256:0c324fbe2e1455c3159184717440f3b52906ef82a39c4a6efc9edb401d31971a

ARG SPARK_VERSION=4.1.2
ARG SPARK_SHA512=490510e13e97da9493302bb33a802574bc417fcc107441419d1a0f87f2b372000db6e26f831a2bf92bc4b18bdf2c9023b1b6f2d021afdc7d6c19c8a11c03e685
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_RUNTIME_SHA512=f4620bb2d20777146a769a8a939a5bed658e1d5708b6a5155e1f6e831c0bfd29bd1f82618dc59bc0f30399ffcb03add3e66656286692dc6c2be94e4e1f4e7479
ARG ICEBERG_AWS_SHA512=bb50f7dc5f36a001efecf15e4d03eff955466f06557261fbacdd7fe0d88113c82e0b9b2af96cd554cef4a1d4c2348fc950270bc3f51cf6bf1e4526f46ce336a0

RUN apt-get update \
    && apt-get install --yes --no-install-recommends python3 python3-venv curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN curl -fsSLo spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && echo "${SPARK_SHA512}  spark.tgz" | sha512sum --check --strict \
    && tar -xzf spark.tgz && rm spark.tgz && mv "spark-${SPARK_VERSION}-bin-hadoop3" spark

RUN base="https://repo1.maven.org/maven2/org/apache/iceberg" \
    && curl -fsSLo "spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
       "${base}/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
    && echo "${ICEBERG_RUNTIME_SHA512}  spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" | sha512sum --check --strict \
    && curl -fsSLo "spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
       "${base}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
    && echo "${ICEBERG_AWS_SHA512}  spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" | sha512sum --check --strict

COPY requirements.txt /opt/requirements.txt
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/requirements.txt

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/venv/bin:/opt/spark/bin:${PATH} \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

# Run as an unprivileged user; the project's code and the read-only AWS profile are mounted at run time.
RUN useradd --create-home --uid 10001 spark
USER spark
WORKDIR /workspace
