ARG SPARK_IMAGE=apache/spark:4.2.0-python3
FROM ${SPARK_IMAGE}

USER root
COPY requirements/runtime.txt /tmp/citibike-runtime-requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/citibike-runtime-requirements.txt

ENV SPARK_HOME=/opt/spark \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/workspace:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip

WORKDIR /workspace
USER spark
ENTRYPOINT []

