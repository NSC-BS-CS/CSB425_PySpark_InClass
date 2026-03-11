FROM apache/spark:3.5.3
USER root
RUN pip install --no-cache-dir pyspark==3.5.3 pyarrow fastparquet avro-python3
USER spark