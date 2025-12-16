FROM flink:1.17-java11

# Enable S3 Plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Prometheus Plugin (Copy to lib to fix classloading)
RUN cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-*.jar /opt/flink/lib/

# Prometheus Plugin (Verified already in plugins, but ensure dir exists if needed)
# It is usually at /opt/flink/plugins/metrics-prometheus/
