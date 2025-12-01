FROM bitnami/spark:3.5

USER root

# Install Python and PostgreSQL client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install pyspark==3.5.0 pandas==2.0.3 psycopg2-binary==2.9.7

# Create scripts directory
RUN mkdir -p /opt/spark/scripts

# Download PostgreSQL JDBC driver
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

USER 1001