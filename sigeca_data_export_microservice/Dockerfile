# Use the official Python image from the Docker Hub
FROM python:3.10-slim

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update and install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    wget \
    gnupg \
    libc-dev \
    gcc \
    software-properties-common \
    libpq-dev \
    default-jdk \
    default-jre \
    file && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Install Python dependencies
COPY requirements.txt .

RUN pip install -r requirements.txt

# Set environment variables
ENV PYTHONPATH=/app

# Copy application code
COPY . /app

# Spark installation
ARG spark_version="3.0.1"
ARG hadoop_version="3.2"
ARG spark_checksum="E8B47C5B658E0FBC1E57EEA06262649D8418AE2B2765E44DA53AAF50094877D17297CC5F0B9B35DF2CEEF830F19AA31D7E56EAD950BBE7F8830D6874F88CFC3C"
ARG openjdk_version="11"

ENV APACHE_SPARK_VERSION="${spark_version}" \
    HADOOP_VERSION="${hadoop_version}"

WORKDIR /tmp
# Using the preferred mirror to download Spark
# hadolint ignore=SC2046
RUN wget -q https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz
RUN tar xzf "spark-3.4.4-bin-hadoop3.tgz" -C /usr/local --owner root --group root --no-same-owner && \
    rm "spark-3.4.4-bin-hadoop3.tgz"

WORKDIR /usr/local

# Configure Spark
ENV SPARK_HOME=/usr/local/spark
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin

RUN ln -s "spark-3.4.4-bin-hadoop3" spark

WORKDIR /app

# Expose any necessary ports (e.g., for Spark UI)
EXPOSE 4040
RUN unset SPARK_HOME
# Define default command
CMD ["python", "main.py", "--run-mode", "continuous"]
