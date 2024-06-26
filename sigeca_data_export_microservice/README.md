# Sigeca Data Export Microservice

This microservice synchronizes data between a local database and an external API using Apache Spark. It supports both continuous synchronization and one-time integration.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Configuration](#configuration)
- [Building the Docker Image](#building-the-docker-image)
- [Running the Application](#running-the-application)
- [Troubleshooting](#troubleshooting)
- [Logs](#logs)
- [Acknowledgements](#acknowledgements)


## Prerequisites
### Docker 
- Docker and Docker Compose installed on your system
- An external database (e.g., PostgreSQL) accessible from your Docker network

### Loacl run 

- Python 3.10
- Java Runtime Environment (JRE) installed
- Apache Hadoop and Apache Spark
- An external database (e.g., PostgreSQL) accessible from your Docker network

## Setup

1. Clone the repository:

    ```bash
    git clone https://github.com/OpenLMIS-Angola/sigeca-synchronization.git
    cd sigeca-synchronization/sigeca_data_export_microservice
    ```
2. Create and run virtual environment 

    ```bash 
    python3 -m venv venv 
    source venv/bin/activate
    ```
3. Install requirements 

    ```bash
    python install -r requirements.txt
    ```

## Configuration

Create the `config.json` file with your specific settings. It can be created based on the provided `config_example.json`:

```json5
{
    "api": {
        "url": "https://api.example.com",  // URL of the external API to which data will be synchronized
        "token": "your_api_token"          // Authentication token for accessing the external API
    },
    "changelog_database": {
        "username": "user1",               // Username for the database used to store synchronization logs
        "password": "password1",           // Password for the database user
        "host": "localhost",               // Hostname or IP address of the database server
        "port": 5432,                      // Port number on which the database server is listening
        "database": "database"             // Name of the database
    },
    "jdbc_reader": { 
        "jdbc_url": "jdbc:postgresql://localhost:5432/database",  // JDBC URL for connecting to the database
        "jdbc_user": "user1",                                     // Username for the database used by JDBC Reader
        "jdbc_password": "password1",                             // Password for the database user used by JDBC Reader
        "jdbc_driver": "org.postgresql.Driver",                   // JDBC driver class for PostgreSQL
        "log_level": "INFO"                                       // Log level for JDBC operations (e.g., INFO, DEBUG)
    },
    "sync": {
        "changelog_sync_cron": { // Cron expression for the scheduled task to synchronize changelog data
            "minute": "*/5"      // Run every 5 minutes
        },
        "full_sync_cron": {      // Cron expression for the scheduled task to perform a full synchronization
            "hour": "1"          // Run at 1:00 AM
        }
    }
}
```

## Running the Application

### Continuous Synchronization

To run the application continuously using Docker Compose:

```bash
docker-compose run app python main.py --run-mode continuous
```

This will run scheduled task which frequency is based on the `config.json`.
> Note: This require OpenLMIS Instance to have [openLMIS-ChangeLog](https://github.com/OpenLMIS-Angola/openLMIS-Changelog) installed.

### One-time Integration

To perform a one-time integration:

1. Run the application with the `one-time` argument:

```bash
docker-compose run app python main.py --run-mode one-time
```


This will run one time task which will synchronize all available data with external system.

## Troubleshooting

### Common Issues

1. **Network Issues**:
    - Ensure the Docker network is properly set if application is run from separate docker-compose than OpenLMIS. 

2. **Configuration Errors**:
    - Double-check your `config.json` file for accuracy, especially the database connection details.

3. **Dependency Issues**:
    - If you encounter issues with Java or Hadoop dependencies, ensure they are correctly installed and the URLs are correct.

### Logs

Logs are stored in the `logs` volume. You can access them for debugging and monitoring purposes:

```bash
docker-compose logs app
```

## Acknowledgements

- [Apache Spark](https://spark.apache.org/)
- [Apache Hadoop](http://hadoop.apache.org/)
- [Docker](https://www.docker.com/)
- [OpenLMIS-Angola](https://github.com/OpenLMIS-Angola)

For any questions or issues, please open an issue on the [GitHub repository](https://github.com/OpenLMIS-Angola/sigeca-synchronization/issues).