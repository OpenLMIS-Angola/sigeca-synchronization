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
    cd sigeca-synchronization/sigeca_data_import_microservice
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
    "open_lmis_api" : { // Used for sending the new entries to the LMIS database
        "api_url": "https://openlmisapi.example.org/api/",  // URL of the API endpoint
        "username": "lmis_user", // Authorized user 
        "password": "password", // Authorized user password 
        "login_token": "dSFdoi1fb4l6bn16YxhgbxhlbdU=" // Basic token value taken from the client request of the server
    },
    "sigeca_api" : {
        "api_url": "http://exmapleapisiggeca.org/api", // Endpoint used for fetchingsource of truth for facilities
        "headers": { // Headers used for the synchronization 
            "ContentType": "application/json"
        },
        "credentials": {  // Credentials used for authorization of user 
            "username": "username",
            "password": "password"
        }
    },
    "database": { // DB Concetion used for the validating existing facilities in ORM
        "username": "db_user",
        "password": "db_passwd", 
        "host": "localhost", 
        "port": 5432,
        "database": "open_lmis"
    },
    "jdbc_reader": { // PySpark connection details for data validation
        "jdbc_url": "jdbc:postgresql://dbserver.example.org:5432/open_lmis", // Points to db on open_lmis_api
        "jdbc_user": "db_user", // DB User 
        "jdbc_password": "db_passwd", // DB Password 
        "jdbc_driver": "org.postgresql.Driver", // Default driver 
        "log_level": "WARN", // Log level for spark operations 
        "ssh_host": "sshOptionalHost", // SSH Server used when tunneling is required to connect to db 
        "ssh_port": 22, // Port for ssh connection 
        "ssh_user": "ubuntu", // User 
        "ssh_private_key_path": "./private_key", // Relative path to the rsa private key 
        "remote_bind_address": "dbserver.example.org", // Address used to connect to server from ssh server
        "remote_bind_port": 5432,  // Port used on the remote 
        "local_bind_port": 5559 // Port binded to localhost 
    },
    "sync": {
        "interval_minutes": 5 // Job interval in minutes 
    }
}
```

## Running the Application

### Continuous Synchronization

To run the application continuously using Docker Compose:

```bash
docker-compose run app python main.py --run-mode continuous
```


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