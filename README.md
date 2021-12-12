# Data Engineering Interview Challenge (Dec 2021)
## _PySpark App_

A local/dev environment PySpark app that processes JSON events data to compute some specific metrics.

## Prerequisites
- **Docker**: [Install Docker Desktop](https://docs.docker.com/engine/install/).
- **Docker Compose**: [Install Docker Compose](https://docs.docker.com/compose/install/).

## Stack
The Docker environment consists of the following technologies:
- Python 3.9
- PySpark (Spark 3.2.0)
- Pytest
- [Poetry](https://python-poetry.org/) (as Python packaging and dependency management tool)
- [MinIO](https://min.io/download#/docker) (as local S3 mock, serves as the storage for the application)

## What this repo contains
This repo is composed of two main parts:
- **interview-challenge-app:** application source code
- **docker:** development environment

```text
├── interview-challenge-app
│   ├── interview_challenge_app
│   │   ├── common
│   │   │   └── utils.py
│   │   ├── jobs
│   │   │   └── metrics
│   │   │       ├── __init__.py
│   │   │       └── metrics_job.py
│   │   └── main.py
│   ├── dist/
│   ├── poetry.lock
│   ├── pyproject.toml
│   └── tests/
├── interview-challenge.sh
├── docker
│   ├── docker-compose.yml
│   ├── docker.env
│   ├── minio
│   │   ├── data
│   │   │   ├── config/
│   │   │   └── events/
│   │   └── setup.sh
│   └── spark
│       ├── conf
│       │   └── spark-defaults.conf
│       └── Dockerfile
└── README.md
```

## Start development environment
Local development environment can be started by running the command below:
```sh
./interview-challenge.sh start
```
This will start services (containers) declared in the **docker-compose.yml** file which are:
- **spark:** based on Python 3.9 image with layers added such as Spark and Poetry.
- **s3:** based on MinIO server image.
- **s3-setup:** based on MinIO client image to set up the server.

The **s3-setup** service is stopped right after the s3 service is set up, while the **spark** and **s3** services continue running and they serve as the development environment.
**Spark** container can be accessed using the following command:
```sh
docker exec -it $(docker ps --filter name=spark -q) /bin/bash
```
When inside the **Spark** container:
- ```poetry build -f wheel``` to build application wheel package.
- ```poetry shell``` to activate the virtual environment.
- ```poetry run pytest``` to run tests.
- ```pyspark``` to start PySpark in shell mode.

Note that the local application source code (**../interview-challenge-app/**) is synchronised with the **/opt/interview-challenge-app/** directory in the container in both directions.

**MinIO** console can be visited from **http://localhost:9001** (login: **minio** / password: **miniosecret**).
The MinIO local server stores in the interview-challenge bucket:
- Configuration files at /data/config/.
- Input events data at /data/events/.
- Computed metrics data at /data/metrics/.

## Run application (w/ tests)
When the application is run, events data (JSON) is read from the **interview-challenge** bucket (prefix: **data/events)**, processed by the application to compute metrics, then written back to interview-challenge/data/metrics in two different ways :
- **/current** in **CSV** format (latest version).
- **/hist** in **Parquet** format (historical version with year, month, day, and time partitioning).

In order to run the application, go to the interview-challenge directory and run the following command:
```sh
./interview-challenge.sh run
```
After the execution, the containers are not stopped so they can still be accessed interactively for investigation.

## Run tests only
Tests can be run by executing the following commands:
```sh
./interview-challenge.sh run-tests
```
The different containers are stopped right after tests have been run.

## Other available commands
Syntax: ```./interview-challenge.sh [command]```
- ```help```                   Display CLI help.
- ```check-prereqs```          Check if prerequisites are installed (docker and docker-compose).
- ```stop```                   Stop local environment by stopping and removing containers and networks created by start command.
- ```clean```                  Stop and remove containers, networks, volumes, and images created by start command.

## Bonus
If you use VSCode, follow the steps below to use VSCode inside the Spark container as this will help tremendously with the development/testing:

1. Install **Remote - Containers** extension in VSCode.
2. [**Start**](#start-development-environment) development environment.
3. Attach VSCode to the Spark container by either selecting Remote-Containers: Attach to Running Container... from the Command Palette or use the Remote Explorer in the Activity Bar and from the Containers view, select the **Attach to Container** inline action on the container name **spark**.
4. Install Python and Pylance extensions and reload the window if necessary.
5. Add the following configuration to your VSCode workspace settings so that Python/Pylance works correctly with Spark:
```
    {
        "python.analysis.extraPaths": ["/opt/spark/python"]
    }
```
