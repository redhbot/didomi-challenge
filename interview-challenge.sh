#!/bin/sh

display_help() {
   echo "****************************************"
   echo "          Interview Challenge"
   echo "****************************************"
   echo 
   echo "Syntax: ./interview-challenge.sh [command]"
   echo
   echo "----------------commands----------------"
   echo "help                   Display CLI help."
   echo "check-prereqs          Check if prerequisites are installed (docker and docker-compose)."
   echo "start                  Start local environment by building and (re)creating containers for all services."
   echo "run                    In addition to what start command does, run the application inside the Spark container using spark-submit."
   echo "run-tests              In addition to what start command does, run application tests and stop all services."
   echo "stop                   Stop local environment by stopping and removing containers and networks created by start command."
   echo "clean                  Stop and remove containers, networks, volumes, and images created by start command."
}

check_prereqs() {
   docker -v >/dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo "docker is not installed."
   else
      echo "docker is installed."
   fi

   docker-compose -v >/dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo "docker-compose is not installed."
   else
      echo "docker-compose is installed."
   fi
}

case "$1" in
check-prereqs)
   check_prereqs
   ;;
start)
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" up
   ;;
run)
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" up -d
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" exec spark /bin/bash -c \
      'poetry install \
      && poetry run pytest \
      && poetry run /opt/spark/bin/spark-submit ./interview_challenge_app/main.py'
   ;;
run-tests)
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" up -d
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" exec spark /bin/bash -c \
      'poetry install \
      && poetry run pytest'
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" down
   ;;
stop)
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" down
   ;;
clean)
   docker-compose -f ./docker/docker-compose.yml -p "interview-challenge" down --volumes --rmi all --remove-orphans
   ;;
help)
   display_help
   ;;
*)
   echo "No command specified, displaying help."
   display_help
   ;;
esac
