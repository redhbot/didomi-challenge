#!/bin/sh

display_help() {
   # Display Help
   echo "***************************************"
   echo "          Didomi Challenge"
   echo "***************************************"
   echo 
   echo "Syntax: ./docker/run.sh [command]"
   echo
   echo "---------------commands---------------"
   echo "help                   Print CLI help."
   echo "start                  Start..."
   echo "stop                   Stop..."
   echo "clean                  Stop and remove..."
   echo "check-prereqs          Check pre-reqs installed (docker and docker-compose)."
}

check_prereqs() {
   docker -v >/dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo -e "'docker' is not installed."
   else
      echo -e "'docker' is installed."
   fi

   docker-compose -v >/dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo -e "'docker-compose' is not installed."
   else
      echo -e "'docker-compose' is installed."
   fi
}

case "$1" in
check-prereqs)
   check_prereqs
   ;;
build)
   docker-compose -f ./docker/docker-compose.yml build spark
   ;;
start)
   docker-compose -f ./docker/docker-compose.yml -p "didomi-challenge" up
   ;;
stop)
   docker-compose -f ./docker/docker-compose.yml -p "didomi-challenge" down
   ;;
clean)
   docker-compose -f ./docker/docker-compose.yml -p "didomi-challenge" down --volumes --remove-orphans
   ;;
help)
   display_help
   ;;
*)
   echo "No command specified, displaying help."
   display_help
   ;;
esac
