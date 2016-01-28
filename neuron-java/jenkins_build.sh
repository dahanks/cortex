# used to check error code of grep without exiting in jenkins
container_name=$1
status=0 && docker ps -a | grep $container_name || status=1
if [ $status -eq 0 ]; then
    docker rm -f $container_name
fi
make
make run
