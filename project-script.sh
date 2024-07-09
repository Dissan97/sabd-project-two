lines="-------------------------------------------"
set -e

build(){
	echo -e "$lines\nBuilding image for prometheus\n$lines"
	docker build prometheus -t flink-prometheus
	echo -e "$lines\ndone....\n$lines"
}

start_compose(){
    docker compose up -d
    if [ "$(id -u)" -eq 0 ]; then
    chmod 0777 cluster-producers/data-sources  
    chmod 0777 results/* 
    chmod 0777 jars
    chmod 0777 tmp/dspflink/checkpoint
    fi
}

install_jars(){
	echo -e "$lines\npackage kafka-producer\n$lines"
	mvn -f cluster-producers/kafka-support/ clean package
	echo -e "$lines\npackage DSP-flink package\n$lines"
	mvn -f DSP-Flink/ clean package
	echo -e "$lines\nbuilded all the jars\n$lines"
    echo -e "$lines\nCan be foun on folder jars\n$lines"
}

remove_jars(){
    echo -e "$lines\nRemoving jars\n$lines"
	mvn -f cluster-producers/kafka-support/ clean
	mvn -f DSP-Flink/ clean
    rm -rf jars/*
    
}

run_jars(){
    if [ -z "$1" ]; then
    
    java -jar jars/AppKafka.jar produce cluster-producers/data-sources/disk-failures cluster &
    PID1=$!
    sleep 1
    docker compose exec jobmanager flink run -m jobmanager:8081 -c dissanuddinahmed.FlinkStream /jars/FlinkStream.jar &
    PID2=$!
    wait $PID1
    wait $PID2
    else
        case "$1" in 
            flink)
            ;;
            produce)
            ;;
        esac 
    fi
}

case "$1" in
	--build)
	build	;;
	--start-compose)
    start_compose
	;;
	--install-jars)
	install_jars
	;;
    --remove-jars)
	remove_jars
	;;
    --run-jar)
    run_jars $2 $3 $4
    ;;
	--help|--h|--man)
	cat script-man/man_run
	;;
	*)
	echo -e "Usage: $0 {--build|--start-compose|--install-jars|--remove-jars|--run-jar}\nfor all the args type --help or -h for info"
	;;
esac
