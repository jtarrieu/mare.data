# MARE DATA PROJECT

Welcome to the mare data project!

The goal of this project is to help the **Oceanographical Observatory of Banyuls-sur-Mer** to manipulate its IoT data in order to query it faster and to have a simpler architecture for data analysis.

The content of this project can be re-used by anyone who wants to implement a real time streaming pipeline for data analysis.

## Introduction
For this use case we are going to take an example: a buoy which has multiple sensors that send synchronized records each second. There can be as much buoys as we want.

The bash instructions for this project are written to be run on a Ubuntu/Debian machine with a bash terminal.

### The database
The database used is MySQL which is constantly overloaded and has a slow querying time. It is an OLTP (OnLine Transactional Processing) database which is row oriented.

The goal here is to add a second databse to the architecture. An OLAP (OnLine Analytical Processing) database which can perform fast querying on a lot of data. We have chosen **elasticsearch** for multiple reasons:
-   document oriented (can contain metadata)
-   querying vui RESTful API
-   Fast
-   Comes with the elastic stack

### The pipeline
The new pipeline is driven by a messaging system : **Apache Kafka**. It allows fast and secure transmission of messages and makes it easy to add a new sensor. It handles replication, acknowledgement of messages and a lot of other parameters.

### The sensor
The sensors are simulated by **python scripts** that creates data each second, the records contains 4 types of data:
-   Text
-   Numbers
-   Timestamp
-   Geo point

Here an example of a **ndjson** records we are simulating.
```python
{
    "timestamp": "2024-01-21 10:06:04.095931",
    "temperature": 20.9,
    "humidity": 59.22,
    "pressure": 1001.87, 
    "sensor_id": 1,
    "location": {
        "lon": 3.185785,
        "lat": 42.488629
    }
}
```

### Transmission
The records are send to a server via **UDP**. Our IoT data is mainly weather related and has a lot of inertia, the loss of a packet isn't problematic. You can Implement TCP/IP if neccessary but we'll keep it simple for this project.

### Vizualisation
To visualize the real-time data we chose **Kibana** as it is part of the Elastic stack. It is very easy to connect to Elasticsearch and has beautiful graphs and maps.

## The Project
This project comes with a prototype which need docker to be run. Keep in mind that docker is a good thing for a prototype but you might want to use kubernetes or another orchestrator to run this architecture for multiple machines. Each container should be a single isolated machine (potentially replicated).

### Requirements
You need :
-   8 to 16 Gb of RAM
-   Docker engine. Docker is a container management system that allow us to manipulate containers (lightweigh virtual machines) with a specific runtime. The goal of using docker is to isolate a code with its runtime in order to make it run on any machine and/or OS.

#### Install Docker engine
The installation commands of docker are stored in a *shell* file in the *installs* directory. Depending on your machine you might want to use sudo or a user with admin privileges to run the isntallation.
```bash
cd installs
./install_docker.sh
```
If it doesn't work you can follow the steps of the [offical docker engine install doc](https://docs.docker.com/engine/install/)

### Run
First make sure docker is running.
```bash
service docker start
```
output:
```
● docker.service - Docker Application Container Engine
     Loaded: loaded (/lib/systemd/system/docker.service; enabled; preset: enabled)
     Active: active (running) since Fri 2023-10-27 14:08:11 CEST; 2 months 25 days ago
TriggeredBy: ● docker.socket
       Docs: https://docs.docker.com
...
```
Run all the containers:
```bash
docker compose up -d
```
This can take a lot of time if you run it for the first time. Docker has to install all the images and to build some custom images too.

You can check the status of all the containers:
```bash
docker ps -a
```
output (zoom out to see the table correcctly)
```
CONTAINER ID   IMAGE                                                  COMMAND                  CREATED      STATUS                  PORTS                                                           NAMES
d7517576ddd3   confluentinc/cp-kafka-connect:7.2.1                    "/etc/confluent/dock…"   4 days ago   Up 4 days (healthy)     0.0.0.0:8083->8083/tcp, :::8083->8083/tcp, 9092/tcp             kafka-connect
99f4919d87d8   project-udp-client-1                                   "python udp_client.py"   4 days ago   Up 4 days                                                                               udp-client-1
499bf04c7e0f   project-udp-client-2                                   "python udp_client.py"   4 days ago   Up 4 days                                                                               udp-client-2
9bdfe44568a1   project-udp-server                                     "python udp_server.py"   4 days ago   Up 4 days               0.0.0.0:20001->20001/udp, :::20001->20001/udp                   udp-server
292140c905b1   confluentinc/cp-kafka-rest:7.1.3                       "/etc/confluent/dock…"   4 days ago   Up 4 days               0.0.0.0:8082->8082/tcp, :::8082->8082/tcp                       project-kafka-rest-proxy-1
76eec74a57f1   confluentinc/cp-kafka:7.1.3                            "/etc/confluent/dock…"   4 days ago   Up 4 days               0.0.0.0:9092->9092/tcp, :::9092->9092/tcp                       kafka
9e9a74949451   docker.elastic.co/kibana/kibana:8.10.2                 "/bin/tini -- /usr/l…"   4 days ago   Up 4 days               0.0.0.0:5601->5601/tcp, :::5601->5601/tcp                       kibana
b46a470f0704   project-python-extraction                              "python keepalive.py"    4 days ago   Up 4 days                                                                               python-extraction
75947ae3464d   docker.elastic.co/elasticsearch/elasticsearch:8.10.2   "/bin/tini -- /usr/l…"   4 days ago   Up 4 days               0.0.0.0:9200->9200/tcp, :::9200->9200/tcp, 9300/tcp             elasticsearch
7dc183bbdb77   confluentinc/cp-zookeeper:7.1.3                        "/etc/confluent/dock…"   4 days ago   Up 4 days               2888/tcp, 0.0.0.0:2181->2181/tcp, :::2181->2181/tcp, 3888/tcp   project-zookeeper-1
f2dffa43db27   mariadb:latest                                         "docker-entrypoint.s…"   4 days ago   Up 4 days               0.0.0.0:3306->3306/tcp, :::3306->3306/tcp                       maria
a4ee3b9626cc   phpmyadmin                                             "/docker-entrypoint.…"   4 days ago   Up 4 days               0.0.0.0:8080->80/tcp, :::8080->80/tcp                           phpmyadmin
df9ca7f95dde   jupyter/base-notebook                                  "tini -g -- start-no…"   4 days ago   Up 4 days (unhealthy)   0.0.0.0:8888->8888/tcp, :::8888->8888/tcp                       jupyter
```
## Docker commands
Restart a container (empty name to restart all the containers):
```bash
docker restart <name fo the container>
```

Kill and erase a container (empty name to kill all the containers):
```bash
docker compose down <container name>
```

To acces the terminal of a container:
```bash
docker exec -it <container name> bash
```

To see the logs of a container (-f to have interactive logs):
```bash
docker logs <container name> <-f>
```
