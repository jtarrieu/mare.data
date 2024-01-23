import socket
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import requests
from typing import Tuple

logging.basicConfig(level=logging.INFO)


def create_kafka_producer(bootstrap_servers) -> KafkaProducer:
    try:
        return KafkaProducer(bootstrap_servers=bootstrap_servers)
    except Exception as e:
        logging.error(f"Error creating Kafka producer: {e}")
        raise

def create_kafka_elastic_sink_connector(connector_config:dict) -> None:
    connect_url = "http://kafka-connect:8083"  # URL to your Kafka Connect REST API
    # headers for the http requests
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json"
    }
    # Create the connector
    response = requests.post(f"{connect_url}/connectors/",
                            headers=headers,
                            data=json.dumps(connector_config))

    # Check the response
    if response.status_code == 201:
        logging.info(f"Connector created successfully.")
    else:
        logging.error(f"Failed to create connector: {response.status_code} - {response.text}", exc_info=True)

def kafka_elastic_connector_config(index:str, topic:str) -> dict:
    connector_config = {
    "name": index, # name of the connector
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector", # choose your connector
        "tasks.max": "1",  # max number of tasks at the same time
        "topics": topic, # names of the topics "topic-1, topic-2, ..."
        "key.ignore": "true", # should it take the key in consideration or not
        "schema.ignore": "true", # should it ignore the schema ?
        "connection.url": "http://elasticsearch:9200", # url of the elasticsearch service
        "type.name": "_doc", # the identifier to search for a document ex /my-index/_doc/id
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "index.name": index
    }
    }
    return connector_config

def get_meteo_mapping() -> dict:
    mapping = {
    "mappings": {
        "properties": {
            "timestamp": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss.SSSSSS"
            },
            "temperature": {
                "type": "float"
            },
            "humidity": {
                "type": "float"
            },
            "pressure": {
                "type": "float"
            },
            "sensor_id": {
                "type": "integer"
            },
            "location": {
                "type": "geo_point"
            }
        }
    }}
    return mapping

def push_mapping_to_index(mapping:str, index:str, host:str) -> bool:
    # Create the index with mapping
    index_url = f"{host}/{index}"
    logging.info('pushing mapping to index')
    response = requests.put(index_url, json=mapping)
    logging.info(f'pushing response: {response.text}')
    # Check the response status
    if response.status_code == 200:
        logging.info(f"Index '{index}' created successfully.")
        return True
    else:
        logging.error(f"Failed to create index. Status code: {response.status_code}, Response: {response.text}")
        return False

def create_udp_socket(server_address, server_port):
    try:
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind((server_address, server_port))
        return udp_socket
    except Exception as e:
        logging.error(f"Error creating UDP socket: {e}")
        raise

def main():
    logging.info('starting the udp server')
    elastic_url= 'http://elasticsearch:9200' # URL to Elasticsearch REST API
    elastic_index = 'meteo'
    kafka_bootstrap_servers = 'kafka:19092'
    kafka_topic = 'meteo'

    # Pushing the mapping to the index
    meteo_mapping = get_meteo_mapping()
    push_mapping_to_index(meteo_mapping, elastic_index, elastic_url)

    # Creating the kafka sik connector to elastic search
    connector_config = kafka_elastic_connector_config(elastic_index, kafka_topic)
    create_kafka_elastic_sink_connector(connector_config)

    # Create the kafka producer
    # Configuration Kafka
    kafka_bootstrap_servers = 'kafka:19092'
    kafka_topic = 'meteo'

    # Creating the UDP server
    # Configuration UDP
    udp_server_address = '0.0.0.0'
    udp_server_port = 20001

    # Creation of Kafka producer and UDP socket
    producer = create_kafka_producer(kafka_bootstrap_servers)
    udp_socket = create_udp_socket(udp_server_address, udp_server_port)

    logging.info(f'UDP server up and running on {udp_server_address}:{udp_server_port}, forwarding to Kafka topic {kafka_topic}')

    # try:
    while True:
        # Receive a UDP message
        message, address = udp_socket.recvfrom(1024)
        current_time = datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f'Received UDP message from {address} at {formatted_time}')

            # try:
        # Send the message to Kafka
        producer.send(kafka_topic, value=message)
        # logging.info(f'Sent to Kafka topic: {kafka_topic}')
            # except Exception as kafka_error:
            #     logging.error(f"Error sending message to Kafka: {kafka_error}")

    # except KeyboardInterrupt:
    #     logging.info("Script terminated by user.")

    # finally:
        # Close the UDP socket and Kafka producer in case of termination
    udp_socket.close()
    producer.close()

if __name__ == '__main__':
    main()
