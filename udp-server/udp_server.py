import socket
from kafka import KafkaProducer
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def create_kafka_producer(bootstrap_servers):
    try:
        return KafkaProducer(bootstrap_servers=bootstrap_servers)
    except Exception as e:
        logging.error(f"Error creating Kafka producer: {e}")
        raise

def create_udp_socket(server_address, server_port):
    try:
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind((server_address, server_port))
        return udp_socket
    except Exception as e:
        logging.error(f"Error creating UDP socket: {e}")
        raise

def main():

    # Create the kafka producer and the UDP socket
    try:
        # Configuration Kafka
        kafka_bootstrap_servers = 'kafka:19092'
        kafka_topic = 'meteo'

        # Configuration UDP
        udp_server_address = '0.0.0.0'
        udp_server_port = 20001

        # Creation of Kafka producer and UDP socket
        producer = create_kafka_producer(kafka_bootstrap_servers)
        udp_socket = create_udp_socket(udp_server_address, udp_server_port)

        logging.info(f'UDP server up and running on {udp_server_address}:{udp_server_port}, forwarding to Kafka topic {kafka_topic}')
    except Exception as e:
        logging.error(f"Error Creating the kafka producer and/or the udp socket: {e}.")

    try:
        while True:
            # Receive a UDP message
            message, address = udp_socket.recvfrom(1024)
            current_time = datetime.now()
            formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f'Received UDP message from {address} at {formatted_time}')

            try:
                # Send the message to Kafka
                producer.send(kafka_topic, value=message)
                # logging.info(f'Sent to Kafka topic: {kafka_topic}')
            except Exception as kafka_error:
                logging.error(f"Error sending message to Kafka: {kafka_error}")

    except KeyboardInterrupt:
        logging.info("Script terminated by user.")
    
    finally:
        # Close the UDP socket and Kafka producer in case of termination
        udp_socket.close()
        producer.close()

if __name__ == '__main__':
    main()
