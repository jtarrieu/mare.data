import os
import socket
import datetime
import time
import random
import json
import logging

logging.basicConfig(level=logging.INFO)

def new_udp_client() -> socket.socket:
    """Create a UDP client socket."""
    try:
        return socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    except socket.error as e:
        logging.error(f"Error creating UDP client socket: {e}")
        raise

def send_udp_message(message: str, server_address: str, server_port: int):
    """Send a UDP message."""
    try:
        sock = new_udp_client()
        sock.sendto(message.encode(), (server_address, server_port))
        sock.close()
    except socket.error as e:
        logging.error(f"Error sending UDP message: {e}")

def generate_iot_record() -> str:
    """Generate a random set of IoT data in NDJSON format."""
    central_latitude, central_longitude = 42.480778, 3.176833

    latitude = round(random.uniform(central_latitude - 0.01, central_latitude + 0.01), 6)
    longitude = round(random.uniform(central_longitude - 0.01, central_longitude + 0.01), 6)

    data = {
        "timestamp": str(datetime.datetime.now()),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(40.0, 60.0), 2),
        "pressure": round(random.uniform(1000.0, 1010.0), 2),
        "sensor_id": 1,
        "location": {"lon": longitude, "lat": latitude},
    }

    ndjson_data = json.dumps(data)
    return ndjson_data

def main():
    server_address = 'udp-server'
    server_port = 20001
    logging.info(f"Server address: {server_address}")
    logging.info(f"Server port: {server_port}")

    _iter = 0

    try:
        while True:
            record = generate_iot_record()
            send_udp_message(record, server_address, server_port)
            
            time.sleep(1) # SMALL VALUE TO CREATE A LOT OF RECORDS
            _iter += 1
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"Record number {_iter} at {current_time}")

    except KeyboardInterrupt:
        logging.info("Script terminated by user.")

if __name__ == '__main__':
    main()
