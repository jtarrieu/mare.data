#!/bin/bash

# Check if the number of arguments is correct
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <topic>"
    exit 1
fi

# Assign the first argument to the variable 'topic'
topic="$1"

# Execute the Docker command with the specified topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic "$topic"
