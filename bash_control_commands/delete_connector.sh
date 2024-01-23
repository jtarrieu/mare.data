#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <connector_name>"
    exit 1
fi

# URL to your Kafka Connect REST API
connect_url="http://localhost:8083"

# Extract the connector name from the command line arguments
connector_name="$1"

# Make the request to delete the connector using curl
response=$(curl -s -X DELETE "$connect_url/connectors/$connector_name")

# Print the response
echo "$response"
