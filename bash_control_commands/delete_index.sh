#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <index name>"
    exit 1
fi

# URL to your elasticsearch REST API
connect_url="http://localhost:9200"

# Extract the index name from the command line arguments
index_name="$1"

# Make the request to delete the index using curl
response=$(curl -s -X DELETE "$connect_url/$index_name")

# Print the response
echo "$response"
