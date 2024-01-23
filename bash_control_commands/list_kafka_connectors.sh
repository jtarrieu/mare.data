#!/bin/bash

# URL to your Kafka Connect REST API
response=$(curl -s "http://localhost:8083/connectors")

echo $response

