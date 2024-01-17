# This script downloads the elastic sink connector,
# places it inside the kafka-connect container and
# creates new connector.

connector_url="https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-elasticsearch/versions/14.0.11/confluentinc-kafka-connect-elasticsearch-14.0.11.zip"
connector_name="confluentinc-kafka-connect-elasticsearch-14.0.11.zip"

# creating the connector file
mkdir connectors
# going to the dir connector
cd connectors

# downloads the connector zip
curl -o $connector_name $connector_url

# unzip the connector
unzip $connector_name && rm $_
