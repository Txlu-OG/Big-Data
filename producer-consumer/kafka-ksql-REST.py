import requests

# Define the ksqlDB server URL
url = 'http://localhost:8088/ksql'  

# Define the headers for the request
headers = {
    'Accept': 'application/vnd.ksql.v1+json',
    'Content-Type': 'application/vnd.ksql.v1+json'
}

# Define the body of the request to create the derived stream
body = {
    "ksql": "CREATE STREAM s2 AS SELECT * FROM s1 EMIT CHANGES;",
    # "ksql": "CREATE STREAM S1_Expanded WITH (KAFKA_TOPIC = 'S1_Expanded', VALUE_FORMAT = 'JSON', PARTITIONS = 4) AS SELECT * FROM S1;",
    "streamsProperties": {
        "ksql.streams.auto.offset.reset": "earliest"
    }
}

# Send the POST request to the ksqlDB server
response = requests.post(url, headers=headers, json=body)

# Print the response
post_response_json = response.json()
print(post_response_json)
