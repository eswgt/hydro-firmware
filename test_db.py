import copy
import json

from datetime import datetime

from google.cloud import bigquery
from google.cloud import pubsub_v1

# TODO(davonprewitt): Specify these in a YAML file.
PROJECT_ID = 'hydroponics-dev'
TOPIC_ID = 'status'
TABLE_NAME = 'sensor_data'

# TODO(davonprewitt): Initialize these in a class.
publisher = pubsub_v1.PublisherClient()
client = bigquery.Client()

status = {
    "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    # TODO(davonprewitt): We will pass this message along to each sensor 
    # and let it populate the JSON message isnstead of explicitly specifying
    # attributes.
    "water_level_m" : 2,
    "pH" : 7,
}

def publish(publisher, status_binary):
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
    ) 

    future = publisher.publish(topic_name, status_binary)
    future.result()

def store(client, status):
    # Format timestamp to specify TIMESTAMP 
    status['timestamp'] = 'TIMESTAMP("{}")'.format(status['timestamp'])

   
    columns = status.keys()
    values = [str(status[column]) for column in columns]
    # Construct a query to insert sensor update.
    QUERY = (
        'insert into `{project_id}.{topic_id}.{table_name}`'
        .format(project_id=PROJECT_ID, 
                topic_id=TOPIC_ID, 
                table_name=TABLE_NAME)
        + ' ({})'.format(','.join(columns))
        + ' values ({})'.format(','.join(values))
    )
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

publish(publisher, json.dumps(status).encode("ascii"))
store(client, status)
