import argparse
import copy
import json
import yaml

from datetime import datetime

from google.cloud import bigquery
from google.cloud import pubsub_v1

def parse():
  args = argparse.ArgumentParser()
  args.add_argument('--config',
                    type=str,
                    help='Passes config filename.')
  return args.parse_args()

# TODO(davonprewitt): Move this to a new file.
class GCPClient:
    def __init__(self, project_id):
        self.publisher = pubsub_v1.PublisherClient()
        self.bq_client = bigquery.Client()
        self.project_id = project_id
         
    def publish(self, topic_id, msg):
        topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=self.project_id,
            topic=topic_id,
        ) 
        future = self.publisher.publish(topic_name, msg)
        future.result()

    def store(self, topic_id, table, msg):
        # Store all keys as columns in table
        columns = msg.keys()
        values = [str(msg[column]) for column in columns]
        # Construct a query to insert sensor update.
        QUERY = (
            'insert into `{project_id}.{topic_id}.{table_name}`'
            .format(project_id=self.project_id, 
                    topic_id=topic_id, 
                    table_name=table)
            + ' ({})'.format(','.join(columns))
            + ' values ({})'.format(','.join(values))
        )
        query_job = self.bq_client.query(QUERY)  # API request
        rows = query_job.result()  # Waits for query to finish

if __name__ == "__main__":
    args = parse()
    config = yaml.load(open(args.config, 'r'))

    # Create Google Cloud Platform (GCP) Client 
    client = GCPClient(config['project_id'])

     # All messages require a timestamp.
    status = {
        "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    status.update({
        # TODO(davonprewitt): We will pass this message along to each sensor 
        # and let it populate the JSON message isnstead of explicitly specifying
        # attributes.
        "water_level_m" : 2,
        "pH" : 7,
    })
     
    # Send data to the PubSub message broker and the BigQuery databases.
    client.publish(config['topic_id'], json.dumps(status).encode("ascii"))
    # Format timestamp to store into database
    msg['timestamp'] = 'TIMESTAMP("{}")'.format(msg['timestamp'])
    client.store(config['topic_id'], config['table_name'], copy.deepcopy(status))
