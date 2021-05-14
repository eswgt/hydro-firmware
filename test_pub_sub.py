from google.cloud import pubsub_v1

PROJECT_ID = 'hydroponics-dev'
TOPIC_ID = 'hydroponics-dev'

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT_ID,
    topic=TOPIC_ID,

# TODO(davonprewitt): Replace with JSON for subscribers to consume
future = publisher.publish(topic_name, b'My first message!')
future.result()

