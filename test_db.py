from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = (
    'insert into `hydroponics-dev.status.sensor_data` values (TIMESTAMP("2017-09-18 01:00:00"), 1.4, 1.3)'
    )
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.name)

