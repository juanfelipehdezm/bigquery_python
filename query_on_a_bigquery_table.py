from google.cloud import bigquery

SERVICE_ACCOUNT_KEY_JSON = r"G:\My Drive\Big Data\BigQuery\credential\coral-sonar-395901-7dd103e03cdc.json"

#build a bigquery client object using the service key
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_JSON)

query_to_exec = """
    SELECT * FROM coral-sonar-395901.felipe_bigquery_py.names_from_py LIMIT 100
"""

query_job = client.query(query_to_exec)

print(query_job)
print("success ran")

for row in query_job:
    print(str(row[0]) + "," + str(row[1]) + "," + str(row[2]))