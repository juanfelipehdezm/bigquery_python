from google.cloud import bigquery

SERVICE_ACCOUNT_KEY_JSON = r"G:\My Drive\Big Data\BigQuery\credential\coral-sonar-395901-7dd103e03cdc.json"

#build a bigquery client object using the service key
#This client object is the one that act as interface and manages the connection
#to bigquery API. This is an important CLASS as it contains all the methods of creating 
#datasets, table, etc. 
#https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_JSON)

dataset_name_to_create = "coral-sonar-395901.felipe_bigquery_py"

dataset = bigquery.Dataset(dataset_name_to_create)
#once we have assing the new dataset, we can now set some attributes which will 
#help us set location,description, expiration time, etc.
#https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.dataset.Dataset
dataset.location = "US"
dataset.description = 'dataset from python'

dataset_ref = client.create_dataset(dataset,timeout = 30)


print(f"created dataset {client.project}.{dataset_ref.dataset_id}")