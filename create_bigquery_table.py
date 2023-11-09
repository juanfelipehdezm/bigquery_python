from google.cloud import bigquery

SERVICE_ACCOUNT_KEY_JSON = r"G:\My Drive\Big Data\BigQuery\credential\coral-sonar-395901-7dd103e03cdc.json"

#build a bigquery client object using the service key
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY_JSON)

#we define the name we wanna ugive to the table
table_name_toCreate = "coral-sonar-395901.felipe_bigquery_py.names_from_py"


#using LoadJobConfig library, we create the schema for the table
#we can do the same either using configui or terminal 
#here is the doc https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
		bigquery.SchemaField("count", "INTEGER")
    ],
	source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1
)


file_path=r"G:\My Drive\Big Data\BigQuery\files\yob1880.txt"

#we open the file and load the into the table
with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_name_toCreate, job_config=job_config)

    job.result()  # Waits for the job to complete.


#use the client object to see the details of the table we just created 
table = client.get_table(table_name_toCreate)
print(
    "Loaded {} rows to {}".format(
        table.num_rows, table_name_toCreate
    )
)