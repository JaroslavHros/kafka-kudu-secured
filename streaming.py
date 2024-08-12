"""
Pyspark Streaming Job
    Author:
        @hrosjar 
    Motivation:
        Testing the streaming usecase with schemas 
"""
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
import json, re
import requests
import subprocess
import os
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
 
# kafka config
topic = "Streaming"
schema_registry_schema = "Streaming"

# kudu config
kudu_masters = "kudu-masters:7051"
kudu_table = "hros_dev.spark_schemas"

# kafka config
kafka_options = {
    "kafka.bootstrap.servers": "kafka-server:9093",  # Use SSL port
    "subscribe": "Streaming",
    "kafka.security.protocol": "SASL_SSL",  # For Kerberos over SSL
    "kafka.sasl.kerberos.service.name": "kafka",
    "kafka.sasl.mechanism": "GSSAPI",
    "kafka.ssl.truststore.location": "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks",
    "spark.kafka.ssl.endpoint.identification.algorithm": "",
    "spark.kafka.sasl.jaas.config":
            'com.sun.security.auth.module.Krb5LoginModule required '
            'useTicketCache=false '
            'renewTicket=true '
            'useKeyTab=true '
            'keyTab="./user.keytab" '
            'principal="user@REALM.COM" '
            'debug=true;',
    "startingOffsets": "earliest"
}

# function to get schema from schema registry API
def get_schema(schema_name):
    # URL of the REST endpoint
    url = "https://schema-registry-server:7790/api/v1/schemaregistry/schemas/"
   

    # Initialize Kerberos authentication
    kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)

    # Make a GET request to the endpoint
    response = requests.get(url+ schema_name  + "/versions/latest/schemaText?branch=MASTER", auth=kerberos_auth, verify=r"/path/to/ca_cert.pem")

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        json_data = response.json()
        print(json_data)
        return json_data   
    else:
        print(f"Failed to retrieve data: {response.status_code} - {response.text}")


 # Regular expression to match only required pattern from gained schema
def clean_schema(schema_str):
    corrected = schema_str.replace("'", '"')
    pattern = re.compile(
        r'({"type":\s*"record",\s*"name":\s*"[^"]+",\s*"fields":\s*\[[^\]]*\]})')
    match = pattern.search(corrected)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid schema string")
      
# create json str from cleaned schema   
def create_json(schema):
    try:
        schema_dict = json.loads(schema)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        raise
    # Create a new dictionary with only the valid Avro schema fields
    correct_schema = {
        "type": schema_dict["type"],
        "name": schema_dict["name"],
        "fields": schema_dict["fields"]
    }
    # Convert the dictionary back to a JSON string
    correct_schema_str = json.dumps(correct_schema, indent=2)
    return correct_schema_str 

# creating spark context 
spark = SparkSession.builder.appName("testSpark").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

# kafka streaming consumer
def spark_consumer():
  df = spark.readStream.format("kafka")\
      .options(**kafka_options)\
      .load()
      
      
      
  # this will print the schema of kafka streaming df
  df.printSchema()     
  string = str(get_schema(schema_registry_schema))

# prepare th schema
  cleaned_schema = clean_schema(string)
  string_json = create_json(cleaned_schema)
  
    # deserialize data 
  fromAvroOptions = {"mode":"PERMISSIVE"}
  decoded_output = df.select(from_avro(func.col("value"), string_json, fromAvroOptions).alias("data"))
  # select only the deserialized data by alias
  value_df = decoded_output.select("data.*")
  value_df.printSchema()  # check the schema before printing 
  

  value_df.printSchema()

  value_df \
    .writeStream \
    .format("org.apache.kudu.spark.kudu") \
    .option("kudu.master", kudu_masters) \
    .option("kudu.table", kudu_table) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .outputMode("append") \
    .start() \
    .awaitTermination() 




# run the job
spark_consumer()
