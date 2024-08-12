# Secured Streaming Job
Pyspark application used to stream data from Kafka topic and then put those data to the Kudu table with apropretiate Avro schema, which is stored in Schema Registry.

# Algorithm  
1. First there is created streaming DF with apropretiate kafka configs.
2. As second we are connecting our HTTP kerberized client to Schema Registry API to get schema for topic which should be streamed.
3. Schema is cleaned and prepared to the correct avro format.
4. Streaming DF is deserialized with cleaned schema.
5. As last step the whole dataframe is written to the selected target e.g. kudu, console, parquet etc..


## Usage
1. Create Kafka Topic using CLI or SMM.
2. Create Kudu table via impala-shell or HUE.
```sql
CREATE TABLE hriste_hros.spark_schemas ( id INT, firstname STRING, surname STRING, address STRING, phone INT, PRIMARY KEY(id)) PARTITION BY HASH PARTITIONS 16 STORED AS KUDU;
```
3. Clone the repo, modify the required parametes (topics, schemas, table, servers..)
   
4. get `request_kerberos` module by pip.
```bash
pip install requests_kerberos
```
5. Start the kafka producer e.g. console
6. kinit as user:
   ```bash
   kinit user
   ```
7. Run te PySpark job
```bash
spark3-submit --master yarn --jars /opt/cloudera/parcels/CDH/lib/kudu/kudu-spark3_2.12.jar  --conf spark.yarn.keytab=qshrosjar_sca2.keytab --conf spark.yarn.principal=principal@REAL.COM --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./spark_jaas.conf --conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./spark_jaas.conf --files spark_jaas.conf,user.keytab streaming.py
```
