Data Engineering: Final Project
1. Knowledge required for this project

2. Create a Kafka Topic
Create a kafka topic named "rsvp_your_id" with 2 partitions and 2 replications.
------------------
kafka-topics --create --bootstrap-server ip-172-31-94-165.ec2.internal:9092 --replication-factor 2 --partitions 2 --topic rsvp_petrock
==================
3. Create Kafka Data Source
Use meetup RSVP event as the Kafka data source. Ingest meetup_rsvp event to the Kafka topic, use kafka-console-consumer to verify it.
Meetup provides a streaming API for retrieving the RSVP event.
https://stream.meetup.com/2/rsvps
Hint:
The easiest way is to use kafka-console-producer
Bonus:
Write a Kafka producer and a Kafka consumer using Scala, or Java, or Python to produce/consume events to/from the Kafka topic.
----------------------
//create producer
kafka-console-producer --broker-list ip-172-31-94-165.ec2.internal:9092 ip-172-31-91-232.ec2.internal:9092 ip-172-31-89-11.ec2.internal:9092 --topic rsvp_petrock

//create consumer
kafka-console-consumer --bootstrap-server ip-172-31-94-165.ec2.internal:9092 --topic rsvp_petrock -group group-petrock

//import http data
curl https://stream.meetup.com/2/rsvps | kafka-console-producer --broker-list ip-172-31-94-165.ec2.internal:9092, ip-172-31-91-232.ec2.internal:9092, ip-172-31-89-11.ec2.internal:9092
topic rsvp_petrock
====================== 
4. Write 5 Spark Structured Streaming Job
Write Spark Streaming jobs to process data from Kafka. 
4.1 Save events in HDFS in text(json) format. Use "kafka" source and "file" sink. Set outputMode to "append".

----------------------
df = spark.readStream
	.format ("kafka")
	.option("kafka.bootstrap.servers","ip-172-31-94-165.ec2.internal:9092")
	.option("subscribe","rsvp_petrock")
	.option("startingOffsets","latest")
	.option("failOnDataloss","false")
	.load()

df.isStreaming

dff = df.selectExpr("CAST(value ASS TRING)")

dff.writeStream
	.trigger(Trigger.ProcessingTime("60seconds"))
	.format ("text")
	.option("path","/user/petrock/rsvp/Q41")
	.option("checkpointLocation","/user/petrock/rsvp/spark_streaming/checkpoint")
	.outputMode("append")
	.start()
======================

4.2 Save events in HDFS in parquet format with schema. Use "kafka" source and "file" sink. Set outputMode to "append". 

----------------------
from pyspark.sql import SparkSession
from pyspark.sql functions import *
spark = SparkSession.builder
	.master("yarn")
	.appName("rsvp")
	.config("spark.dynamicAllocation.enabled","false")
	.config("spark.streaming.backpresure.enabled", 100)
	.config("spark.kafka.maxRatePerPartition", 100)
	.config("spark.sql.shuffle.partitions", 12)
	.getOrCreate()

df4 = spark.readStream
	.format ("kafka")
	.option("kafka.bootstrap.servers","ip-172-31-94-165.ec2.internal:9092")
	.option("subscribe","rsvp_petrock")
	.option("startingOffsets","latest")
	.option("failOnDataloss","false")
	.load()

dff4 = df4.select(("value").cast("string"))


# get static schema
staticSchema = spark.read.json("/user/petrock/rsvp")
staticSchema.printSchema

df5 = dff4.select("jason.*")
df5.writeStream
	.trigger(Trigger.ProcessingTime("60seconds"))
	.format ("parquet")
	.option("path","/user/petrock/rsvp/Q42")
	.option("checkpointLocation","/user/petrock/rsvp/spark_streaming/checkpoint2")
	.outputMode("append")
	.start()
======================

4.3 Show how many events are received, display in a 2-minute tumbling window. Show result at 1-minute interval. Use "kafka" source and "console" sink. Set outputMode to "complete". 

----------------------
df.printSchema
df6 = df.select (“timestamp”)
df6.printSchema
df6.writeStream
	.format("console")
	.option("truncate","false")
	.start

#Display tumbling window
df6.groupBy(window(col("timestamp"),"2 minutes"))
	.count()
	.orderBy("window")
	.writeStream
	.trigger(Trigger.ProcessingTime("60 seconds"))
	.queryName("df6")
	.format ("console")
	.outputMode ("complete")
	.option("truncate","false")
	.start()
======================

4.4 Show how many events are received for each country, display it in a sliding window 
   (set windowDuration to 3 minutes and slideDuration to 1 minutes). Show result at 
   1-minute interval. Use "kafka" source and "console" sink. Set outputMode to "complete".

----------------------
from pyspark.sql functions import *
df7 = df.select ("timestamp","value")


df8 = df7.withcolumn("value", col("value").cast(StringType))
df8.printSchema()
df8.writeStream
	.format("console")
	.start()
df9 = df8.select(col("timestamp"), from_json(col("value"),staticSchema.schema).as("data")
df9.printSchema()
df9.isStreaming
df10 = df9.select (col("timestamp"),col("data.*"))
df10.printSchema()
df13 = df10.groupBy(window(col ("timestamp"),"3 minutes","1 minutes"),col ("group.group_country"))
	.count()
	.orderBy ("window")

df13.writeStream
	.queryName ("df13")
	.trigger(Trigger.ProcessingTime (“60 seconds”))
	.format("console")
	.outputMode ("complete")
	.option("truncate", "false")
	.start()
======================

4.5 Use impala to create a KUDU table. Do dataframe transformation to extract information 
and write to the KUDU table. Use "kafka" source and "kudu" sink.

----------------------
create table if not exists rsvp_db.rsvp_kudu_petrock
(
	rsvp_id 		bigint primary key,
	member_id 		bigint,
	member_name 	string,
	group_id		bigint,
	group_name		string,	
	group_city		string,
	group_country	string,
	event_name		string,
	event_time		bigint
)
PARTITION BY HASH (rsvp_id) PARTITIONS 2
STORED AS KUDU;

ssh petrock@254.86.193.122
ssh petrock@ip-172-31-92-98.ec2.internal

spark-shell --master local[2] --repositories "https://repository.cloudera.com/artifactory/cloudera-repos"
import org.apache.spark.sql.streaming.Trigger

dfx = df10.writeStream
	.trigger(Trigger.ProcessingTime ("60 seconds"))
	.format("kudu")
	.option("kudu.master","ip-172-31-89-172.ec2.internal,ip-172-31-86-198.ec2.internal,ip-172-31-93-228.ec2.internal")
	.option("kudu.table","impala::rsvp_db.rsvp_kudu_petrock")
	.option("kudu.operation","upsert")
	.option("path","/user/petrock/rsvp/kudu")
	.option ("checkpointLocation","/user/petrock/rsvp/checkpoint3")
	.outputMode ("append")
	.start()
======================

4.5.1 Use impala query to verify that the streaming job is inserting data correctly.

----------------------
	SELECT * FROM rsvp_db_kudu_petrock LIMIT 10
======================

4.6 Use YARN web console to monitor the job.

----------------------

======================
