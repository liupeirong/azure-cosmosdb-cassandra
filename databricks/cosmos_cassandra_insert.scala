// Databricks notebook source
// MAGIC %md
// MAGIC This sample attempts to ingest a csv file to Cosmos DB Cassandra API by optimizing parallelism to fully utilize the allocated Cosmos DB throughput
// MAGIC * The cluster should be configured with Cosmos DB Cassandra API connection info as following:
// MAGIC 
// MAGIC ```
// MAGIC spark.cassandra.connection.host {cosmos db account name}.cassandra.cosmosdb.azure.com
// MAGIC spark.cassandra.auth.username {cosmos db account name}
// MAGIC spark.cassandra.auth.password {cosmos db account key}
// MAGIC spark.cassandra.connection.port 10350
// MAGIC spark.cassandra.connection.ssl.enabled true
// MAGIC ```
// MAGIC 
// MAGIC * The cluster must have the following libraries, make sure the versions match the Spark and Scala version of your cluster:
// MAGIC 
// MAGIC ```
// MAGIC com.datastax.spark:spark-cassandra-connector_2.11:2.4.0
// MAGIC com.microsoft.azure.cosmosdb:azure-cosmos-cassandra-spark-helper:1.0.0
// MAGIC ```
// MAGIC 
// MAGIC * The csv file must have 3 columns - integer, double, and float. It uses comma as a separator, and must not have a header row.
// MAGIC 
// MAGIC ```csv
// MAGIC 98, 100.1, -123
// MAGIC 99, 100.2, -78.6
// MAGIC ```
// MAGIC 
// MAGIC * Cosmos DB keyspace and table must be pre-created. For example:
// MAGIC 
// MAGIC ```sql
// MAGIC create keyspace mytestks with cosmosdb_provisioned_throughput = 40000;
// MAGIC CREATE TABLE IF NOT EXISTS mytestks.mytesttbl (tid varint, tidx double, tval float, PRIMARY KEY (tid, tidx)) WITH CLUSTERING ORDER BY (tidx ASC) ;
// MAGIC ```

// COMMAND ----------

import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.microsoft.azure.cosmosdb.cassandra
import org.apache.spark.sql.types._
import java.util.Calendar

spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")

// COMMAND ----------

// DBTITLE 1,Configure parallelism and throughput
// Adjust these numbers based on your cluster size
val num_of_nodes = 2
val num_of_cores_per_node = 4

//Configure parallelism and throughput
//spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "4") //default 1
//spark.conf.set("spark.cassandra.connection.keep_alive_ms", "6000") //default 5000
//spark.conf.set("spark.cassandra.output.throughput_mb_per_sec", "3") //throughput mb/s/core, default 2147483647 (max of integer)
//spark.conf.set("spark.cassandra.output.batch.size.rows", "1") //default auto
spark.conf.set("spark.cassandra.output.concurrent.writes", "1") //Maximum number of batches executed in parallel by a single Spark task

// COMMAND ----------

// DBTITLE 1,Read data and spread into partitions to distribute among cluster nodes and cores
val num_of_partitions = num_of_nodes * num_of_cores_per_node * 2

val schema = new StructType().
      add("tid", IntegerType).
      add("tidx", DoubleType).
      add("tval",FloatType)

val wdf = spark
            .read
            .schema(schema)
            .format("csv")
            .load("/FileStore/tables/test.csv")
            .repartition(num_of_partitions)

// COMMAND ----------

// DBTITLE 1,Check data distribution on Cosmos DB partitions
val stats = wdf.groupBy("tid").count()
display(stats)

// COMMAND ----------

// DBTITLE 1,Import data to Cosmos DB
print ("start at " + Calendar.getInstance().getTime())
wdf.write
  .mode("append")
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "mytesttbl", "keyspace" -> "mytestks"))
  .save()
print ("end at " + Calendar.getInstance().getTime())
