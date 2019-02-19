# Java sample that measures Request Unit for specific queries and runs load test on Azure Cosmos DB Cassandra API
This sample lets you run custom CQL queries and outputs the Azure Cosmos DB Request Units consumed by each query.  It can also run a stress test by importing a csv in multiple threads. 

## How to run simple queries and measure Request Unit
1. Set your Cosmos DB Cassandra connection information in [config.properties](src/main/resources/config.properties).
2. Configure [query.properties](src/main/resources/query.properties) for creating a keyspace, table, and the queries you want to run.
3. Run `mvn clean package` from the project root folder. This will generate an uber jar `cosmosdb-cassandra-test.jar` under `target` folder.
4. Run `java -jar target/cosmosdb-cassandra-test.jar` and observe console output.

## How to run stress test
The stress test is less configurable. 
1. It uses DataStax Cassandra driver annotation in [RecordToInsert.java](src/main/java/com/paigeliu/cosmoscassandra/RecordToInsert.java).  The keyspace name is `mytestks` and the table name is `mytesttbl`. You must create the keyspace and the table first. You can use the run simple query option described above to create them. The table must be of the following schema:
```sql
tid varint
tidx double 
tval float
```
2. Create or generate a csv file with 3 columns mapping to the above schema. The csv file must use comma "," as separator, and must not have header. Here's a [sample data file](data/sample.csv).
3. Configure the csv file path in [query.properties](src/main/resources/query.properties). 
4. You can also set the number of threads that will simultaneously insert data into Cosmos DB, and the max number of retries for each insert upon throttling in [config.properties](src/main/resources/config.properties).
5. Run `mvn clean package` from the project root folder. This will generate an uber jar `cosmosdb-cassandra-test.jar` under `target` folder.
6. Run `java -jar target/cosmosdb-cassandra-test.jar --load`. The app will print out the duration of the test at the end. Meanwhile, you can also observe throughput and error in Azure portal, for example:
![Alt text](/images/cosmos128.GIF?raw=true "Cosmos DB metrics")

## How to calculate how much Request Units I need
TODO