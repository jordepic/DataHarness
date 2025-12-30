# DataHarness Repository

## Project Overview

In the past few years, we've seen the proliferation of new table formats for analyzing big data. While these
work well enough for many analytical workloads, the role of the lake-house keeps expanding, and more development
work goes into making "hybrid" solutions.

Examples:

- Hybrid kafka/iceberg topics (RedPanda, Streambased, Confluent)
- Various delete semantics in lakehouses (positional/equality/deletion vectors)
- Mixing lakehouses with transactional databases (Mooncake/Crunchy Data)
- Proprietary "HTAP" solutions for hybrid processing (DataBricks, Snowflake)
- Streaming connectors between kafka and iceberg for exactly once processing (Kafka Connect, Flink)

**DataHarness** aims to remove the need for the proprietary technology to fuse data sources,
and let teams bring their own data sources. This should allow you to use the technology best
suited towards your pipeline.

## Key Intentions

- **Unified Access**: One DataHarness table can contain data from many different sources
- **Simplified Insertion**: Single metadata store ensures that you never see data across sources in an inconsistent
  state, eliminating the need to use Flink/Kafka Connect for simple pipelines
- **Multi-Source Support**: Seamlessly integrate Kafka topics, relational databases, data lakes, and more
- **Multi-Query Engine Support**: Currently developing for Spark and Trino, with plans to expand

## What Can Be A Data Source?

For a data system to work as a "source" for a harness table, it must only satisfy one constraint:

- It must be able to provide an API to see state at a prior period of time

Examples:

- In Kafka, you can specify offsets to read between and get the same data back, regardless of if there are new messages
- In Iceberg/YugabyteDB, you can specify a read timestamp to see historic state of a table

What does this mean?

- In their vanilla state, we cannot support MySQL and PostgresSQL as first class citizens of DataHarness
- However, we can support MySQL and PostgresSQL wire-protocol compliant databases like TiDB and YugabyteDB or use
  extensions!

## Creating A DataHarness Table

This example displays how to create a data harness table which contains data from a topic partition
with avro-encoded data, an iceberg table, and a YugabyteDB table. The DataHarness is running on
localhost:50051.

```
      ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
      CatalogServiceGrpc.CatalogServiceBlockingStub stub = CatalogServiceGrpc.newBlockingStub(channel);

      CreateTableRequest createTableRequest = CreateTableRequest.newBuilder()
        .setName(DATA_HARNESS_TABLE)
        .build();

      stub.createTable(createTableRequest);

      KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
        .setTrinoCatalogName("kafka")
        .setTrinoSchemaName("default")
        .setTopicName(TOPIC)
        .setStartOffset(0)
        .setEndOffset(kafkaResult.messageCount)
        .setPartitionNumber(0)
        .setBrokerUrls(BOOTSTRAP_SERVERS)
        .setSchemaType(SchemaType.AVRO)
        .setSchema(kafkaResult.avroSchema)
        .build();

      SourceUpdate kafkaSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setKafkaSource(kafkaSource)
        .build();

      UpsertSourcesRequest.Builder upsertSourcesBuilder = UpsertSourcesRequest.newBuilder()
        .addSources(kafkaSourceUpdate);

      IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
        .setTrinoCatalogName("iceberg")
        .setTrinoSchemaName("default")
        .setTableName(ICEBERG_TABLE_NAME)
        .setReadTimestamp(icebergResult.snapshotId)
        .setSparkCatalogName("gravitino")
        .setSparkSchemaName("default")
        .build();

      SourceUpdate icebergSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setIcebergSource(icebergSource)
        .build();

      upsertSourcesBuilder.addSources(icebergSourceUpdate);

      YugabyteDBSourceMessage yugabyteSource = YugabyteDBSourceMessage.newBuilder()
        .setTrinoCatalogName(NOT_IMPLEMENTED)
        .setTrinoSchemaName(NOT_IMPLEMENTED)
        .setTableName(YUGABYTE_TABLE_NAME)
        .setJdbcUrl(YUGABYTE_JDBC_URL)
        .setUsername(YUGABYTE_USER)
        .setPassword(YUGABYTE_PASSWORD)
        .setReadTimestamp(yugabyteTimestamp)
        .build();

      SourceUpdate yugabyteSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setYugabytedbSource(yugabyteSource)
        .build();

      upsertSourcesBuilder.addSources(yugabyteSourceUpdate);

      stub.upsertSources(upsertSourcesBuilder.build());

      ProtobufSchema protobufSchemaObj = new ProtobufSchema(org.dataharness.test.TestMessage.getDescriptor());
      String protobufSchema = protobufSchemaObj.canonicalString();

      SetSchemaRequest.Builder schemaRequestBuilder = SetSchemaRequest.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setAvroSchema(kafkaResult.avroSchema)
        .setProtobufSchema(protobufSchema);

      schemaRequestBuilder.setIcebergSchema(icebergResult.icebergSchema);

      SetSchemaRequest schemaRequest = schemaRequestBuilder.build();

      stub.setSchema(schemaRequest);
```

## Currently Supported

|                                                              | Apache Spark | Apache Trino |
|--------------------------------------------------------------|--------------|--------------|
| Kafka with Avro Encoded Data (Confluent Schema Registry)     | ✅            | ✅            |
| Kafka with Protobuf Encoded Data (Confluent Schema Registry) |              | ✅            |
| Pulsar with Avro Encoded Data                                |              |              |
| Pulsar with Protobuf Encoded Data                            |              |              |
| Amazon Kinesis with Avro Encoded Data                        |              |              |
| Amazon Kinesis with Protobuf Encoded Data                    |              |              |
| Azure Event Hubs with Avro Encoded Data                      |              |              |
| Azure Event Hubs with Protobuf Encoded Data                  |              |              |
| PostgreSQL (Temporal Tables Extension)                       | ✅            |              |
| YugaByteDB (Postgres compatible)                             | ✅            |              |
| YugaByteDB (Cassandra compatible)                            |              |              |
| CockroachDB (Postgres compatible)                            |              |              |
| TiDB (MySQL compatible)                                      |              |              |
| CockroachDB (Postgres compatible)                            |              |              |
| Google Spanner                                               |              |              |
| Apache Iceberg                                               | ✅            | ✅            |
| Apache Hudi                                                  |              |              |
| Delta Lake                                                   |              |              |
| Duck Lake                                                    |              |              |
| Avro Files                                                   |              |              |
| Parquet Files                                                |              |              |
| Orc Files                                                    |              |              |

Main supported features:

- Transfer data between data sources and atomically update their state in the DataHarness
- Perform a rolling update of schemas and then atomically update the table schema in the DataHarness
- Specify the table's schema in terms of Avro schema, Iceberg schema, or Protocol Buffers schema

Two main features that are not currently supported but we hope to support soon:

- Primary key tables (right now the data from each table source is unioned together)
- Schema evolutions are currently atomic, though tables may temporarily break due to modifying inner fields of complex
  types

## Reading Data Harness Tables From Apache Spark

This repository exposes a spark catalog that you can set up very easily. To do so, create your spark program as follows:

```bash
./bin/spark-sql --packages org.example:dataharness-spark-catalog:1.0-SNAPSHOT
--conf spark.sql.catalog.harness=org.dataharness.spark.DataHarnessCatalog
--conf spark.sql.extensions=org.dataharness.spark.DataHarnessExtension
--conf spark.sql.catalog.harness.data-harness-host=data-harness
--conf spark.sql.catalog.harness.data-harness-port=50051
```

This creates a DataHarness catalog called `harness`. To query it, you can run:
`SELECT * FROM harness.data_harness.<table_name>;` -> Note that `data_harness` is currently the only supported namespace

While the above configuration allows using the DataHarness catalog, it does not import any dependencies that might
be needed to read:

- Avro data
- Protobuf data
- Iceberg data
- And so on...

You can
see [our integration test docker compose file](./dataharness-server/src/test/java/org/dataharness/bootstrap/docker-compose.yaml)
for an example of us runnning with an iceberg catalog, a YugabyteDB table, and a kafka topic with avro-encoded data.

NOTE:

- For iceberg tables, the "spark catalog" and "spark namespace" should be equal to the name of the catalog in spark and
  its namespace
- For kafka topic partitions, spark needs to know the broker URLs for each source, there is no "kafka catalog"
    - It needs to know the schema as well, there is no schema registry support for the time being
- For JDBC tables, the jdbc url, username, password, etc... will all need to be specified in the source

## Reading Data Harness Tables From Apache Trino

TODO

## Contributing Guide

TODO
