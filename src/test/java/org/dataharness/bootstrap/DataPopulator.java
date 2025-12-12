package org.dataharness.bootstrap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dataharness.proto.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;

public class DataPopulator {
  private static final Logger logger = LoggerFactory.getLogger(DataPopulator.class);
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  private static final String TOPIC = "testKafka";
  private static final String DATA_HARNESS_TABLE = "bootstrap";
  private static final String ICEBERG_TABLE_NAME = "testIceberg";

  public long populateKafka() throws Exception {
    SchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 100);

    Schema schema = ReflectData.get().getSchema(TestMessage.class);

    client.register(TOPIC + "-value", schema);
    logger.info("Schema registered for topic: {}", TOPIC);

    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    producerProps.put("key.serializer", StringSerializer.class);
    producerProps.put("value.serializer", KafkaAvroSerializer.class);
    producerProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);

    List<TestMessage> messages = new ArrayList<>();
    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
      messages.add(new TestMessage(1, "Alice"));
      messages.add(new TestMessage(2, "Bob"));
      messages.add(new TestMessage(3, "Charlie"));

      for (TestMessage msg : messages) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", msg.id());
        record.put("name", msg.name());

        producer.send(new ProducerRecord<>(TOPIC, String.valueOf(msg.id()), record));
        logger.info("Sent message: id={}, name={}", msg.id(), msg.name());
      }
      producer.flush();
    }

    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    consumerProps.put("key.deserializer", StringDeserializer.class);
    consumerProps.put("value.deserializer", KafkaAvroDeserializer.class);
    consumerProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);
    consumerProps.put("group.id", "test-group-" + System.currentTimeMillis());
    consumerProps.put("auto.offset.reset", "earliest");
    consumerProps.put("session.timeout.ms", "10000");
    consumerProps.put("fetch.min.bytes", "1");

    try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Collections.singletonList(TOPIC));

      ConsumerRecords<String, GenericRecord> records = consumer.poll(10000);
      logger.info("Read {} messages from topic", records.count());

      for (ConsumerRecord<String, GenericRecord> record : records) {
        GenericRecord value = record.value();
        int id = (int) value.get("id");
        String name = value.get("name").toString();
        logger.info("Read message: id={}, name={}", id, name);
      }
    }

    return messages.size();
  }

  public long populateIceberg(@TempDir Path tempDir) throws Exception {
    logger.info("Using temp directory: {}", tempDir);

    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get())
    );
    logger.info("Created Iceberg schema: {}", icebergSchema);

    Map<String, String> properties = new HashMap<>();
    properties.put("uri", "http://localhost:9001/iceberg");

    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("rest", properties);
    logger.info("Initialized RESTCatalog");

    Namespace namespace = Namespace.of("default");
    if (!catalog.namespaceExists(namespace)) {
      catalog.createNamespace(namespace);
      logger.info("Created namespace: default");
    }

    TableIdentifier tableId = TableIdentifier.of(namespace, ICEBERG_TABLE_NAME);

    if (catalog.tableExists(tableId)) {
      catalog.dropTable(tableId);
      logger.info("Dropped existing table");
    }

    Table table = catalog.createTable(tableId, icebergSchema);
    logger.info("Created table: default.{} with schema: {}", ICEBERG_TABLE_NAME, icebergSchema);

    List<Record> records = new ArrayList<>();

    org.apache.iceberg.data.GenericRecord record1 = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    record1.setField("id", 1);
    record1.setField("name", "Alice");
    records.add(record1);

    org.apache.iceberg.data.GenericRecord record2 = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    record2.setField("id", 2);
    record2.setField("name", "Bob");
    records.add(record2);

    org.apache.iceberg.data.GenericRecord record3 = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    record3.setField("id", 3);
    record3.setField("name", "Charlie");
    records.add(record3);

    logger.info("Created {} records with schema", records.size());
    for (Record record : records) {
      logger.info("Record: id={}, name={}", record.getField("id"), record.getField("name"));
    }

    OutputFile outputFile = table.io().newOutputFile(tempDir.toString() + "/data.parquet");

    FileAppenderFactory<Record> factory = new GenericAppenderFactory(table.schema());
    FileAppender<Record> appender = factory.newAppender(outputFile, FileFormat.PARQUET);

    for (Record record : records) {
      appender.add(record);
    }

    appender.close();

    DataFile dataFile = DataFiles.builder(table.spec())
      .withInputFile(table.io().newInputFile(tempDir + "/data.parquet"))
      .withMetrics(appender.metrics())
      .withFormat(FileFormat.PARQUET)
      .build();

    table.newAppend()
      .appendFile(dataFile)
      .commit();

    logger.info("Successfully wrote {} records to Iceberg table", records.size());
    logger.info("Data written to temp directory: {}", tempDir);

    logger.info("{}", table.currentSnapshot().summary().get("added-records"));

    return table.currentSnapshot().snapshotId();
  }

  @Test
  public void bootstrapDataHarness(@TempDir Path tempDir) throws Exception {
    logger.info("=== Starting Data Harness Bootstrap ===");

    long kafkaMessages = populateKafka();
    logger.info("Kafka population complete");

    long snapshotId = populateIceberg(tempDir);
    logger.info("Iceberg population complete");

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
    CatalogServiceGrpc.CatalogServiceBlockingStub stub = CatalogServiceGrpc.newBlockingStub(channel);

    try {
      CreateTableRequest createTableRequest = CreateTableRequest.newBuilder()
        .setName(DATA_HARNESS_TABLE)
        .build();

      var createTableResponse = stub.createTable(createTableRequest);
      logger.info("Created table '{}': {}", DATA_HARNESS_TABLE, createTableResponse.getMessage());

      KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
        .setTrinoCatalogName("bootstrap-catalog")
        .setTrinoSchemaName("default")
        .setTopicName(TOPIC)
        .setStartOffset(0)
        .setEndOffset(kafkaMessages)
        .setPartitionNumber(0)
        .build();

      UpsertSourceRequest kafkaSourceRequest = UpsertSourceRequest.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setKafkaSource(kafkaSource)
        .build();

      var kafkaSourceResponse = stub.upsertSource(kafkaSourceRequest);
      logger.info("Registered Kafka source: {}", kafkaSourceResponse.getMessage());

      IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
        .setTrinoCatalogName("bootstrap-catalog")
        .setTrinoSchemaName("default")
        .setTableName(ICEBERG_TABLE_NAME)
        .setReadTimestamp(snapshotId)
        .build();

      UpsertSourceRequest icebergSourceRequest = UpsertSourceRequest.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setIcebergSource(icebergSource)
        .build();

      var icebergSourceResponse = stub.upsertSource(icebergSourceRequest);
      logger.info("Registered Iceberg source: {}", icebergSourceResponse.getMessage());

      fetchAndValidateSources(stub);

      logger.info("=== Data Harness Bootstrap Complete ===");
    } finally {
      channel.shutdown();
    }
  }

  private void fetchAndValidateSources(CatalogServiceGrpc.CatalogServiceBlockingStub stub) throws Exception {
    logger.info("=== Fetching and Validating Sources ===");

    FetchSourcesRequest request = FetchSourcesRequest.newBuilder()
      .setTableName(DATA_HARNESS_TABLE)
      .build();

    FetchSourcesResponse response = stub.fetchSources(request);
    logger.info("Fetched sources for table '{}': {} source(s)", DATA_HARNESS_TABLE, response.getSourcesCount());

    int kafkaCount = 0;
    int icebergCount = 0;

    for (TableSourceMessage source : response.getSourcesList()) {
      if (source.hasKafkaSource()) {
        kafkaCount++;
        KafkaSourceMessage kafkaSource = source.getKafkaSource();
        logger.info("Kafka Source {}: topic={}, startOffset={}, endOffset={}",
          kafkaCount, kafkaSource.getTopicName(), kafkaSource.getStartOffset(), kafkaSource.getEndOffset());
      } else if (source.hasIcebergSource()) {
        icebergCount++;
        IcebergSourceMessage icebergSource = source.getIcebergSource();
        logger.info("Iceberg Source {}: table={}, snapshotId={}",
          icebergCount, icebergSource.getTableName(), icebergSource.getReadTimestamp());
      }
    }

    if (kafkaCount > 0 && icebergCount > 0) {
      logger.info("✓ Successfully retrieved both Kafka and Iceberg sources");
    } else {
      logger.warn("✗ Missing expected sources: Kafka={}, Iceberg={}", kafkaCount, icebergCount);
    }
  }
}
