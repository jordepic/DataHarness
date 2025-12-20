package org.dataharness.bootstrap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
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

import java.util.*;

/**
 * Integration test that populates Kafka, Iceberg, and DataHarness with test data.
 * <p>
 * Before running this test, ensure that the required services are running by executing:
 * ./start_images.sh
 * <p>
 * This test is idempotent and will clean up existing data before populating new data.
 */
public class DataPopulatorIntegrationTest {
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  private static final String TOPIC = "kafka_avro_test";
  private static final String DATA_HARNESS_TABLE = "bootstrap";
  private static final String ICEBERG_TABLE_NAME = "iceberg_test";

  @Test
  public void bootstrapDataHarness() throws Exception {
    deleteKafkaTopic();
    deleteDataHarness();

    KafkaPopulationResult kafkaResult = populateKafka();
    IcebergPopulationResult icebergResult = populateIceberg();

    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
    CatalogServiceGrpc.CatalogServiceBlockingStub stub = CatalogServiceGrpc.newBlockingStub(channel);

    try {
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

      IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
        .setTrinoCatalogName("iceberg")
        .setTrinoSchemaName("default")
        .setTableName(ICEBERG_TABLE_NAME)
        .setReadTimestamp(icebergResult.snapshotId)
        .build();

      SourceUpdate icebergSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setIcebergSource(icebergSource)
        .build();

      UpsertSourcesRequest upsertSourcesRequest = UpsertSourcesRequest.newBuilder()
        .addSources(kafkaSourceUpdate)
        .addSources(icebergSourceUpdate)
        .build();

      stub.upsertSources(upsertSourcesRequest);

      ProtobufSchema protobufSchemaObj = new ProtobufSchema(org.dataharness.test.TestMessage.getDescriptor());
      String protobufSchema = protobufSchemaObj.canonicalString();

      SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setAvroSchema(kafkaResult.avroSchema)
        .setIcebergSchema(icebergResult.icebergSchema)
        .setProtobufSchema(protobufSchema)
        .build();

      stub.setSchema(schemaRequest);

      fetchAndValidateSources(stub);
    } finally {
      channel.shutdown();
    }
  }

  private void deleteKafkaTopic() throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    try (AdminClient admin = AdminClient.create(props)) {
      ListTopicsResult topics = admin.listTopics();
      if (topics.names().get().contains(TOPIC)) {
        admin.deleteTopics(Collections.singleton(TOPIC)).all().get();
      }
    } catch (Exception e) {
      // Topic may not exist, continue
    }
  }

  private void deleteDataHarness() throws Exception {
    try {
      ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
      CatalogServiceGrpc.CatalogServiceBlockingStub stub = CatalogServiceGrpc.newBlockingStub(channel);

      try {
        DropTableRequest dropRequest = DropTableRequest.newBuilder()
          .setTableName(DATA_HARNESS_TABLE)
          .build();
        stub.dropTable(dropRequest);
      } finally {
        channel.shutdown();
      }
    } catch (Exception e) {
      // Table may not exist, continue
    }
  }

  public KafkaPopulationResult populateKafka() throws Exception {
    SchemaRegistryClient client = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 100);

    Schema schema = ReflectData.get().getSchema(KafkaTestRecord.class);
    String avroSchemaJson = schema.toString();

    client.register(TOPIC + "-value", schema);

    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    producerProps.put("key.serializer", StringSerializer.class);
    producerProps.put("value.serializer", KafkaAvroSerializer.class);
    producerProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);

    List<KafkaTestRecord> messages = new ArrayList<>();
    try (Producer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
      messages.add(new KafkaTestRecord(1, "KafkaAlice", "123 Kafka St"));
      messages.add(new KafkaTestRecord(2, "KafkaBob", "456 Message Ave"));
      messages.add(new KafkaTestRecord(3, "KafkaCharlie", "789 Topic Ln"));

      for (KafkaTestRecord msg : messages) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", msg.id());
        record.put("name", msg.name());
        record.put("address", msg.address());

        producer.send(new ProducerRecord<>(TOPIC, record));
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

      for (ConsumerRecord<String, GenericRecord> record : records) {
        GenericRecord value = record.value();
      }
    }

    return new KafkaPopulationResult(avroSchemaJson, messages.size());
  }

  public IcebergPopulationResult populateIceberg() throws Exception {

    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "address", Types.StringType.get())
    );

    String icebergSchemaJson = SchemaParser.toJson(icebergSchema);

    Map<String, String> properties = new HashMap<>();
    properties.put("uri", "http://localhost:9001/iceberg");

    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("rest", properties);

    Namespace namespace = Namespace.of("default");
    if (!catalog.namespaceExists(namespace)) {
      catalog.createNamespace(namespace);
    }

    TableIdentifier tableId = TableIdentifier.of(namespace, ICEBERG_TABLE_NAME);

    if (catalog.tableExists(tableId)) {
      catalog.dropTable(tableId);
    }

    Table table = catalog.createTable(tableId, icebergSchema);

    List<Record> records = new ArrayList<>();

    org.apache.iceberg.data.GenericRecord record1 = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    record1.setField("id", 1);
    record1.setField("name", "IcebergAlice");
    record1.setField("address", "123 Iceberg St");
    records.add(record1);

    org.apache.iceberg.data.GenericRecord record2 = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    record2.setField("id", 2);
    record2.setField("name", "IcebergBob");
    record2.setField("address", "456 Snapshot Ave");
    records.add(record2);

    org.apache.iceberg.data.GenericRecord record3 = org.apache.iceberg.data.GenericRecord.create(icebergSchema);
    record3.setField("id", 3);
    record3.setField("name", "IcebergCharlie");
    record3.setField("address", "789 Catalog Ln");
    records.add(record3);

    String fileLocation = "/tmp/data.parquet";
    try {
      table.io().deleteFile(fileLocation);
    } catch (Exception e) {
      // File may not exist
    }

    OutputFile outputFile = table.io().newOutputFile(fileLocation);

    FileAppenderFactory<Record> factory = new GenericAppenderFactory(table.schema());
    FileAppender<Record> appender = factory.newAppender(outputFile, FileFormat.PARQUET);

    for (Record record : records) {
      appender.add(record);
    }

    appender.close();

    DataFile dataFile = DataFiles.builder(table.spec())
      .withInputFile(table.io().newInputFile(fileLocation))
      .withMetrics(appender.metrics())
      .withFormat(FileFormat.PARQUET)
      .build();

    table.newAppend()
      .appendFile(dataFile)
      .commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    try {
      table.io().deleteFile(fileLocation);
    } catch (Exception e) {
      // File cleanup, ignore errors
    }

    return new IcebergPopulationResult(icebergSchemaJson, snapshotId);
  }

  private void fetchAndValidateSources(CatalogServiceGrpc.CatalogServiceBlockingStub stub) throws Exception {
    LoadTableRequest request = LoadTableRequest.newBuilder()
      .setTableName(DATA_HARNESS_TABLE)
      .build();

    LoadTableResponse response = stub.loadTable(request);

    int schemaCount = 0;
    if (response.hasAvroSchema()) {
      schemaCount++;
    }
    if (response.hasIcebergSchema()) {
      schemaCount++;
    }
    if (response.hasProtobufSchema()) {
      schemaCount++;
    }

    for (TableSourceMessage source : response.getSourcesList()) {
      if (source.hasKafkaSource()) {
      } else if (source.hasIcebergSource()) {
      }
    }
  }

  record KafkaTestRecord(int id, String name, String address) {

  }

  record KafkaPopulationResult(String avroSchema, long messageCount) {

  }

  record IcebergPopulationResult(String icebergSchema, long snapshotId) {

  }
}
