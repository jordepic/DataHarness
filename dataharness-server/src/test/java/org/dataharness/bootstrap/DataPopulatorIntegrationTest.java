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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

import java.net.URI;
import java.sql.*;
import java.util.*;

/**
 * Integration test that populates Kafka, YugabyteDB, Iceberg, and DataHarness with test data.
 * <p>
 * Before running this test, ensure that the required services are running by executing:
 * ./start_images.sh
 * <p>
 * This test is idempotent and will clean up existing data before populating new data.
 */
public class DataPopulatorIntegrationTest {
  public static final String NOT_IMPLEMENTED = "";
  private static final Logger logger = LoggerFactory.getLogger(DataPopulatorIntegrationTest.class);
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  private static final String TOPIC = "kafka_avro_test";
  private static final String DATA_HARNESS_TABLE = "bootstrap";
  private static final String ICEBERG_TABLE_NAME = "iceberg_test";
  private static final String YUGABYTE_TABLE_NAME = "yugabyte_test";
  private static final String YUGABYTE_JDBC_URL = "jdbc:postgresql://localhost:5433/yugabyte?sslmode=disable";
  private static final String YUGABYTE_USER = "yugabyte";
  private static final String YUGABYTE_PASSWORD = "";
  private static final String TABLE_SCHEMA = "id INT PRIMARY KEY, name TEXT, address TEXT";
  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String MINIO_ACCESS_KEY = "minioadmin";
  private static final String MINIO_SECRET_KEY = "minioadmin";
  private static final String MINIO_BUCKET = "iceberg-bucket";

  @Test
  public void bootstrapDataHarness() throws Exception {
    deleteKafkaTopic();
    deleteDataHarness();
    deleteYugabyteTable();

    long yugabyteTimestamp = populateYugabyteDB();
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

  private void deleteDataHarness() {
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
        record.put("id", msg.id);
        record.put("name", msg.name);
        record.put("address", msg.address);

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
    System.setProperty("aws.region", "us-east-1");
    
    createMinIOBucket();

    org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "name", Types.StringType.get()),
      Types.NestedField.required(3, "address", Types.StringType.get())
    );

    String icebergSchemaJson = SchemaParser.toJson(icebergSchema);

    Map<String, String> properties = new HashMap<>();
    properties.put("uri", "http://localhost:9001/iceberg");
    properties.put("s3.endpoint", MINIO_ENDPOINT);
    properties.put("s3.access-key-id", MINIO_ACCESS_KEY);
    properties.put("s3.secret-access-key", MINIO_SECRET_KEY);
    properties.put("s3.path-style-access", "true");
    properties.put("s3.region", "us-east-1");

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

    String fileLocation = "s3://iceberg-bucket/data.parquet";
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

    return new IcebergPopulationResult(icebergSchemaJson, snapshotId);
  }

  private void fetchAndValidateSources(CatalogServiceGrpc.CatalogServiceBlockingStub stub) throws Exception {
    LoadTableRequest request = LoadTableRequest.newBuilder()
      .setTableName(DATA_HARNESS_TABLE)
      .build();

    LoadTableResponse response = stub.loadTable(request);
  }

  private void createMinIOBucket() {
    try {
      AwsBasicCredentials credentials = AwsBasicCredentials.create(MINIO_ACCESS_KEY, MINIO_SECRET_KEY);
      S3Client s3Client = S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .endpointOverride(URI.create(MINIO_ENDPOINT))
        .region(Region.US_EAST_1)
        .forcePathStyle(true)
        .build();

      try {
        HeadBucketRequest headRequest = HeadBucketRequest.builder()
          .bucket(MINIO_BUCKET)
          .build();
        s3Client.headBucket(headRequest);
      } catch (Exception e) {
        CreateBucketRequest createRequest = CreateBucketRequest.builder()
          .bucket(MINIO_BUCKET)
          .build();
        s3Client.createBucket(createRequest);
        logger.info("Created MinIO bucket: {}", MINIO_BUCKET);
      }

      s3Client.close();
    } catch (Exception e) {
      logger.warn("Failed to create MinIO bucket: {}", e.getMessage());
    }
  }

  private void deleteYugabyteTable() {
    try (Connection conn = DriverManager.getConnection(YUGABYTE_JDBC_URL, YUGABYTE_USER, YUGABYTE_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + YUGABYTE_TABLE_NAME);
      }
    } catch (Exception e) {
      System.err.println("Failed to delete YugabyteDB table: " + e.getMessage());
    }
  }

  private long populateYugabyteDB() throws Exception {
    try (Connection conn = DriverManager.getConnection(YUGABYTE_JDBC_URL, YUGABYTE_USER, YUGABYTE_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + YUGABYTE_TABLE_NAME + " (" + TABLE_SCHEMA + ")");
      }

      String insertSql = "INSERT INTO " + YUGABYTE_TABLE_NAME + " (id, name, address) VALUES (?, ?, ?)";
      try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        pstmt.setInt(1, 1);
        pstmt.setString(2, "YugabyteAlice");
        pstmt.setString(3, "123 Yugabyte St");
        pstmt.addBatch();

        pstmt.setInt(1, 2);
        pstmt.setString(2, "YugabyteBob");
        pstmt.setString(3, "456 Distributed Ave");
        pstmt.addBatch();

        pstmt.setInt(1, 3);
        pstmt.setString(2, "YugabyteCharlie");
        pstmt.setString(3, "789 Database Ln");
        pstmt.addBatch();

        pstmt.executeBatch();
      }

      long timestamp = 0;
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT (EXTRACT (EPOCH FROM CURRENT_TIMESTAMP)*1000000)::decimal(38,0)");
        if (rs.next()) {
          timestamp = Long.parseLong(rs.getString(1));
        }
      }

      try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
        pstmt.setInt(1, 4);
        pstmt.setString(2, "YugabyteDiana");
        pstmt.setString(3, "321 SQL St");
        pstmt.addBatch();

        pstmt.setInt(1, 5);
        pstmt.setString(2, "YugabyteEve");
        pstmt.setString(3, "654 YSQL Ave");
        pstmt.addBatch();

        pstmt.setInt(1, 6);
        pstmt.setString(2, "YugabyteFrank");
        pstmt.setString(3, "987 Consistency Ln");
        pstmt.addBatch();

        pstmt.executeBatch();
      }

      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = stmt.executeQuery("SELECT id, name, address FROM " + YUGABYTE_TABLE_NAME + " ORDER BY id");
        System.out.println("YugabyteDB records:");
        while (rs.next()) {
          System.out.println("  id=" + rs.getInt("id") + ", name=" + rs.getString("name") + ", address=" + rs.getString("address"));
        }
      }

      return timestamp;
    }
  }

  public static class KafkaTestRecord {

    public int id;
    public String name;
    public String address;

    public KafkaTestRecord(int id, String name, String address) {
      this.id = id;
      this.name = name;
      this.address = address;
    }
  }

  public static class KafkaPopulationResult {

    public String avroSchema;
    public long messageCount;

    public KafkaPopulationResult(String avroSchema, long messageCount) {
      this.avroSchema = avroSchema;
      this.messageCount = messageCount;
    }
  }

  public static class IcebergPopulationResult {

    public String icebergSchema;
    public long snapshotId;

    public IcebergPopulationResult(String icebergSchema, long snapshotId) {
      this.icebergSchema = icebergSchema;
      this.snapshotId = snapshotId;
    }

  }

}
