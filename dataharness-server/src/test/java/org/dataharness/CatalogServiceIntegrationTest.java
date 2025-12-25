// Copyright (c) 2025
package org.dataharness;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.dataharness.db.HibernateSessionManager;
import org.dataharness.proto.*;
import org.dataharness.server.GrpcServer;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CatalogServiceIntegrationTest {
  private static PostgreSQLContainer<?> postgres;
  private static GrpcServer grpcServer;
  private static ManagedChannel channel;
  private CatalogServiceGrpc.CatalogServiceBlockingStub stub;

  @BeforeAll
  public static void setUpClass() throws IOException {
    postgres = new PostgreSQLContainer<>("postgres:15").withDatabaseName("dataharness").withUsername("postgres")
      .withPassword("postgres");
    postgres.start();

    System.setProperty("hibernate.connection.url",
      "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getFirstMappedPort() + "/dataharness");
    System.setProperty("hibernate.connection.username", "postgres");
    System.setProperty("hibernate.connection.password", "postgres");
    System.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    System.setProperty("grpc.server.port", "50052");

    grpcServer = new GrpcServer();
    grpcServer.start();

    channel = ManagedChannelBuilder.forAddress("localhost", 50052).usePlaintext().build();
  }

  @AfterAll
  public static void tearDownClass() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
    }
    if (grpcServer != null) {
      grpcServer.stop();
    }
    if (postgres != null) {
      postgres.stop();
    }
  }

  @BeforeEach
  public void setUp() {
    clearDatabase();
    try {
      if (stub != null) {
        channel.shutdown();
      }
      channel = ManagedChannelBuilder.forAddress("localhost", 50052).usePlaintext().build();
    } catch (Exception e) {
      e.printStackTrace();
    }
    stub = CatalogServiceGrpc.newBlockingStub(channel);
  }

  private void clearDatabase() {
    try (Session session = HibernateSessionManager.getSession()) {
      session.beginTransaction();

      Query<?> deleteKafkaSources = session.createQuery("DELETE FROM KafkaSourceEntity");
      deleteKafkaSources.executeUpdate();

      Query<?> deleteIcebergSources = session.createQuery("DELETE FROM IcebergSourceEntity");
      deleteIcebergSources.executeUpdate();

      Query<?> deleteYugabyteSources = session.createQuery("DELETE FROM YugabyteSourceEntity");
      deleteYugabyteSources.executeUpdate();

      Query<?> deleteTables = session.createQuery("DELETE FROM DataHarnessTable");
      deleteTables.executeUpdate();

      session.getTransaction().commit();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCreateTable() {
    CreateTableRequest request = CreateTableRequest.newBuilder().setName("test responsetable response1").build();

    CreateTableResponse response = stub.createTable(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Table created successfully");
    assertThat(response.getTableId()).isNotEqualTo(0);
  }

  @Test
  public void testCreateTableWithDuplicateName() {
    CreateTableRequest request1 = CreateTableRequest.newBuilder().setName("duplicate responsetable").build();

    CreateTableResponse response1 = stub.createTable(request1);
    assertThat(response1.getSuccess()).isTrue();

    CreateTableRequest request2 = CreateTableRequest.newBuilder().setName("duplicate responsetable").build();

    assertThatThrownBy(() -> stub.createTable(request2)).isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void testUpsertKafkaSource() {
    String tableName = "test responsetable responsekafka";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder().setTrinoCatalogName("trino responsecatalog")
      .setTrinoSchemaName("public").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
      .setTopicName("test responsetopic")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    SourceUpdate sourceUpdate = SourceUpdate.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource).build();

    UpsertSourcesRequest request = UpsertSourcesRequest.newBuilder().addSources(sourceUpdate).build();

    UpsertSourcesResponse response = stub.upsertSources(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Sources upserted successfully");
  }

  @Test
  public void testUpsertIcebergSource() {
    String tableName = "test responsetable responseiceberg";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    String trinocatalog = "trino responsecatalog";
    String trinoschema = "public";
    String icebergTableName = "iceberg responsetable";
    long readTimestamp = System.currentTimeMillis();
    String sparkCatalogName = "spark responsecatalog";
    String sparkSchemaName = "spark responseschema";

    IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
      .setTrinoCatalogName(trinocatalog)
      .setTrinoSchemaName(trinoschema)
      .setTableName(icebergTableName)
      .setReadTimestamp(readTimestamp)
      .setSparkCatalogName(sparkCatalogName)
      .setSparkSchemaName(sparkSchemaName)
      .build();

    SourceUpdate sourceUpdate = SourceUpdate.newBuilder().setTableName(tableName)
      .setIcebergSource(icebergSource).build();

    UpsertSourcesRequest request = UpsertSourcesRequest.newBuilder().addSources(sourceUpdate).build();

    UpsertSourcesResponse response = stub.upsertSources(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Sources upserted successfully");

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse loadResponse = stub.loadTable(loadRequest);

    assertThat(loadResponse.getSourcesCount()).isEqualTo(1);
    assertThat(loadResponse.getSourcesList().get(0).hasIcebergSource()).isTrue();

    IcebergSourceMessage loadedSource = loadResponse.getSourcesList().get(0).getIcebergSource();
    assertThat(loadedSource.getTrinoCatalogName()).isEqualTo(trinocatalog);
    assertThat(loadedSource.getTrinoSchemaName()).isEqualTo(trinoschema);
    assertThat(loadedSource.getTableName()).isEqualTo(icebergTableName);
    assertThat(loadedSource.getReadTimestamp()).isEqualTo(readTimestamp);
    assertThat(loadedSource.getSparkCatalogName()).isEqualTo(sparkCatalogName);
    assertThat(loadedSource.getSparkSchemaName()).isEqualTo(sparkSchemaName);
  }

  @Test
  public void testUpsertYugabyteDBSource() {
    String tableName = "test responsetable responseyugabyte";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    String trinocatalog = "trino responsecatalog";
    String trinoschema = "public";
    String tableNameYugabyte = "yugabyte responsetable";
    String jdbcUrl = "jdbc:postgresql://localhost:5433/yugabyte?sslmode=disable";
    String username = "yugabyte";
    String password = "";
    long readTimestamp = System.currentTimeMillis() * 1000;

    YugabyteDBSourceMessage yugabyteSource = YugabyteDBSourceMessage.newBuilder()
      .setTrinoCatalogName(trinocatalog)
      .setTrinoSchemaName(trinoschema)
      .setTableName(tableNameYugabyte)
      .setJdbcUrl(jdbcUrl)
      .setUsername(username)
      .setPassword(password)
      .setReadTimestamp(readTimestamp)
      .build();

    SourceUpdate sourceUpdate = SourceUpdate.newBuilder().setTableName(tableName)
      .setYugabytedbSource(yugabyteSource).build();

    UpsertSourcesRequest request = UpsertSourcesRequest.newBuilder().addSources(sourceUpdate).build();

    UpsertSourcesResponse response = stub.upsertSources(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Sources upserted successfully");

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse loadResponse = stub.loadTable(loadRequest);

    assertThat(loadResponse.getSourcesCount()).isEqualTo(1);
    assertThat(loadResponse.getSourcesList().get(0).hasYugabytedbSource()).isTrue();

    YugabyteDBSourceMessage loadedSource = loadResponse.getSourcesList().get(0).getYugabytedbSource();
    assertThat(loadedSource.getTrinoCatalogName()).isEqualTo(trinocatalog);
    assertThat(loadedSource.getTrinoSchemaName()).isEqualTo(trinoschema);
    assertThat(loadedSource.getTableName()).isEqualTo(tableNameYugabyte);
    assertThat(loadedSource.getJdbcUrl()).isEqualTo(jdbcUrl);
    assertThat(loadedSource.getUsername()).isEqualTo(username);
    assertThat(loadedSource.getPassword()).isEqualTo(password);
    assertThat(loadedSource.getReadTimestamp()).isEqualTo(readTimestamp);
  }

  @Test
  public void testLoadTable() {
    String tableName = "test responsetable responseload";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder().setTrinoCatalogName("trino responsecatalog responsekafka")
      .setTrinoSchemaName("schema1").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
      .setTopicName("topic1")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    SourceUpdate sourceUpdate = SourceUpdate.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource).build();
    UpsertSourcesRequest upsertRequest = UpsertSourcesRequest.newBuilder().addSources(sourceUpdate).build();
    stub.upsertSources(upsertRequest);

    String avroSchemaString = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}";
    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setAvroSchema(avroSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasAvroSchema()).isTrue();
    assertThat(response.getAvroSchema()).isEqualTo(avroSchemaString);
    assertThat(response.getSourcesCount()).isEqualTo(1);
    assertThat(response.getSourcesList().get(0).hasKafkaSource()).isTrue();
    assertThat(response.getSourcesList().get(0).getTableName()).isEqualTo(tableName);
  }

  @Test
  public void testLoadTableWithoutSchema() {
    String tableName = "test responsetable responsenoschema";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasAvroSchema()).isFalse();
    assertThat(response.hasIcebergSchema()).isFalse();
    assertThat(response.getSourcesCount()).isEqualTo(0);
  }

  @Test
  public void testUpsertKafkaSourceUpdate() {
    String tableName = "test responsetable responseupdate";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource1 = KafkaSourceMessage.newBuilder().setTrinoCatalogName("catalog responsev1")
      .setTrinoSchemaName("schema responsev1").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
      .setTopicName("test responsetopic")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    SourceUpdate sourceUpdate1 = SourceUpdate.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource1).build();
    UpsertSourcesRequest request1 = UpsertSourcesRequest.newBuilder().addSources(sourceUpdate1).build();
    stub.upsertSources(request1);

    KafkaSourceMessage kafkaSource2 = KafkaSourceMessage.newBuilder().setTrinoCatalogName("catalog responsev2")
      .setTrinoSchemaName("schema responsev2").setStartOffset(100).setEndOffset(200).setPartitionNumber(0)
      .setTopicName("test responsetopic")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    SourceUpdate sourceUpdate2 = SourceUpdate.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource2).build();
    UpsertSourcesRequest request2 = UpsertSourcesRequest.newBuilder().addSources(sourceUpdate2).build();
    stub.upsertSources(request2);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasAvroSchema()).isFalse();
    assertThat(response.hasIcebergSchema()).isFalse();
  }

  @Test
  public void testListTables() {
    String tableName1 = "test responsetable responselist1";
    String tableName2 = "test responsetable responselist2";
    String tableName3 = "test responsetable responselist3";

    stub.createTable(CreateTableRequest.newBuilder().setName(tableName1).build());
    stub.createTable(CreateTableRequest.newBuilder().setName(tableName2).build());
    stub.createTable(CreateTableRequest.newBuilder().setName(tableName3).build());

    ListTablesRequest request = ListTablesRequest.newBuilder().build();
    ListTablesResponse response = stub.listTables(request);

    assertThat(response.getTableNamesList()).contains(tableName1, tableName2, tableName3);
    assertThat(response.getTableNamesCount()).isGreaterThanOrEqualTo(3);
  }

  @Test
  public void testListTablesReturnsResponse() {
    ListTablesRequest request = ListTablesRequest.newBuilder().build();
    ListTablesResponse response = stub.listTables(request);

    assertThat(response).isNotNull();
    assertThat(response.getTableNamesList()).isNotNull();
  }

  @Test
  public void testSetSchema() {
    String tableName = "test responsetable responseavro";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String avroSchemaString = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}";

    SetSchemaRequest request = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setAvroSchema(avroSchemaString)
      .build();

    SetSchemaResponse response = stub.setSchema(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Schema set successfully");
  }

  @Test
  public void testSetSchemaAndFetchSources() {
    String tableName = "test responsetable responseavro responsefetch";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String avroSchemaString = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}";

    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setAvroSchema(avroSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
      .setTrinoCatalogName("trino responsecatalog")
      .setTrinoSchemaName("schema1")
      .setStartOffset(0)
      .setEndOffset(100)
      .setPartitionNumber(0)
      .setTopicName("topic1")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema(avroSchemaString)
      .build();

    SourceUpdate sourceUpdate = SourceUpdate.newBuilder()
      .setTableName(tableName)
      .setKafkaSource(kafkaSource)
      .build();
    UpsertSourcesRequest upsertRequest = UpsertSourcesRequest.newBuilder()
      .addSources(sourceUpdate)
      .build();
    stub.upsertSources(upsertRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasAvroSchema()).isTrue();
    assertThat(response.getAvroSchema()).isEqualTo(avroSchemaString);
    assertThat(response.getSourcesCount()).isEqualTo(1);
  }

  @Test
  public void testSetSchemaWithNullValue() {
    String tableName = "test responsetable responseauroschema responsenull";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String avroSchemaString = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}";

    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setAvroSchema(avroSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    SetSchemaRequest clearRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .build();
    SetSchemaResponse clearResponse = stub.setSchema(clearRequest);

    assertThat(clearResponse.getSuccess()).isTrue();

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse loadResponse = stub.loadTable(loadRequest);

    assertThat(loadResponse.hasAvroSchema()).isFalse();
    assertThat(loadResponse.hasIcebergSchema()).isFalse();
    assertThat(loadResponse.getSourcesCount()).isEqualTo(0);
  }

  @Test
  public void testSetSchemaTableNotFound() {
    SetSchemaRequest request = SetSchemaRequest.newBuilder()
      .setTableName("nonexistent responsetable")
      .setAvroSchema("{\"type\": \"record\"}")
      .build();

    assertThatThrownBy(() -> stub.setSchema(request))
      .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void testSetIcebergSchema() {
    String tableName = "test responsetable responseiceberg responseschema";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String icebergSchemaString = "{\"type\": \"struct\", \"fields\": [{\"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"int\"}, {\"id\": 2, \"name\": \"name\", \"required\": true, \"type\": \"string\"}]}";

    SetSchemaRequest request = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setIcebergSchema(icebergSchemaString)
      .build();

    SetSchemaResponse response = stub.setSchema(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Schema set successfully");
  }

  @Test
  public void testLoadTableWithIcebergSchema() {
    String tableName = "test responsetable responseiceberg responseload";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String icebergSchemaString = "{\"type\": \"struct\", \"fields\": [{\"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"int\"}, {\"id\": 2, \"name\": \"name\", \"required\": true, \"type\": \"string\"}]}";

    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setIcebergSchema(icebergSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasIcebergSchema()).isTrue();
    assertThat(response.getIcebergSchema()).isEqualTo(icebergSchemaString);
  }

  @Test
  public void testLoadTableWithBothSchemas() {
    String tableName = "test responsetable responseboth responseschemas";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String avroSchemaString = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}";
    String icebergSchemaString = "{\"type\": \"struct\", \"fields\": [{\"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"int\"}, {\"id\": 2, \"name\": \"name\", \"required\": true, \"type\": \"string\"}]}";

    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setAvroSchema(avroSchemaString)
      .setIcebergSchema(icebergSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasAvroSchema()).isTrue();
    assertThat(response.getAvroSchema()).isEqualTo(avroSchemaString);
    assertThat(response.hasIcebergSchema()).isTrue();
    assertThat(response.getIcebergSchema()).isEqualTo(icebergSchemaString);
  }

  @Test
  public void testSetProtobufSchema() {
    String tableName = "test responsetable responseprotobuf";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String protobufSchemaString = "syntax=\"proto3\"; message TestMessage { int32 id = 1; string name = 2; }";

    SetSchemaRequest request = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setProtobufSchema(protobufSchemaString)
      .build();

    SetSchemaResponse response = stub.setSchema(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Schema set successfully");
  }

  @Test
  public void testLoadTableWithProtobufSchema() {
    String tableName = "test responsetable responseprotobuf responseload";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String protobufSchemaString = "syntax=\"proto3\"; message TestMessage { int32 id = 1; string name = 2; }";

    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setProtobufSchema(protobufSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasProtobufSchema()).isTrue();
    assertThat(response.getProtobufSchema()).isEqualTo(protobufSchemaString);
  }

  @Test
  public void testLoadTableWithAllThreeSchemas() {
    String tableName = "test responsetable responseall responseschemas";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    String avroSchemaString = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}";
    String icebergSchemaString = "{\"type\": \"struct\", \"fields\": [{\"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"int\"}, {\"id\": 2, \"name\": \"name\", \"required\": true, \"type\": \"string\"}]}";
    String protobufSchemaString = "syntax=\"proto3\"; message TestMessage { int32 id = 1; string name = 2; }";

    SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
      .setTableName(tableName)
      .setAvroSchema(avroSchemaString)
      .setIcebergSchema(icebergSchemaString)
      .setProtobufSchema(protobufSchemaString)
      .build();
    stub.setSchema(schemaRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse response = stub.loadTable(loadRequest);

    assertThat(response.hasAvroSchema()).isTrue();
    assertThat(response.getAvroSchema()).isEqualTo(avroSchemaString);
    assertThat(response.hasIcebergSchema()).isTrue();
    assertThat(response.getIcebergSchema()).isEqualTo(icebergSchemaString);
    assertThat(response.hasProtobufSchema()).isTrue();
    assertThat(response.getProtobufSchema()).isEqualTo(protobufSchemaString);
  }

  @Test
  public void testDropTable() {
    String tableName = "test responsetable responsedrop";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse createResponse = stub.createTable(tableRequest);
    assertThat(createResponse.getSuccess()).isTrue();

    DropTableRequest dropRequest = DropTableRequest.newBuilder().setTableName(tableName).build();
    DropTableResponse dropResponse = stub.dropTable(dropRequest);

    assertThat(dropResponse.getSuccess()).isTrue();
    assertThat(dropResponse.getMessage()).isEqualTo("Table dropped successfully");

    assertThatThrownBy(() -> stub.loadTable(LoadTableRequest.newBuilder().setTableName(tableName).build()))
      .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void testDropTableWithSources() {
    String tableName = "test responsetable responsedrop responsesources";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
      .setTrinoCatalogName("trino responsecatalog")
      .setTrinoSchemaName("schema1")
      .setStartOffset(0)
      .setEndOffset(100)
      .setPartitionNumber(0)
      .setTopicName("topic1")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
      .setTrinoCatalogName("trino responsecatalog")
      .setTrinoSchemaName("schema1")
      .setTableName("iceberg responsetable")
      .setReadTimestamp(System.currentTimeMillis())
      .build();

    SourceUpdate kafkaUpdate = SourceUpdate.newBuilder()
      .setTableName(tableName)
      .setKafkaSource(kafkaSource)
      .build();

    SourceUpdate icebergUpdate = SourceUpdate.newBuilder()
      .setTableName(tableName)
      .setIcebergSource(icebergSource)
      .build();

    UpsertSourcesRequest upsertRequest = UpsertSourcesRequest.newBuilder()
      .addSources(kafkaUpdate)
      .addSources(icebergUpdate)
      .build();
    stub.upsertSources(upsertRequest);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(tableName).build();
    LoadTableResponse loadResponse = stub.loadTable(loadRequest);
    assertThat(loadResponse.getSourcesCount()).isEqualTo(2);

    DropTableRequest dropRequest = DropTableRequest.newBuilder().setTableName(tableName).build();
    DropTableResponse dropResponse = stub.dropTable(dropRequest);

    assertThat(dropResponse.getSuccess()).isTrue();

    assertThatThrownBy(() -> stub.loadTable(LoadTableRequest.newBuilder().setTableName(tableName).build()))
      .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void testDropTableNotFound() {
    DropTableRequest dropRequest = DropTableRequest.newBuilder().setTableName("nonexistent responsetable").build();

    assertThatThrownBy(() -> stub.dropTable(dropRequest))
      .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void testDropTableWithEmptyName() {
    DropTableRequest dropRequest = DropTableRequest.newBuilder().setTableName("").build();

    assertThatThrownBy(() -> stub.dropTable(dropRequest))
      .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  public void testUpsertMultipleSourcesAtomically() {
    String table1 = "test responsetable responseatomic1";
    String table2 = "test responsetable responseatomic2";
    CreateTableRequest request1 = CreateTableRequest.newBuilder().setName(table1).build();
    CreateTableRequest request2 = CreateTableRequest.newBuilder().setName(table2).build();
    stub.createTable(request1);
    stub.createTable(request2);

    KafkaSourceMessage kafkaSource1 = KafkaSourceMessage.newBuilder()
      .setTrinoCatalogName("catalog1")
      .setTrinoSchemaName("schema1")
      .setStartOffset(0)
      .setEndOffset(100)
      .setPartitionNumber(0)
      .setTopicName("topic1")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    IcebergSourceMessage icebergSource2 = IcebergSourceMessage.newBuilder()
      .setTrinoCatalogName("catalog2")
      .setTrinoSchemaName("schema2")
      .setTableName("iceberg responsetable")
      .setReadTimestamp(System.currentTimeMillis())
      .build();

    SourceUpdate update1 = SourceUpdate.newBuilder()
      .setTableName(table1)
      .setKafkaSource(kafkaSource1)
      .build();

    SourceUpdate update2 = SourceUpdate.newBuilder()
      .setTableName(table2)
      .setIcebergSource(icebergSource2)
      .build();

    UpsertSourcesRequest upsertRequest = UpsertSourcesRequest.newBuilder()
      .addSources(update1)
      .addSources(update2)
      .build();

    UpsertSourcesResponse response = stub.upsertSources(upsertRequest);

    assertThat(response.getSuccess()).isTrue();

    LoadTableRequest loadRequest1 = LoadTableRequest.newBuilder().setTableName(table1).build();
    LoadTableResponse loadResponse1 = stub.loadTable(loadRequest1);
    assertThat(loadResponse1.getSourcesCount()).isEqualTo(1);
    assertThat(loadResponse1.getSourcesList().get(0).hasKafkaSource()).isTrue();

    LoadTableRequest loadRequest2 = LoadTableRequest.newBuilder().setTableName(table2).build();
    LoadTableResponse loadResponse2 = stub.loadTable(loadRequest2);
    assertThat(loadResponse2.getSourcesCount()).isEqualTo(1);
    assertThat(loadResponse2.getSourcesList().get(0).hasIcebergSource()).isTrue();
  }

  @Test
  public void testUpsertMultipleSourcesAtomicRollback() {
    String table1 = "test responsetable responseatomic responserollback";
    stub.createTable(CreateTableRequest.newBuilder().setName(table1).build());

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
      .setTrinoCatalogName("catalog")
      .setTrinoSchemaName("schema")
      .setStartOffset(0)
      .setEndOffset(100)
      .setPartitionNumber(0)
      .setTopicName("topic")
      .setBrokerUrls("localhost:9092")
      .setSchemaType(SchemaType.AVRO)
      .setSchema("{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}")
      .build();

    SourceUpdate validUpdate = SourceUpdate.newBuilder()
      .setTableName(table1)
      .setKafkaSource(kafkaSource)
      .build();

    SourceUpdate invalidUpdate = SourceUpdate.newBuilder()
      .setTableName("nonexistent responsetable")
      .setKafkaSource(kafkaSource)
      .build();

    UpsertSourcesRequest upsertRequest = UpsertSourcesRequest.newBuilder()
      .addSources(validUpdate)
      .addSources(invalidUpdate)
      .build();

    assertThatThrownBy(() -> stub.upsertSources(upsertRequest))
      .isInstanceOf(StatusRuntimeException.class);

    LoadTableRequest loadRequest = LoadTableRequest.newBuilder().setTableName(table1).build();
    LoadTableResponse loadResponse = stub.loadTable(loadRequest);

    assertThat(loadResponse.getSourcesCount()).isEqualTo(0);
  }

  @Test
  public void testTableExists() {
    String tableName = "test responsetable responseexists";
    CreateTableRequest request = CreateTableRequest.newBuilder().setName(tableName).build();
    stub.createTable(request);

    TableExistsRequest existsRequest = TableExistsRequest.newBuilder().setTableName(tableName).build();
    TableExistsResponse response = stub.tableExists(existsRequest);

    assertThat(response.getExists()).isTrue();
  }

  @Test
  public void testTableExistsNotFound() {
    String tableName = "nonexistent responsetable";
    TableExistsRequest request = TableExistsRequest.newBuilder().setTableName(tableName).build();
    TableExistsResponse response = stub.tableExists(request);

    assertThat(response.getExists()).isFalse();
  }
}
