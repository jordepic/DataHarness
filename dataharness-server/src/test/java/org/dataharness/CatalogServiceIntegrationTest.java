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
      .setTopicName("test responsetopic").build();

    UpsertSourceRequest request = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource).build();

    UpsertSourceResponse response = stub.upsertSource(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Source upserted successfully");
  }

  @Test
  public void testUpsertIcebergSource() {
    String tableName = "test responsetable responseiceberg";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder().setTrinoCatalogName("trino responsecatalog")
      .setTrinoSchemaName("public").setTableName("iceberg responsetable").setReadTimestamp(System.currentTimeMillis())
      .build();

    UpsertSourceRequest request = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setIcebergSource(icebergSource).build();

    UpsertSourceResponse response = stub.upsertSource(request);

    assertThat(response.getSuccess()).isTrue();
    assertThat(response.getMessage()).isEqualTo("Source upserted successfully");
  }

  @Test
  public void testLoadTable() {
    String tableName = "test responsetable responseload";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder().setTrinoCatalogName("trino responsecatalog responsekafka")
      .setTrinoSchemaName("schema1").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
      .setTopicName("topic1").build();

    UpsertSourceRequest upsertRequest1 = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource).build();
    stub.upsertSource(upsertRequest1);

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
      .setTopicName("test responsetopic").build();

    UpsertSourceRequest request1 = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource1).build();
    stub.upsertSource(request1);

    KafkaSourceMessage kafkaSource2 = KafkaSourceMessage.newBuilder().setTrinoCatalogName("catalog responsev2")
      .setTrinoSchemaName("schema responsev2").setStartOffset(100).setEndOffset(200).setPartitionNumber(0)
      .setTopicName("test responsetopic").build();

    UpsertSourceRequest request2 = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource2).build();
    stub.upsertSource(request2);

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
      .build();

    UpsertSourceRequest upsertRequest = UpsertSourceRequest.newBuilder()
      .setTableName(tableName)
      .setKafkaSource(kafkaSource)
      .build();
    stub.upsertSource(upsertRequest);

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
}
