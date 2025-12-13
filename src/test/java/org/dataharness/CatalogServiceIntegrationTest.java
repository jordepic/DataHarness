// Copyright (c) 2025
package org.dataharness;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.dataharness.proto.*;
import org.dataharness.server.GrpcServer;
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
    stub = CatalogServiceGrpc.newBlockingStub(channel);
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
  public void testFetchSources() {
    String tableName = "test responsetable responsefetch";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder().setTrinoCatalogName("trino responsecatalog responsekafka")
      .setTrinoSchemaName("schema1").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
      .setTopicName("topic1").build();

    UpsertSourceRequest upsertRequest1 = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setKafkaSource(kafkaSource).build();
    stub.upsertSource(upsertRequest1);

    IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
      .setTrinoCatalogName("trino responsecatalog responseiceberg").setTrinoSchemaName("schema2")
      .setTableName("iceberg responsetable1").setReadTimestamp(12345L).build();

    UpsertSourceRequest upsertRequest2 = UpsertSourceRequest.newBuilder().setTableName(tableName)
      .setIcebergSource(icebergSource).build();
    stub.upsertSource(upsertRequest2);

    FetchSourcesRequest fetchRequest = FetchSourcesRequest.newBuilder().setTableName(tableName).build();

    FetchSourcesResponse response = stub.fetchSources(fetchRequest);

    assertThat(response.getSourcesCount()).isEqualTo(2);

    TableSourceMessage source1 = response.getSourcesList().get(0);
    assertThat(source1.hasKafkaSource()).isTrue();
    assertThat(source1.getTableName()).isEqualTo(tableName);
    assertThat(source1.getKafkaSource().getTopicName()).isEqualTo("topic1");

    TableSourceMessage source2 = response.getSourcesList().get(1);
    assertThat(source2.hasIcebergSource()).isTrue();
    assertThat(source2.getTableName()).isEqualTo(tableName);
    assertThat(source2.getIcebergSource().getTableName()).isEqualTo("iceberg responsetable1");
  }

  @Test
  public void testFetchSourcesEmptyTable() {
    String tableName = "test responsetable responseempty";
    CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
    CreateTableResponse tableResponse = stub.createTable(tableRequest);

    FetchSourcesRequest fetchRequest = FetchSourcesRequest.newBuilder().setTableName(tableName).build();

    FetchSourcesResponse response = stub.fetchSources(fetchRequest);

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

    FetchSourcesRequest fetchRequest = FetchSourcesRequest.newBuilder().setTableName(tableName).build();

    FetchSourcesResponse response = stub.fetchSources(fetchRequest);

    assertThat(response.getSourcesCount()).isEqualTo(1);
    TableSourceMessage source = response.getSourcesList().get(0);
    assertThat(source.getTableName()).isEqualTo(tableName);
    assertThat(source.getKafkaSource().getTrinoCatalogName()).isEqualTo("catalog responsev2");
    assertThat(source.getKafkaSource().getTrinoSchemaName()).isEqualTo("schema responsev2");
    assertThat(source.getKafkaSource().getStartOffset()).isEqualTo(100);
    assertThat(source.getKafkaSource().getEndOffset()).isEqualTo(200);
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
}
