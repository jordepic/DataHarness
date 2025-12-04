// Copyright (c) 2025
package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.example.proto.*;
import org.example.server.GrpcServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;

import static org.assertj.core.api.Assertions.*;

public class CatalogServiceIntegrationTest {
	private static PostgreSQLContainer<?> postgres;
	private static GrpcServer grpcServer;
	private static ManagedChannel channel;
	private CatalogServiceGrpc.CatalogServiceBlockingStub stub;

	@BeforeClass
	public static void setUpClass() throws IOException {
		postgres = new PostgreSQLContainer<>("postgres:15").withDatabaseName("dataharness").withUsername("postgres")
				.withPassword("postgres");
		postgres.start();

		System.setProperty("hibernate.connection.url",
				"jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getFirstMappedPort() + "/dataharness");
		System.setProperty("hibernate.connection.username", "postgres");
		System.setProperty("hibernate.connection.password", "postgres");
		System.setProperty("hibernate.hbm2ddl.auto", "create-drop");

		grpcServer = new GrpcServer();
		grpcServer.start();

		channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
	}

	@AfterClass
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

	@Before
	public void setUp() {
		stub = CatalogServiceGrpc.newBlockingStub(channel);
	}

	@Test
	public void testCreateTable() {
		CreateTableRequest request = CreateTableRequest.newBuilder().setName("test_table_1").build();

		CreateTableResponse response = stub.createTable(request);

		assertThat(response.getSuccess()).isTrue();
		assertThat(response.getMessage()).isEqualTo("Table created successfully");
		assertThat(response.getTableId()).isNotEqualTo(0);
	}

	@Test
	public void testCreateTableWithDuplicateName() {
		CreateTableRequest request1 = CreateTableRequest.newBuilder().setName("duplicate_table").build();

		CreateTableResponse response1 = stub.createTable(request1);
		assertThat(response1.getSuccess()).isTrue();

		CreateTableRequest request2 = CreateTableRequest.newBuilder().setName("duplicate_table").build();

		assertThatThrownBy(() -> stub.createTable(request2)).isInstanceOf(StatusRuntimeException.class);
	}

	@Test
	public void testUpsertKafkaSource() {
		String tableName = "test_table_kafka";
		CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
		CreateTableResponse _ = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder().setTrinoCatalogName("trino_catalog")
				.setTrinoSchemaName("public").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
				.setTopicName("test_topic").build();

		UpsertSourceRequest request = UpsertSourceRequest.newBuilder().setTableName(tableName)
				.setKafkaSource(kafkaSource).build();

		UpsertSourceResponse response = stub.upsertSource(request);

		assertThat(response.getSuccess()).isTrue();
		assertThat(response.getMessage()).isEqualTo("Source upserted successfully");
	}

	@Test
	public void testUpsertIcebergSource() {
		String tableName = "test_table_iceberg";
		CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
		CreateTableResponse _ = stub.createTable(tableRequest);

    IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder().setTrinoCatalogName("trino_catalog")
				.setTrinoSchemaName("public").setTableName("iceberg_table").setReadTimestamp(System.currentTimeMillis())
				.build();

		UpsertSourceRequest request = UpsertSourceRequest.newBuilder().setTableName(tableName)
				.setIcebergSource(icebergSource).build();

		UpsertSourceResponse response = stub.upsertSource(request);

		assertThat(response.getSuccess()).isTrue();
		assertThat(response.getMessage()).isEqualTo("Source upserted successfully");
	}

	@Test
	public void testFetchSources() {
		String tableName = "test_table_fetch";
		CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
		CreateTableResponse _ = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder().setTrinoCatalogName("trino_catalog_kafka")
				.setTrinoSchemaName("schema1").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
				.setTopicName("topic1").build();

		UpsertSourceRequest upsertRequest1 = UpsertSourceRequest.newBuilder().setTableName(tableName)
				.setKafkaSource(kafkaSource).build();
		stub.upsertSource(upsertRequest1);

		IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
				.setTrinoCatalogName("trino_catalog_iceberg").setTrinoSchemaName("schema2")
				.setTableName("iceberg_table1").setReadTimestamp(12345L).build();

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
		assertThat(source2.getIcebergSource().getTableName()).isEqualTo("iceberg_table1");
	}

	@Test
	public void testFetchSourcesEmptyTable() {
		String tableName = "test_table_empty";
		CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
		CreateTableResponse tableResponse = stub.createTable(tableRequest);

    FetchSourcesRequest fetchRequest = FetchSourcesRequest.newBuilder().setTableName(tableName).build();

		FetchSourcesResponse response = stub.fetchSources(fetchRequest);

		assertThat(response.getSourcesCount()).isEqualTo(0);
	}

	@Test
	public void testUpsertKafkaSourceUpdate() {
		String tableName = "test_table_update";
		CreateTableRequest tableRequest = CreateTableRequest.newBuilder().setName(tableName).build();
		CreateTableResponse tableResponse = stub.createTable(tableRequest);

    KafkaSourceMessage kafkaSource1 = KafkaSourceMessage.newBuilder().setTrinoCatalogName("catalog_v1")
				.setTrinoSchemaName("schema_v1").setStartOffset(0).setEndOffset(100).setPartitionNumber(0)
				.setTopicName("test_topic").build();

		UpsertSourceRequest request1 = UpsertSourceRequest.newBuilder().setTableName(tableName)
				.setKafkaSource(kafkaSource1).build();
		stub.upsertSource(request1);

		KafkaSourceMessage kafkaSource2 = KafkaSourceMessage.newBuilder().setTrinoCatalogName("catalog_v2")
				.setTrinoSchemaName("schema_v2").setStartOffset(100).setEndOffset(200).setPartitionNumber(0)
				.setTopicName("test_topic").build();

		UpsertSourceRequest request2 = UpsertSourceRequest.newBuilder().setTableName(tableName)
				.setKafkaSource(kafkaSource2).build();
		stub.upsertSource(request2);

		FetchSourcesRequest fetchRequest = FetchSourcesRequest.newBuilder().setTableName(tableName).build();

		FetchSourcesResponse response = stub.fetchSources(fetchRequest);

		assertThat(response.getSourcesCount()).isEqualTo(1);
		TableSourceMessage source = response.getSourcesList().getFirst();
		assertThat(source.getTableName()).isEqualTo(tableName);
		assertThat(source.getKafkaSource().getTrinoCatalogName()).isEqualTo("catalog_v2");
		assertThat(source.getKafkaSource().getTrinoSchemaName()).isEqualTo("schema_v2");
		assertThat(source.getKafkaSource().getStartOffset()).isEqualTo(100);
		assertThat(source.getKafkaSource().getEndOffset()).isEqualTo(200);
	}
}
