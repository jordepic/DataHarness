/*
 * The MIT License
 * Copyright © 2026 Jordan Epstein
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.github.jordepic.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.github.jordepic.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connect.Dataset;
import org.apache.spark.sql.connect.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Integration test that populates Kafka, YugabyteDB, Iceberg, and DataHarness with test data.
 *
 * <p>Before running this test, ensure that the required services are running by executing:
 * ./start_images.sh
 *
 * <p>This test is idempotent and will clean up existing data before populating new data.
 */
public class DataPopulatorIntegrationTest {
    public static final String NOT_IMPLEMENTED = "";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String BOOTSTRAP_SERVERS_FOR_SPARK = "kafka:29092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC = "kafka_avro_test";
    private static final String DATA_HARNESS_TABLE = "bootstrap";
    private static final String ICEBERG_TABLE_NAME = "iceberg_test";
    private static final String YUGABYTE_TABLE_NAME = "yugabyte_test";
    private static final String YUGABYTE_JDBC_URL = "jdbc:postgresql://localhost:5433/yugabyte?sslmode=disable";
    private static final String YUGABYTE_JDBC_URL_FOR_SPARK =
            "jdbc:postgresql://yugabytedb:5433/yugabyte?sslmode=disable";
    private static final String YUGABYTE_USER = "yugabyte";
    private static final String YUGABYTE_PASSWORD = "";
    private static final String POSTGRES_JDBC_URL = "jdbc:postgresql://localhost:5432/postgres?sslmode=disable";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "postgres";
    private static final String POSTGRES_TABLE_NAME = "postgres_test";
    private static final String TABLE_SCHEMA = "id INT PRIMARY KEY, name TEXT, address TEXT";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";

    @Test
    public void bootstrapDataHarness() throws Exception {
        DataSourcePopulator.deleteKafkaTopic(BOOTSTRAP_SERVERS, TOPIC);
        DataHarnessHelper.deleteDataHarnessTable(getStub(), DATA_HARNESS_TABLE);
        deleteYugabyteTable();
        DataSourcePopulator.deletePostgresTable(
                POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE_NAME);

        long yugabyteTimestamp = populateYugabyteDB();
        DataSourcePopulator.KafkaPopulationResult kafkaResult =
                DataSourcePopulator.populateKafka(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC);
        DataSourcePopulator.IcebergPopulationResult icebergResult = DataSourcePopulator.populateIceberg(
                MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ICEBERG_TABLE_NAME);
        long postgresTimestamp = DataSourcePopulator.populatePostgres(
                POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_TABLE_NAME);

        CatalogServiceGrpc.CatalogServiceBlockingStub stub = getStub();

        DataHarnessHelper.createDataHarnessTable(
                stub,
                DATA_HARNESS_TABLE,
                TOPIC,
                BOOTSTRAP_SERVERS_FOR_SPARK,
                kafkaResult,
                ICEBERG_TABLE_NAME,
                icebergResult,
                POSTGRES_TABLE_NAME,
                postgresTimestamp);

        UpsertSourcesRequest.Builder upsertSourcesBuilder = UpsertSourcesRequest.newBuilder();

        YugabyteDBSourceMessage yugabyteSource = YugabyteDBSourceMessage.newBuilder()
                .setTrinoCatalogName(NOT_IMPLEMENTED)
                .setTrinoSchemaName(NOT_IMPLEMENTED)
                .setTableName(YUGABYTE_TABLE_NAME)
                .setJdbcUrl(YUGABYTE_JDBC_URL_FOR_SPARK)
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

        ProtobufSchema protobufSchemaObj = new ProtobufSchema(TestMessage.getDescriptor());
        String protobufSchema = protobufSchemaObj.canonicalString();

        SetSchemaRequest.Builder schemaRequestBuilder = SetSchemaRequest.newBuilder()
                .setTableName(DATA_HARNESS_TABLE)
                .setAvroSchema(kafkaResult.avroSchema)
                .setProtobufSchema(protobufSchema);

        schemaRequestBuilder.setIcebergSchema(icebergResult.icebergSchema);

        SetSchemaRequest schemaRequest = schemaRequestBuilder.build();

        stub.setSchema(schemaRequest);

        fetchAndValidateSources(stub);
        connectAndQuerySparkConnect();
        DataHarnessHelper.queryTrinoAndVerify("SELECT * FROM data_harness.default.bootstrap", 9);
    }

    private CatalogServiceGrpc.CatalogServiceBlockingStub getStub() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();
        return CatalogServiceGrpc.newBlockingStub(channel);
    }

    private void fetchAndValidateSources(CatalogServiceGrpc.CatalogServiceBlockingStub stub) {
        LoadTableRequest request =
                LoadTableRequest.newBuilder().setTableName(DATA_HARNESS_TABLE).build();

        LoadTableResponse response = stub.loadTable(request);
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
                ResultSet rs =
                        stmt.executeQuery("SELECT (EXTRACT (EPOCH FROM CURRENT_TIMESTAMP)*1000000)::decimal(38,0)");
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
                ResultSet rs =
                        stmt.executeQuery("SELECT id, name, address FROM " + YUGABYTE_TABLE_NAME + " ORDER BY id");
                System.out.println("YugabyteDB records:");
                while (rs.next()) {
                    System.out.println("  id="
                            + rs.getInt("id")
                            + ", name="
                            + rs.getString("name")
                            + ", address="
                            + rs.getString("address"));
                }
            }

            return timestamp;
        }
    }

    private void connectAndQuerySparkConnect() {
        try (SparkSession spark =
                SparkSession.builder().remote("sc://localhost:15002").getOrCreate()) {
            Dataset<Row> result = spark.sql("SELECT * FROM harness.data_harness.bootstrap");

            assertThat(result.collectAsList()).hasSize(12);
        }
    }
}
