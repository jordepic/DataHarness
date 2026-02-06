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

import io.github.jordepic.proto.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connect.Dataset;
import org.apache.spark.sql.connect.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHarnessHelper {
    private static final Logger logger = LoggerFactory.getLogger(DataHarnessHelper.class);

    public static void deleteDataHarnessTable(CatalogServiceGrpc.CatalogServiceBlockingStub stub, String tableName) {
        try {
            DropTableRequest dropRequest =
                    DropTableRequest.newBuilder().setTableName(tableName).build();
            stub.dropTable(dropRequest);
            logger.info("Deleted DataHarness table: {}", tableName);
        } catch (Exception e) {
            logger.debug("DataHarness table may not exist: {}", e.getMessage());
        }
    }

    public static void createDataHarnessTable(
            CatalogServiceGrpc.CatalogServiceBlockingStub stub,
            String tableName,
            String topic,
            String bootstrapServersForSpark,
            DataSourcePopulator.KafkaPopulationResult kafkaResult,
            String icebergTableName,
            DataSourcePopulator.IcebergPopulationResult icebergResult,
            String postgresTableName,
            long postgresTimestamp) {
        CreateTableRequest createTableRequest =
                CreateTableRequest.newBuilder().setName(tableName).build();

        stub.createTable(createTableRequest);
        logger.info("Created DataHarness table: {}", tableName);

        KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
                .setTrinoCatalogName("kafka")
                .setTrinoSchemaName("default")
                .setTopicName(topic)
                .setStartOffset(0)
                .setEndOffset(kafkaResult.messageCount)
                .setPartitionNumber(0)
                .setBrokerUrls(bootstrapServersForSpark)
                .setSchemaType(SchemaType.AVRO)
                .setSchema(kafkaResult.avroSchema)
                .build();

        SourceUpdate kafkaSourceUpdate = SourceUpdate.newBuilder()
                .setTableName(tableName)
                .setKafkaSource(kafkaSource)
                .build();

        UpsertSourcesRequest.Builder upsertSourcesBuilder =
                UpsertSourcesRequest.newBuilder().addSources(kafkaSourceUpdate);

        IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
                .setTrinoCatalogName("iceberg")
                .setTrinoSchemaName("default")
                .setTableName(icebergTableName)
                .setReadTimestamp(icebergResult.snapshotId)
                .setSparkCatalogName("gravitino")
                .setSparkSchemaName("default")
                .build();

        SourceUpdate icebergSourceUpdate = SourceUpdate.newBuilder()
                .setTableName(tableName)
                .setIcebergSource(icebergSource)
                .build();

        upsertSourcesBuilder.addSources(icebergSourceUpdate);

        PostgresDBSourceMessage postgresSource = PostgresDBSourceMessage.newBuilder()
                .setTrinoCatalogName("postgresql")
                .setTrinoSchemaName("public")
                .setTableName(postgresTableName)
                .setTableNameNoTstzrange(postgresTableName + "_view")
                .setHistoryTableName(postgresTableName + "_temporal")
                .setHistoryTableNameNoTstzrange(postgresTableName + "_temporal_view")
                .setJdbcUrl("jdbc:postgresql://postgres:5432/postgres?sslmode=disable")
                .setUsername("postgres")
                .setPassword("postgres")
                .setReadTimestamp(postgresTimestamp)
                .build();

        SourceUpdate postgresSourceUpdate = SourceUpdate.newBuilder()
                .setTableName(tableName)
                .setPostgresdbSource(postgresSource)
                .build();

        upsertSourcesBuilder.addSources(postgresSourceUpdate);

        stub.upsertSources(upsertSourcesBuilder.build());
        logger.info("Upserted sources for DataHarness table");

        SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
                .setTableName(tableName)
                .setAvroSchema(kafkaResult.avroSchema)
                .setIcebergSchema(icebergResult.icebergSchema)
                .build();

        stub.setSchema(schemaRequest);
        logger.info("Set schema for DataHarness table");
    }

    public static void querySparkConnectAndVerify(String sparkQuery, int expectedRowCount) {
        try (SparkSession spark =
                SparkSession.builder().remote("sc://localhost:15002").getOrCreate()) {
            Dataset<Row> result = spark.sql(sparkQuery);

            List<Row> rows = result.collectAsList();
            logger.info("Spark query returned {} rows", rows.size());
            for (Row row : rows) {
                logger.info("  id={}, name={}, address={}", row.getString(0), row.getInt(1), row.getString(2));
            }

            assertThat(rows).hasSize(expectedRowCount);
            logger.info("Successfully verified {} rows from DataHarness table in Spark", expectedRowCount);
        }
    }

    public static void queryTrinoAndVerify(String trinoQuery, int expectedRowCount) throws Exception {
        String jdbcUrl = "jdbc:trino://localhost:8082";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "trino", "")) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(trinoQuery);
                int rowCount = 0;
                while (rs.next()) {
                    logger.info(
                            "  id={}, name={}, address={}",
                            rs.getInt("id"),
                            rs.getString("name"),
                            rs.getString("address"));
                    rowCount++;
                }
                logger.info("Trino query returned {} rows", rowCount);
                assertThat(rowCount).isEqualTo(expectedRowCount);
                logger.info("Successfully verified {} rows from DataHarness table in Trino", expectedRowCount);
            }
        }
    }

    public static void createPartitionedDataHarnessTable(
            CatalogServiceGrpc.CatalogServiceBlockingStub stub,
            String tableName,
            String icebergTableName,
            DataSourcePopulator.PartitionedIcebergResult partitionedResult) {
        CreateTableRequest createTableRequest =
                CreateTableRequest.newBuilder().setName(tableName).build();

        stub.createTable(createTableRequest);
        logger.info("Created DataHarness table: {}", tableName);

        UpsertSourcesRequest.Builder initialUpsertBuilder = UpsertSourcesRequest.newBuilder();

        for (Map.Entry<Integer, Long> entry : partitionedResult.partitionSnapshots.entrySet()) {
            int id = entry.getKey();

            IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
                    .setName("partition_" + id + "_source")
                    .setTrinoCatalogName("iceberg")
                    .setTrinoSchemaName("default")
                    .setTableName(icebergTableName)
                    .setReadTimestamp(0)
                    .setSparkCatalogName("gravitino")
                    .setSparkSchemaName("default")
                    .setPartitionFilter("id = " + id)
                    .build();

            SourceUpdate icebergSourceUpdate = SourceUpdate.newBuilder()
                    .setTableName(tableName)
                    .setIcebergSource(icebergSource)
                    .build();

            initialUpsertBuilder.addSources(icebergSourceUpdate);
        }

        stub.upsertSources(initialUpsertBuilder.build());
        logger.info("Upserted initial sources with read_timestamp=0");

        ClaimSourcesRequest.Builder claimBuilder = ClaimSourcesRequest.newBuilder();
        for (int id : partitionedResult.partitionSnapshots.keySet()) {
            String modifier = "partition_" + id + "_modifier";
            SourceToClaim sourceToClaim = SourceToClaim.newBuilder()
                    .setName("partition_" + id + "_source")
                    .setModifier(modifier)
                    .build();
            claimBuilder.addSources(sourceToClaim);
        }

        stub.claimSources(claimBuilder.build());
        logger.info("Claimed all partitioned sources");

        for (Map.Entry<Integer, Long> entry : partitionedResult.partitionSnapshots.entrySet()) {
            int id = entry.getKey();
            long snapshotId = entry.getValue();
            String modifier = "partition_" + id + "_modifier";

            IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
                    .setName("partition_" + id + "_source")
                    .setTrinoCatalogName("iceberg")
                    .setTrinoSchemaName("default")
                    .setTableName(icebergTableName)
                    .setReadTimestamp(snapshotId)
                    .setSparkCatalogName("gravitino")
                    .setSparkSchemaName("default")
                    .setPartitionFilter("id = " + id)
                    .setModifier(modifier)
                    .build();

            SourceUpdate icebergSourceUpdate = SourceUpdate.newBuilder()
                    .setTableName(tableName)
                    .setIcebergSource(icebergSource)
                    .build();

            UpsertSourcesRequest upsertRequest = UpsertSourcesRequest.newBuilder()
                    .addSources(icebergSourceUpdate)
                    .build();

            stub.upsertSources(upsertRequest);
            logger.info("Upserted partition {} with snapshot {} and partition filter", id, snapshotId);
        }

        SetSchemaRequest schemaRequest = SetSchemaRequest.newBuilder()
                .setTableName(tableName)
                .setIcebergSchema(partitionedResult.icebergSchema)
                .build();

        stub.setSchema(schemaRequest);
        logger.info("Set schema for DataHarness table");
    }

    public static void querySparkConnectWithExplain(String sparkQuery, int expectedRowCount) {
        try (SparkSession spark =
                SparkSession.builder().remote("sc://localhost:15002").getOrCreate()) {
            logger.info("=== Spark Query Plan ===");
            Dataset<Row> result = spark.sql("EXPLAIN FORMATTED " + sparkQuery);
            List<Row> explainRows = result.collectAsList();
            for (Row row : explainRows) {
                logger.info("{}", row.getString(0));
            }

            logger.info("=== Spark Query Results ===");
            result = spark.sql(sparkQuery);
            List<Row> rows = result.collectAsList();
            for (Row row : rows) {
                logger.info("  id={}, name={}, address={}", row.getInt(0), row.getString(1), row.getString(2));
            }
            logger.info("Spark query returned {} rows", rows.size());
            assertThat(rows).hasSize(expectedRowCount);
        }
    }

    public static void queryTrinoWithExplain(String trinoQuery, int expectedRowCount) throws Exception {
        String jdbcUrl = "jdbc:trino://localhost:8082";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "trino", "")) {
            try (Statement stmt = conn.createStatement()) {
                logger.info("=== Trino Query Plan ===");
                ResultSet rs = stmt.executeQuery("EXPLAIN ANALYZE " + trinoQuery);
                while (rs.next()) {
                    logger.info("{}", rs.getString(1));
                }

                logger.info("=== Trino Query Results ===");
                rs = stmt.executeQuery(trinoQuery);
                int rowCount = 0;
                while (rs.next()) {
                    logger.info(
                            "  id={}, name={}, address={}",
                            rs.getInt("id"),
                            rs.getString("name"),
                            rs.getString("address"));
                    rowCount++;
                }
                logger.info("Trino query returned {} rows", rowCount);
                assertThat(rowCount).isEqualTo(expectedRowCount);
            }
        }
    }
}
