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

import io.github.jordepic.db.HibernateSessionManager;
import io.github.jordepic.proto.CatalogServiceGrpc;
import io.github.jordepic.server.GrpcServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Integration test that uses TestContainers to spin up all necessary services
 * (Kafka, PostgreSQL, Iceberg, MinIO) and runs the DataHarness gRPC server
 * locally for testing the DataHarness system.
 *
 * <p>The DataHarness server runs locally and connects to the PostgreSQL container started
 * by docker-compose. Data is populated into Kafka, PostgreSQL, and Iceberg, and queries
 * are run against the unified DataHarness table via Spark and Trino.
 */
public class DataHarnessTest {
    private static final Logger logger = LoggerFactory.getLogger(DataHarnessTest.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String BOOTSTRAP_SERVERS_FOR_SPARK = "kafka:29092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC = "kafka_avro_test";
    private static final String DATA_HARNESS_TABLE = "bootstrap";
    private static final String ICEBERG_TABLE_NAME = "iceberg_test";
    private static final String POSTGRES_TABLE_NAME = "postgres_test";
    private static final String TABLE_SCHEMA = "id INT PRIMARY KEY, name TEXT, address TEXT";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";

    static DockerComposeContainer<?> environment;

    private static GrpcServer grpcServer;
    private static CatalogServiceGrpc.CatalogServiceBlockingStub stub;
    private static DataSourcePopulator.KafkaPopulationResult kafkaResult;
    private static DataSourcePopulator.IcebergPopulationResult icebergResult;
    private static long postgresTimestamp;

    static {
        try {
            Path pluginZip = Paths.get("../dataharness-trino/target/dataharness-trino-2.0.zip")
                    .toAbsolutePath();
            Path targetDir = Paths.get("src/test/java/io/github/jordepic/bootstrap/dataharness-trino/target")
                    .toAbsolutePath();
            Files.createDirectories(targetDir);
            Path destination = targetDir.resolve("dataharness-trino-2.0.zip");
            Files.copy(pluginZip, destination, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Copied Trino plugin zip to test directory");
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy Trino plugin zip", e);
        }
    }

    @BeforeAll
    public static void setUpClass() throws Exception {
        environment = new DockerComposeContainer<>(
                        new File("src/test/java/io/github/jordepic/bootstrap/docker-compose-testcontainers.yaml"))
                .withServices(
                        "kafka", "schema-registry", "minio", "gravitino-iceberg-rest", "postgres", "spark", "trino")
                .withExposedService("kafka", 9092, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService(
                        "schema-registry", 8081, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService("minio", 9000, Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService(
                        "gravitino-iceberg-rest",
                        9001,
                        Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService(
                        "postgres", 5432, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService("spark", 15002, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(300)))
                .withExposedService("trino", 8080, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(300)));

        environment.start();
        logger.info("Docker Compose environment started");

        String postgresHost = environment.getServiceHost("postgres", 5432);
        int postgresPort = environment.getServicePort("postgres", 5432);

        logger.info("PostgreSQL container running at {}:{}", postgresHost, postgresPort);

        System.setProperty(
                "hibernate.connection.url", "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/dataharness");
        System.setProperty("hibernate.connection.username", "postgres");
        System.setProperty("hibernate.connection.password", "postgres");
        System.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        System.setProperty("grpc.server.port", "50051");

        grpcServer = new GrpcServer();
        grpcServer.start();
        logger.info("DataHarness gRPC server started on port 50051");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();
        stub = CatalogServiceGrpc.newBlockingStub(channel);

        DataSourcePopulator.deleteKafkaTopic(BOOTSTRAP_SERVERS, TOPIC);
        DataHarnessHelper.deleteDataHarnessTable(stub, DATA_HARNESS_TABLE);
        DataSourcePopulator.deletePostgresTable(
                "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres", POSTGRES_TABLE_NAME);

        postgresTimestamp = DataSourcePopulator.populatePostgres(
                "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres", POSTGRES_TABLE_NAME);
        kafkaResult = DataSourcePopulator.populateKafka(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC);
        icebergResult = DataSourcePopulator.populateIceberg(
                MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ICEBERG_TABLE_NAME);

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
    }

    @AfterAll
    public static void tearDownClass() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.stop();
            logger.info("DataHarness gRPC server stopped");
        }
        HibernateSessionManager.closeSessionFactory();
        if (environment != null) {
            try {
                environment.stop();
                logger.info("Docker Compose environment stopped");
                Thread.sleep(2000);
            } catch (Exception e) {
                logger.error("Error stopping Docker Compose environment", e);
            }
        }
    }

    @Test
    public void testDataHarnessViaSparkConnect() throws Exception {
        DataHarnessHelper.querySparkConnectAndVerify("SELECT * FROM harness.data_harness.bootstrap", 9);
        logger.info("DataHarness Spark Connect test completed successfully");
    }

    @Test
    public void testDataHarnessViaTrino() throws Exception {
        DataHarnessHelper.queryTrinoAndVerify("SELECT * FROM data_harness.default.bootstrap", 9);
        logger.info("DataHarness Trino test completed successfully");
    }

    @Test
    public void testPartitionedDataHarnessViaSparkAndTrino() throws Exception {
        String partitionedIcebergTableName = "iceberg_partitioned_test";

        DataHarnessHelper.deleteDataHarnessTable(stub, "partitioned_bootstrap");
        DataSourcePopulator.deleteKafkaTopic(BOOTSTRAP_SERVERS, "test_partitioned_topic");

        DataSourcePopulator.PartitionedIcebergResult partitionedResult = DataSourcePopulator.populatePartitionedIceberg(
                MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, partitionedIcebergTableName);

        DataHarnessHelper.createPartitionedDataHarnessTable(
                stub, "partitioned_bootstrap", partitionedIcebergTableName, partitionedResult);

        DataHarnessHelper.querySparkConnectWithExplain(
                "SELECT * FROM harness.data_harness.partitioned_bootstrap WHERE id = 1", 1);

        DataHarnessHelper.queryTrinoWithExplain(
                "SELECT * FROM data_harness.default.partitioned_bootstrap WHERE id = 1", 1);

        logger.info("Partitioned DataHarness test completed successfully");
    }
}
