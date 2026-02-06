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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.net.URI;
import java.util.*;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketPolicyRequest;

/**
 * Helper class for populating data sources (Kafka, Iceberg, PostgreSQL) for integration tests.
 */
public class DataSourcePopulator {
    private static final Logger logger = LoggerFactory.getLogger(DataSourcePopulator.class);

    public static void deleteKafkaTopic(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            ListTopicsResult topics = admin.listTopics();
            if (topics.names().get().contains(topic)) {
                admin.deleteTopics(Collections.singleton(topic)).all().get();
                logger.info("Deleted Kafka topic: {}", topic);
            }
        } catch (Exception e) {
            logger.warn("Failed to delete Kafka topic: {}", e.getMessage());
        }
    }

    public static void deletePostgresTable(String jdbcUrl, String username, String password, String tableName) {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, username, password)) {
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("DROP VIEW IF EXISTS " + tableName + "_view CASCADE");
                stmt.execute("DROP VIEW IF EXISTS " + tableName + "_temporal_view CASCADE");
                stmt.execute("DROP TABLE IF EXISTS " + tableName + "_temporal CASCADE");
                stmt.execute("DROP TABLE IF EXISTS " + tableName + " CASCADE");
                logger.info("Cleaned up PostgreSQL tables");
            }
        } catch (Exception e) {
            logger.debug("PostgreSQL cleanup: {}", e.getMessage());
        }
    }

    public static KafkaPopulationResult populateKafka(String bootstrapServers, String schemaRegistryUrl, String topic)
            throws Exception {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        Schema schema = ReflectData.get().getSchema(KafkaTestRecord.class);
        String avroSchemaJson = schema.toString();

        client.register(topic + "-value", schema);

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", KafkaAvroSerializer.class);
        producerProps.put("schema.registry.url", schemaRegistryUrl);

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

                producer.send(new ProducerRecord<>(topic, record));
            }
            producer.flush();
        }

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", KafkaAvroDeserializer.class);
        consumerProps.put("schema.registry.url", schemaRegistryUrl);
        consumerProps.put("group.id", "test-group-" + System.currentTimeMillis());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("session.timeout.ms", "10000");
        consumerProps.put("fetch.min.bytes", "1");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, GenericRecord> records = consumer.poll(10000);
            logger.info("Populated Kafka with {} messages", records.count());
        }

        return new KafkaPopulationResult(avroSchemaJson, messages.size());
    }

    public static IcebergPopulationResult populateIceberg(
            String minioEndpoint, String minioAccessKey, String minioSecretKey, String icebergTableName)
            throws Exception {
        System.setProperty("aws.region", "us-east-1");

        createMinIOBucket(minioEndpoint, minioAccessKey, minioSecretKey);

        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "address", Types.StringType.get()));

        String icebergSchemaJson = SchemaParser.toJson(icebergSchema);

        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "http://localhost:9001/iceberg");
        properties.put("s3.endpoint", minioEndpoint);
        properties.put("s3.access-key-id", minioAccessKey);
        properties.put("s3.secret-access-key", minioSecretKey);
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");

        try (RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("rest", properties);

            Namespace namespace = Namespace.of("default");
            if (!catalog.namespaceExists(namespace)) {
                catalog.createNamespace(namespace);
            }

            TableIdentifier tableId = TableIdentifier.of(namespace, icebergTableName);

            if (catalog.tableExists(tableId)) {
                catalog.dropTable(tableId);
            }

            String tableLocation = "s3a://iceberg-bucket/warehouse/" + icebergTableName;
            Table table =
                    catalog.createTable(tableId, icebergSchema, PartitionSpec.unpartitioned(), tableLocation, Map.of());

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

            String fileLocation = tableLocation + "/data/data.parquet";
            try {
                table.io().deleteFile(fileLocation);
            } catch (Exception e) {
                logger.debug("File may not exist: {}", e.getMessage());
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

            table.newAppend().appendFile(dataFile).commit();

            long snapshotId = table.currentSnapshot().snapshotId();

            logger.info("Populated Iceberg table with {} records", records.size());
            return new IcebergPopulationResult(icebergSchemaJson, snapshotId);
        }
    }

    public static class PartitionedIcebergResult {
        public String icebergSchema;
        public Map<Integer, Long> partitionSnapshots;

        public PartitionedIcebergResult(String icebergSchema, Map<Integer, Long> partitionSnapshots) {
            this.icebergSchema = icebergSchema;
            this.partitionSnapshots = partitionSnapshots;
        }
    }

    public static PartitionedIcebergResult populatePartitionedIceberg(
            String minioEndpoint, String minioAccessKey, String minioSecretKey, String icebergTableName)
            throws Exception {
        System.setProperty("aws.region", "us-east-1");

        createMinIOBucket(minioEndpoint, minioAccessKey, minioSecretKey);

        org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "address", Types.StringType.get()));

        String icebergSchemaJson = SchemaParser.toJson(icebergSchema);

        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "http://localhost:9001/iceberg");
        properties.put("s3.endpoint", minioEndpoint);
        properties.put("s3.access-key-id", minioAccessKey);
        properties.put("s3.secret-access-key", minioSecretKey);
        properties.put("s3.path-style-access", "true");
        properties.put("s3.region", "us-east-1");

        try (RESTCatalog catalog = new RESTCatalog()) {
            catalog.initialize("rest", properties);

            Namespace namespace = Namespace.of("default");
            if (!catalog.namespaceExists(namespace)) {
                catalog.createNamespace(namespace);
            }

            TableIdentifier tableId = TableIdentifier.of(namespace, icebergTableName);

            if (catalog.tableExists(tableId)) {
                catalog.dropTable(tableId);
            }

            String tableLocation = "s3a://iceberg-bucket/warehouse/" + icebergTableName;
            PartitionSpec partitionSpec =
                    PartitionSpec.builderFor(icebergSchema).identity("id").build();
            Table table = catalog.createTable(tableId, icebergSchema, partitionSpec, tableLocation, Map.of());

            Map<Integer, Long> partitionSnapshots = new HashMap<>();

            int[] ids = {1, 2, 3};
            String[] names = {"PartitionedAlice", "PartitionedBob", "PartitionedCharlie"};
            String[] addresses = {"123 Partition St", "456 Filter Ave", "789 Identity Ln"};

            for (int i = 0; i < ids.length; i++) {
                List<Record> records = new ArrayList<>();
                org.apache.iceberg.data.GenericRecord record =
                        org.apache.iceberg.data.GenericRecord.create(icebergSchema);
                record.setField("id", ids[i]);
                record.setField("name", names[i]);
                record.setField("address", addresses[i]);
                records.add(record);

                String fileLocation = tableLocation + "/data/id=" + ids[i] + "/data" + i + ".parquet";
                try {
                    table.io().deleteFile(fileLocation);
                } catch (Exception e) {
                    logger.debug("File may not exist: {}", e.getMessage());
                }

                OutputFile outputFile = table.io().newOutputFile(fileLocation);
                FileAppenderFactory<Record> factory = new GenericAppenderFactory(table.schema());
                FileAppender<Record> appender = factory.newAppender(outputFile, FileFormat.PARQUET);

                for (Record record2 : records) {
                    appender.add(record2);
                }
                appender.close();

                DataFile dataFile = DataFiles.builder(table.spec())
                        .withInputFile(table.io().newInputFile(fileLocation))
                        .withMetrics(appender.metrics())
                        .withFormat(FileFormat.PARQUET)
                        .withPartitionPath("id=" + ids[i])
                        .build();

                table.newAppend().appendFile(dataFile).commit();

                long snapshotId = table.currentSnapshot().snapshotId();
                partitionSnapshots.put(ids[i], snapshotId);

                logger.info("Added record to partition id={} with snapshot {}", ids[i], snapshotId);
            }

            logger.info("Populated partitioned Iceberg table with {} partitions", ids.length);
            return new PartitionedIcebergResult(icebergSchemaJson, partitionSnapshots);
        }
    }

    public static long populatePostgres(String jdbcUrl, String username, String password, String tableName)
            throws Exception {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(jdbcUrl, username, password)) {
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS temporal_tables");
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute(
                        "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT PRIMARY KEY, name TEXT, address TEXT)");
                logger.info("Created PostgreSQL table: {}", tableName);
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute(
                        "ALTER TABLE " + tableName
                                + " ADD COLUMN IF NOT EXISTS sys_period tstzrange NOT NULL DEFAULT tstzrange(CURRENT_TIMESTAMP, NULL)");
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName + "_temporal (LIKE " + tableName + ")");
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TRIGGER IF EXISTS versioning_trigger ON " + tableName);
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TRIGGER versioning_trigger "
                        + "BEFORE INSERT OR UPDATE OR DELETE ON " + tableName + " "
                        + "FOR EACH ROW EXECUTE PROCEDURE versioning('sys_period', '" + tableName
                        + "_temporal', true)");
            }

            String insertSql = "INSERT INTO " + tableName + " (id, name, address) VALUES (?, ?, ?)";
            try (java.sql.PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "PostgresAlice");
                pstmt.setString(3, "123 Postgres St");
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.setString(2, "PostgresBob");
                pstmt.setString(3, "456 Temporal Ave");
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.setString(2, "PostgresCharlie");
                pstmt.setString(3, "789 Table Ln");
                pstmt.addBatch();

                pstmt.executeBatch();
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("DROP VIEW IF EXISTS " + tableName + "_view");
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + tableName + "_view AS SELECT id, name, address, "
                        + "lower(sys_period) AS tsstart, upper(sys_period) AS tsend FROM " + tableName);
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("DROP VIEW IF EXISTS " + tableName + "_temporal_view");
            }

            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + tableName + "_temporal_view AS SELECT id, name, address, "
                        + "lower(sys_period) AS tsstart, upper(sys_period) AS tsend FROM " + tableName + "_temporal");
            }

            long timestamp = System.currentTimeMillis();
            logger.info("Populated PostgreSQL table with 3 records and created temporal views");
            return timestamp;
        }
    }

    private static void createMinIOBucket(String endpoint, String accessKey, String secretKey) {
        try {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
            S3Client s3Client = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .forcePathStyle(true)
                    .build();

            try {
                HeadBucketRequest headRequest =
                        HeadBucketRequest.builder().bucket("iceberg-bucket").build();
                s3Client.headBucket(headRequest);
            } catch (Exception e) {
                CreateBucketRequest createRequest =
                        CreateBucketRequest.builder().bucket("iceberg-bucket").build();
                s3Client.createBucket(createRequest);
                logger.info("Created MinIO bucket: iceberg-bucket");

                String policyJson = """
          {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                  "arn:aws:s3:::iceberg-bucket",
                  "arn:aws:s3:::iceberg-bucket/*"
                ]
              }
            ]
          }
          """;

                PutBucketPolicyRequest policyRequest = PutBucketPolicyRequest.builder()
                        .bucket("iceberg-bucket")
                        .policy(policyJson)
                        .build();

                s3Client.putBucketPolicy(policyRequest);
                logger.info("Set public policy for MinIO bucket");
            }

            s3Client.close();
        } catch (Exception e) {
            logger.warn("Failed to create MinIO bucket: {}", e.getMessage());
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
