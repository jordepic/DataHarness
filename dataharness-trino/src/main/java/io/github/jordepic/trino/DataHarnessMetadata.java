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
package io.github.jordepic.trino;

import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.github.jordepic.proto.*;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHarnessMetadata implements ConnectorMetadata {
    public static final int MILLIS_TO_NANOS = 1_000_000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataHarnessMetadata.class);
    private static final String DEFAULT_SCHEMA = "default";
    private final TypeManager typeManager;
    private final DataHarnessGrpcClientFactory grpcClientFactory;
    private final AvroSchemaConverter avroSchemaConverter;

    @Inject
    public DataHarnessMetadata(TypeManager typeManager, DataHarnessGrpcClientFactory grpcClientFactory) {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.grpcClientFactory = requireNonNull(grpcClientFactory, "grpcClientFactory is null");
        this.avroSchemaConverter = new AvroSchemaConverter(typeManager, AvroSchemaConverter.EmptyFieldStrategy.FAIL);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        if (schemaName.isPresent() && !DEFAULT_SCHEMA.equals(schemaName.get())) {
            return ImmutableList.of();
        }

        try {
            CatalogServiceGrpc.CatalogServiceBlockingStub stub = grpcClientFactory.getStub();
            ListTablesRequest request = ListTablesRequest.newBuilder().build();
            ListTablesResponse response = stub.listTables(request);

            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String tableName : response.getTableNamesList()) {
                builder.add(new SchemaTableName(DEFAULT_SCHEMA, tableName));
            }
            return builder.build();
        } catch (Exception e) {
            LOGGER.warn("Failed to fetch tables from data harness: {}", e.getMessage(), e);
            return ImmutableList.of();
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName) {
        try {
            CatalogServiceGrpc.CatalogServiceBlockingStub stub = grpcClientFactory.getStub();
            LoadTableRequest request = LoadTableRequest.newBuilder()
                    .setTableName(viewName.getTableName())
                    .build();
            LoadTableResponse response = stub.loadTable(request);

            List<ConnectorViewDefinition.ViewColumn> viewColumns =
                    extractViewColumns(viewName.getTableName(), response);
            List<String> columnNames = viewColumns.stream()
                    .map(ConnectorViewDefinition.ViewColumn::getName)
                    .toList();

            String viewDefinition = buildViewDefinition(viewName.getTableName(), response, columnNames);

            return Optional.of(new ConnectorViewDefinition(
                    viewDefinition,
                    Optional.empty(),
                    Optional.empty(),
                    viewColumns,
                    Optional.empty(),
                    Optional.of("data_harness"),
                    false,
                    ImmutableList.of()));
        } catch (TrinoException e) {
            throw e;
        } catch (Exception e) {
            throw new TrinoException(
                    TABLE_NOT_FOUND, "Failed to load table '" + viewName.getTableName() + "': " + e.getMessage(), e);
        }
    }

    private List<ConnectorViewDefinition.ViewColumn> extractViewColumns(String tableName, LoadTableResponse response) {
        String avroSchemaStr = response.getAvroSchema();
        if (!avroSchemaStr.isEmpty()) {
            return extractColumnsFromAvroSchema(avroSchemaStr);
        }

        String icebergSchemaStr = response.getIcebergSchema();
        if (!icebergSchemaStr.isEmpty()) {
            return extractColumnsFromIcebergSchema(icebergSchemaStr);
        }

        String protobufSchemaStr = response.getProtobufSchema();
        if (!protobufSchemaStr.isEmpty()) {
            throw new TrinoException(
                    INVALID_VIEW,
                    "Table '" + tableName + "' only specifies a protobuf schema, which is not yet supported by Trino");
        }

        throw new TrinoException(
                INVALID_VIEW, "Table '" + tableName + "' does not have an avro, iceberg, or protobuf schema");
    }

    private List<ConnectorViewDefinition.ViewColumn> extractColumnsFromAvroSchema(String avroSchemaStr) {
        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        List<Type> columnTypes = avroSchemaConverter.convertAvroSchema(avroSchema);
        List<Schema.Field> fields = avroSchema.getFields();

        ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> viewColumns = ImmutableList.builder();
        for (int i = 0; i < columnTypes.size() && i < fields.size(); i++) {
            String columnName = fields.get(i).name();
            TypeId typeId = columnTypes.get(i).getTypeId();
            viewColumns.add(new ConnectorViewDefinition.ViewColumn(columnName, typeId, Optional.empty()));
        }
        return viewColumns.build();
    }

    private List<ConnectorViewDefinition.ViewColumn> extractColumnsFromIcebergSchema(String icebergSchemaStr) {
        org.apache.iceberg.Schema icebergSchema = SchemaParser.fromJson(icebergSchemaStr);
        ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> viewColumns = ImmutableList.builder();
        for (Types.NestedField field : icebergSchema.columns()) {
            String columnName = field.name();
            Type trinoType = TypeConverter.toTrinoType(field.type(), typeManager);
            TypeId typeId = trinoType.getTypeId();
            viewColumns.add(new ConnectorViewDefinition.ViewColumn(columnName, typeId, Optional.empty()));
        }
        return viewColumns.build();
    }

    private String buildViewDefinition(String tableName, LoadTableResponse response, List<String> columnNames) {
        LOGGER.info("Table '{}' has {} sources", tableName, response.getSourcesCount());

        String columnList = columnNames.isEmpty()
                ? "*"
                : columnNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));

        ImmutableList.Builder<String> queries = ImmutableList.builder();

        for (TableSourceMessage source : response.getSourcesList()) {
            if (source.hasKafkaSource()) {
                String kafkaQuery = buildKafkaQuery(source.getKafkaSource(), columnList);
                LOGGER.info("Generated Kafka query for table '{}': {}", tableName, kafkaQuery);
                queries.add(kafkaQuery);
            }
            if (source.hasIcebergSource()) {
                String icebergQuery = buildIcebergQuery(source.getIcebergSource(), columnList);
                LOGGER.info("Generated Iceberg query for table '{}': {}", tableName, icebergQuery);
                queries.add(icebergQuery);
            }
            if (source.hasPostgresdbSource()) {
                String postgresQuery = buildPostgresQuery(source.getPostgresdbSource(), columnList);
                LOGGER.info("Generated Postgres query for table '{}': {}", tableName, postgresQuery);
                queries.add(postgresQuery);
            }
        }

        List<String> queryList = queries.build();
        if (queryList.isEmpty()) {
            throw new TrinoException(INVALID_VIEW, "Table '" + tableName + "' does not have any sources configured");
        }

        if (queryList.size() == 1) {
            return queryList.get(0);
        }

        return String.join(" UNION ALL ", queryList);
    }

    private String buildKafkaQuery(io.github.jordepic.proto.KafkaSourceMessage kafkaSource, String columnList) {
        String baseQuery = String.format(
                "SELECT %s FROM \"%s\".\"%s\".\"%s\" WHERE _partition_id = %d AND _partition_offset >= %d AND _partition_offset <= %d",
                columnList,
                kafkaSource.getTrinoCatalogName(),
                kafkaSource.getTrinoSchemaName(),
                kafkaSource.getTopicName(),
                kafkaSource.getPartitionNumber(),
                kafkaSource.getStartOffset(),
                kafkaSource.getEndOffset());

        if (!kafkaSource.getPartitionFilter().isEmpty()) {
            return baseQuery + " AND (" + kafkaSource.getPartitionFilter() + ")";
        }
        return baseQuery;
    }

    private String buildIcebergQuery(io.github.jordepic.proto.IcebergSourceMessage icebergSource, String columnList) {
        String baseQuery = String.format(
                "SELECT %s FROM \"%s\".\"%s\".\"%s\" FOR VERSION AS OF %d",
                columnList,
                icebergSource.getTrinoCatalogName(),
                icebergSource.getTrinoSchemaName(),
                icebergSource.getTableName(),
                icebergSource.getReadTimestamp());

        if (!icebergSource.getPartitionFilter().isEmpty()) {
            return baseQuery + " WHERE " + icebergSource.getPartitionFilter();
        }
        return baseQuery;
    }

    private String buildPostgresQuery(
            io.github.jordepic.proto.PostgresDBSourceMessage postgresSource, String columnList) {
        String catalog = postgresSource.getTrinoCatalogName();
        String schema = postgresSource.getTrinoSchemaName();
        String tableName = postgresSource.getTableNameNoTstzrange();
        String historyTableName = postgresSource.getHistoryTableNameNoTstzrange();
        long readTimestamp = postgresSource.getReadTimestamp();

        long readTimestampNanos = readTimestamp * MILLIS_TO_NANOS;

        String baseQuery = String.format(
                "SELECT %s FROM (SELECT * FROM \"%s\".\"%s\".\"%s\" UNION ALL SELECT * FROM \"%s\".\"%s\".\"%s\") AS combined WHERE tsstart <= from_unixtime_nanos(%d) AND (tsend IS NULL OR tsend > from_unixtime_nanos(%d))",
                columnList,
                catalog,
                schema,
                tableName,
                catalog,
                schema,
                historyTableName,
                readTimestampNanos,
                readTimestampNanos);

        if (!postgresSource.getPartitionFilter().isEmpty()) {
            return baseQuery + " AND (" + postgresSource.getPartitionFilter() + ")";
        }
        return baseQuery;
    }

    private String quoteIdentifier(String name) {
        return "\"" + name + "\"";
    }
}
