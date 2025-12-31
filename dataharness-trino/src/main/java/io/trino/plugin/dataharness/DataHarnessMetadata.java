package io.trino.plugin.dataharness;

import static io.trino.spi.StandardErrorCode.INVALID_VIEW;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.Descriptors;
import io.trino.decoder.protobuf.ProtobufUtils;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.dataharness.proto.CatalogServiceGrpc;
import org.dataharness.proto.ListTablesRequest;
import org.dataharness.proto.ListTablesResponse;
import org.dataharness.proto.LoadTableRequest;
import org.dataharness.proto.LoadTableResponse;
import org.dataharness.proto.TableSourceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHarnessMetadata implements ConnectorMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataHarnessMetadata.class);
  private static final String DEFAULT_SCHEMA = "default";
  private final TypeManager typeManager;
  private final DataHarnessGrpcClientFactory grpcClientFactory;
  private final AvroSchemaConverter avroSchemaConverter;

  @Inject
  public DataHarnessMetadata(
      TypeManager typeManager, DataHarnessGrpcClientFactory grpcClientFactory) {
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
    this.grpcClientFactory = requireNonNull(grpcClientFactory, "grpcClientFactory is null");
    this.avroSchemaConverter =
        new AvroSchemaConverter(typeManager, AvroSchemaConverter.EmptyFieldStrategy.FAIL);
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
  public Optional<ConnectorViewDefinition> getView(
      ConnectorSession session, SchemaTableName viewName) {
    try {
      CatalogServiceGrpc.CatalogServiceBlockingStub stub = grpcClientFactory.getStub();
      LoadTableRequest request =
          LoadTableRequest.newBuilder().setTableName(viewName.getTableName()).build();
      LoadTableResponse response = stub.loadTable(request);

      List<ConnectorViewDefinition.ViewColumn> viewColumns =
          extractViewColumns(viewName.getTableName(), response);
      List<String> columnNames =
          viewColumns.stream().map(ConnectorViewDefinition.ViewColumn::getName).toList();

      String viewDefinition = buildViewDefinition(viewName.getTableName(), response, columnNames);

      return Optional.of(
          new ConnectorViewDefinition(
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
          TABLE_NOT_FOUND,
          "Failed to load table '" + viewName.getTableName() + "': " + e.getMessage(),
          e);
    }
  }

  private List<ConnectorViewDefinition.ViewColumn> extractViewColumns(
      String tableName, LoadTableResponse response) {
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
      return extractColumnsFromProtobufSchema(protobufSchemaStr);
    }

    throw new TrinoException(
        INVALID_VIEW,
        "Table '" + tableName + "' does not have an avro, iceberg, or protobuf schema");
  }

  private List<ConnectorViewDefinition.ViewColumn> extractColumnsFromAvroSchema(
      String avroSchemaStr) {
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

  private List<ConnectorViewDefinition.ViewColumn> extractColumnsFromIcebergSchema(
      String icebergSchemaStr) {
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

  private List<ConnectorViewDefinition.ViewColumn> extractColumnsFromProtobufSchema(
      String protobufSchemaStr) {
    try {
      Descriptors.FileDescriptor fileDescriptor =
          ProtobufUtils.getFileDescriptor(protobufSchemaStr);
      List<Descriptors.Descriptor> messageTypes = fileDescriptor.getMessageTypes();

      if (messageTypes.isEmpty()) {
        throw new TrinoException(INVALID_VIEW, "Protobuf schema contains no message types");
      }

      Descriptors.Descriptor mainMessage = messageTypes.get(0);
      ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> viewColumns =
          ImmutableList.builder();

      for (Descriptors.FieldDescriptor fieldDescriptor : mainMessage.getFields()) {
        String columnName = fieldDescriptor.getName();
        Type trinoType = protobufFieldToTrinoType(fieldDescriptor);
        TypeId typeId = trinoType.getTypeId();
        viewColumns.add(
            new ConnectorViewDefinition.ViewColumn(columnName, typeId, Optional.empty()));
      }

      return viewColumns.build();
    } catch (TrinoException e) {
      throw e;
    } catch (Exception e) {
      throw new TrinoException(
          INVALID_VIEW, "Failed to parse protobuf schema: " + e.getMessage(), e);
    }
  }

  private Type protobufFieldToTrinoType(Descriptors.FieldDescriptor fieldDescriptor) {
    return switch (fieldDescriptor.getJavaType()) {
      case BOOLEAN -> typeManager.getType(new TypeSignature(StandardTypes.BOOLEAN));
      case INT -> typeManager.getType(new TypeSignature(StandardTypes.INTEGER));
      case LONG -> typeManager.getType(new TypeSignature(StandardTypes.BIGINT));
      case FLOAT -> typeManager.getType(new TypeSignature(StandardTypes.REAL));
      case DOUBLE -> typeManager.getType(new TypeSignature(StandardTypes.DOUBLE));
      case BYTE_STRING -> typeManager.getType(new TypeSignature(StandardTypes.VARBINARY));
      case STRING, ENUM -> VarcharType.createUnboundedVarcharType();
      case MESSAGE -> typeManager.getType(new TypeSignature(StandardTypes.JSON));
    };
  }

  private String buildViewDefinition(
      String tableName, LoadTableResponse response, List<String> columnNames) {
    LOGGER.info("Table '{}' has {} sources", tableName, response.getSourcesCount());

    String columnList =
        columnNames.isEmpty()
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
    }

    List<String> queryList = queries.build();
    if (queryList.isEmpty()) {
      throw new TrinoException(
          INVALID_VIEW, "Table '" + tableName + "' does not have any sources configured");
    }

    if (queryList.size() == 1) {
      return queryList.get(0);
    }

    return String.join(" UNION ALL ", queryList);
  }

  private String buildKafkaQuery(
      org.dataharness.proto.KafkaSourceMessage kafkaSource, String columnList) {
    return String.format(
        "SELECT %s FROM \"%s\".\"%s\".\"%s\" WHERE _partition_id = %d AND _partition_offset >= %d AND _partition_offset <= %d",
        columnList,
        kafkaSource.getTrinoCatalogName(),
        kafkaSource.getTrinoSchemaName(),
        kafkaSource.getTopicName(),
        kafkaSource.getPartitionNumber(),
        kafkaSource.getStartOffset(),
        kafkaSource.getEndOffset());
  }

  private String buildIcebergQuery(
      org.dataharness.proto.IcebergSourceMessage icebergSource, String columnList) {
    return String.format(
        "SELECT %s FROM \"%s\".\"%s\".\"%s\" FOR VERSION AS OF %d",
        columnList,
        icebergSource.getTrinoCatalogName(),
        icebergSource.getTrinoSchemaName(),
        icebergSource.getTableName(),
        icebergSource.getReadTimestamp());
  }

  private String quoteIdentifier(String name) {
    return "\"" + name + "\"";
  }
}
