// Copyright (c) 2025
package org.dataharness.service;

import io.grpc.stub.StreamObserver;
import org.dataharness.db.HibernateSessionManager;
import org.dataharness.entity.DataHarnessTable;
import org.dataharness.entity.IcebergSourceEntity;
import org.dataharness.entity.KafkaSourceEntity;
import org.dataharness.entity.YugabyteSourceEntity;
import org.dataharness.proto.*;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of the CatalogService gRPC service. Provides operations for
 * managing data source catalogs.
 */
public class CatalogServiceImpl extends CatalogServiceGrpc.CatalogServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(CatalogServiceImpl.class);

  /**
   * Creates a new DataHarnessTable with the given name.
   *
   * @param request          The create table request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void createTable(CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {

    try (Session session = HibernateSessionManager.getSession()) {
      Transaction transaction = session.beginTransaction();

      String tableName = request.getName();

      if (tableName.isEmpty()) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      try {
        DataHarnessTable table = new DataHarnessTable(tableName);
        session.persist(table);
        transaction.commit();

        CreateTableResponse response = CreateTableResponse.newBuilder().setSuccess(true)
          .setMessage("Table created successfully").setTableId(table.getId()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        try {
          if (transaction.isActive()) {
            transaction.rollback();
          }
        } catch (Exception rollbackEx) {
          logger.warn("Error rolling back transaction", rollbackEx);
        }
        logger.error("Error creating table", e);
        responseObserver.onError(e);
      }
    } catch (Exception e) {
      logger.error("Error creating table - session failure", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Upserts multiple Kafka and/or Iceberg sources into the database atomically.
   * For Kafka sources: uses (table_id, topic_name, partition_number) as the unique key
   * For Iceberg sources: uses (table_id, table_name) as the unique key
   *
   * @param request          The upsert sources request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void upsertSources(UpsertSourcesRequest request, StreamObserver<UpsertSourcesResponse> responseObserver) {

    try (Session session = HibernateSessionManager.getSession()) {
      Transaction transaction = session.beginTransaction();

      if (request.getSourcesList().isEmpty()) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("At least one source must be provided"));
        return;
      }

      try {
        for (SourceUpdate sourceUpdate : request.getSourcesList()) {
          String tableName = sourceUpdate.getTableName();

          if (tableName.isEmpty()) {
            transaction.rollback();
            responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
            return;
          }

          DataHarnessTable table = findTableByName(session, tableName);
          if (table == null) {
            transaction.rollback();
            responseObserver.onError(new IllegalArgumentException("Table not found: " + tableName));
            return;
          }

          long tableId = table.getId();

          if (sourceUpdate.hasKafkaSource()) {
            KafkaSourceMessage kafkaMsg = sourceUpdate.getKafkaSource();
            KafkaSourceEntity existing = findKafkaSource(session, tableId, kafkaMsg.getTopicName(),
              kafkaMsg.getPartitionNumber());

            if (existing != null) {
              existing.setTrinoCatalogName(kafkaMsg.getTrinoCatalogName());
              existing.setTrinoSchemaName(kafkaMsg.getTrinoSchemaName());
              existing.setStartOffset(kafkaMsg.getStartOffset());
              existing.setEndOffset(kafkaMsg.getEndOffset());
              session.merge(existing);
            } else {
              KafkaSourceEntity entity = new KafkaSourceEntity(tableId, kafkaMsg.getTrinoCatalogName(),
                kafkaMsg.getTrinoSchemaName(), kafkaMsg.getStartOffset(), kafkaMsg.getEndOffset(),
                kafkaMsg.getPartitionNumber(), kafkaMsg.getTopicName());
              session.persist(entity);
            }
          } else if (sourceUpdate.hasIcebergSource()) {
            IcebergSourceMessage icebergMsg = sourceUpdate.getIcebergSource();
            IcebergSourceEntity existing = findIcebergSource(session, tableId, icebergMsg.getTableName());

            if (existing != null) {
              existing.setTrinoCatalogName(icebergMsg.getTrinoCatalogName());
              existing.setTrinoSchemaName(icebergMsg.getTrinoSchemaName());
              existing.setReadTimestamp(icebergMsg.getReadTimestamp());
              existing.setSparkCatalogName(icebergMsg.getSparkCatalogName());
              existing.setSparkSchemaName(icebergMsg.getSparkSchemaName());
              session.merge(existing);
            } else {
              IcebergSourceEntity entity = new IcebergSourceEntity(tableId, icebergMsg.getTrinoCatalogName(),
                icebergMsg.getTrinoSchemaName(), icebergMsg.getTableName(), icebergMsg.getReadTimestamp(),
                icebergMsg.getSparkCatalogName(), icebergMsg.getSparkSchemaName());
              session.persist(entity);
            }
          } else if (sourceUpdate.hasYugabytedbSource()) {
            YugabyteDBSourceMessage yugabyteMsg = sourceUpdate.getYugabytedbSource();
            YugabyteSourceEntity existing = findYugabyteSource(session, tableId, yugabyteMsg.getTableName());

            if (existing != null) {
              existing.setTrinoCatalogName(yugabyteMsg.getTrinoCatalogName());
              existing.setTrinoSchemaName(yugabyteMsg.getTrinoSchemaName());
              existing.setJdbcUrl(yugabyteMsg.getJdbcUrl());
              existing.setUsername(yugabyteMsg.getUsername());
              existing.setPassword(yugabyteMsg.getPassword());
              existing.setReadTimestamp(yugabyteMsg.getReadTimestamp());
              session.merge(existing);
            } else {
              YugabyteSourceEntity entity = new YugabyteSourceEntity(tableId, yugabyteMsg.getTrinoCatalogName(),
                yugabyteMsg.getTrinoSchemaName(), yugabyteMsg.getTableName(), yugabyteMsg.getJdbcUrl(),
                yugabyteMsg.getUsername(), yugabyteMsg.getPassword(), yugabyteMsg.getReadTimestamp());
              session.persist(entity);
            }
          }
        }

        transaction.commit();

        UpsertSourcesResponse response = UpsertSourcesResponse.newBuilder().setSuccess(true)
          .setMessage("Sources upserted successfully").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        try {
          if (transaction.isActive()) {
            transaction.rollback();
          }
        } catch (Exception rollbackEx) {
          logger.warn("Error rolling back transaction", rollbackEx);
        }
        logger.error("Error upserting sources", e);
        responseObserver.onError(e);
      }
    } catch (Exception e) {
      logger.error("Error upserting sources - session failure", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Loads a table with its schema and sources for a given data harness table name.
   *
   * @param request          The load table request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void loadTable(LoadTableRequest request, StreamObserver<LoadTableResponse> responseObserver) {

    try (Session session = HibernateSessionManager.getSession()) {
      Transaction transaction = session.beginTransaction();

      String tableName = request.getTableName();

      if (tableName.isEmpty()) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      DataHarnessTable table = findTableByName(session, tableName);
      if (table == null) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table not found: " + tableName));
        return;
      }

      try {
        long tableId = table.getId();

        List<KafkaSourceEntity> kafkaSources = findAllKafkaSources(session, tableId);
        List<IcebergSourceEntity> icebergSources = findAllIcebergSources(session, tableId);
        List<YugabyteSourceEntity> yugabyteSources = findAllYugabyteSources(session, tableId);

        transaction.commit();

        LoadTableResponse.Builder responseBuilder = LoadTableResponse.newBuilder();
        if (table.getAvroSchema() != null) {
          responseBuilder.setAvroSchema(table.getAvroSchema());
        }
        if (table.getIcebergSchema() != null) {
          responseBuilder.setIcebergSchema(table.getIcebergSchema());
        }
        if (table.getProtobufSchema() != null) {
          responseBuilder.setProtobufSchema(table.getProtobufSchema());
        }

        for (KafkaSourceEntity kafka : kafkaSources) {
          KafkaSourceMessage kafkaMsg = KafkaSourceMessage.newBuilder()
            .setTrinoCatalogName(kafka.getTrinoCatalogName()).setTrinoSchemaName(kafka.getTrinoSchemaName())
            .setStartOffset(kafka.getStartOffset()).setEndOffset(kafka.getEndOffset())
            .setPartitionNumber(kafka.getPartitionNumber()).setTopicName(kafka.getTopicName()).build();

          TableSourceMessage sourceMessage = TableSourceMessage.newBuilder().setTableName(tableName)
            .setKafkaSource(kafkaMsg).build();

          responseBuilder.addSources(sourceMessage);
        }

        for (IcebergSourceEntity iceberg : icebergSources) {
          IcebergSourceMessage icebergMsg = IcebergSourceMessage.newBuilder()
            .setTrinoCatalogName(iceberg.getTrinoCatalogName())
            .setTrinoSchemaName(iceberg.getTrinoSchemaName()).setTableName(iceberg.getTableName())
            .setReadTimestamp(iceberg.getReadTimestamp())
            .setSparkCatalogName(iceberg.getSparkCatalogName() != null ? iceberg.getSparkCatalogName() : "")
            .setSparkSchemaName(iceberg.getSparkSchemaName() != null ? iceberg.getSparkSchemaName() : "")
            .build();

          TableSourceMessage sourceMessage = TableSourceMessage.newBuilder().setTableName(tableName)
            .setIcebergSource(icebergMsg).build();

          responseBuilder.addSources(sourceMessage);
        }

        for (YugabyteSourceEntity yugabyte : yugabyteSources) {
          YugabyteDBSourceMessage yugabyteMsg = YugabyteDBSourceMessage.newBuilder()
            .setTrinoCatalogName(yugabyte.getTrinoCatalogName())
            .setTrinoSchemaName(yugabyte.getTrinoSchemaName())
            .setTableName(yugabyte.getTableName())
            .setJdbcUrl(yugabyte.getJdbcUrl())
            .setUsername(yugabyte.getUsername())
            .setPassword(yugabyte.getPassword())
            .setReadTimestamp(yugabyte.getReadTimestamp()).build();

          TableSourceMessage sourceMessage = TableSourceMessage.newBuilder().setTableName(tableName)
            .setYugabytedbSource(yugabyteMsg).build();

          responseBuilder.addSources(sourceMessage);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
      } catch (Exception e) {
        try {
          if (transaction.isActive()) {
            transaction.rollback();
          }
        } catch (Exception rollbackEx) {
          logger.warn("Error rolling back transaction", rollbackEx);
        }
        logger.error("Error loading table", e);
        responseObserver.onError(e);
      }
    } catch (Exception e) {
      logger.error("Error loading table - session failure", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Sets the Avro and/or Iceberg schema for a DataHarnessTable.
   *
   * @param request          The set schema request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void setSchema(SetSchemaRequest request, StreamObserver<SetSchemaResponse> responseObserver) {
    try (Session session = HibernateSessionManager.getSession()) {
      Transaction transaction = session.beginTransaction();

      String tableName = request.getTableName();

      if (tableName.isEmpty()) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      DataHarnessTable table = findTableByName(session, tableName);
      if (table == null) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table not found: " + tableName));
        return;
      }

      try {
        String avroSchema = request.hasAvroSchema() ? request.getAvroSchema() : null;
        String icebergSchema = request.hasIcebergSchema() ? request.getIcebergSchema() : null;
        String protobufSchema = request.hasProtobufSchema() ? request.getProtobufSchema() : null;
        table.setAvroSchema(avroSchema);
        table.setIcebergSchema(icebergSchema);
        table.setProtobufSchema(protobufSchema);
        session.merge(table);
        transaction.commit();

        SetSchemaResponse response = SetSchemaResponse.newBuilder().setSuccess(true)
          .setMessage("Schema set successfully").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        try {
          if (transaction.isActive()) {
            transaction.rollback();
          }
        } catch (Exception rollbackEx) {
          logger.warn("Error rolling back transaction", rollbackEx);
        }
        logger.error("Error setting schema", e);
        responseObserver.onError(e);
      }
    } catch (Exception e) {
      logger.error("Error setting schema - session failure", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Lists all tables in the DataHarnessTable catalog.
   *
   * @param request          The list tables request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void listTables(ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
    try (Session session = HibernateSessionManager.getSession()) {
      List<DataHarnessTable> tables = findAllTables(session);

      ListTablesResponse.Builder responseBuilder = ListTablesResponse.newBuilder();
      for (DataHarnessTable table : tables) {
        responseBuilder.addTableNames(table.getName());
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("Error listing tables", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Checks if a table exists in the DataHarnessTable catalog.
   *
   * @param request          The table exists request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void tableExists(TableExistsRequest request, StreamObserver<TableExistsResponse> responseObserver) {
    try (Session session = HibernateSessionManager.getSession()) {
      String tableName = request.getTableName();

      if (tableName.isEmpty()) {
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      DataHarnessTable table = findTableByName(session, tableName);
      boolean exists = table != null;

      TableExistsResponse response = TableExistsResponse.newBuilder().setExists(exists).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("Error checking if table exists", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Drops a DataHarnessTable and all of its associated sources.
   *
   * @param request          The drop table request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void dropTable(DropTableRequest request, StreamObserver<DropTableResponse> responseObserver) {
    try (Session session = HibernateSessionManager.getSession()) {
      Transaction transaction = session.beginTransaction();

      String tableName = request.getTableName();

      if (tableName.isEmpty()) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      DataHarnessTable table = findTableByName(session, tableName);
      if (table == null) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table not found: " + tableName));
        return;
      }

      try {
        long tableId = table.getId();

        Query<?> deleteKafkaSources = session.createQuery("DELETE FROM KafkaSourceEntity WHERE tableId = :tableId");
        deleteKafkaSources.setParameter("tableId", tableId);
        deleteKafkaSources.executeUpdate();

        Query<?> deleteIcebergSources = session.createQuery("DELETE FROM IcebergSourceEntity WHERE tableId = :tableId");
        deleteIcebergSources.setParameter("tableId", tableId);
        deleteIcebergSources.executeUpdate();

        Query<?> deleteYugabyteSources = session.createQuery("DELETE FROM YugabyteSourceEntity WHERE tableId = :tableId");
        deleteYugabyteSources.setParameter("tableId", tableId);
        deleteYugabyteSources.executeUpdate();

        session.remove(table);

        transaction.commit();

        DropTableResponse response = DropTableResponse.newBuilder().setSuccess(true)
          .setMessage("Table dropped successfully").build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
      } catch (Exception e) {
        try {
          if (transaction.isActive()) {
            transaction.rollback();
          }
        } catch (Exception rollbackEx) {
          logger.warn("Error rolling back transaction", rollbackEx);
        }
        logger.error("Error dropping table", e);
        responseObserver.onError(e);
      }
    } catch (Exception e) {
      logger.error("Error dropping table - session failure", e);
      responseObserver.onError(e);
    }
  }

  private DataHarnessTable findTableByName(Session session, String tableName) {
    Query<DataHarnessTable> query = session.createQuery("FROM DataHarnessTable WHERE name = :name",
      DataHarnessTable.class);
    query.setParameter("name", tableName);

    return query.uniqueResult();
  }

  private KafkaSourceEntity findKafkaSource(Session session, long tableId, String topicName, int partitionNumber) {
    Query<KafkaSourceEntity> query = session.createQuery(
      "FROM KafkaSourceEntity WHERE tableId = :tableId AND topicName = :topicName AND partitionNumber = :partitionNumber",
      KafkaSourceEntity.class);
    query.setParameter("tableId", tableId);
    query.setParameter("topicName", topicName);
    query.setParameter("partitionNumber", partitionNumber);

    return query.uniqueResult();
  }

  private IcebergSourceEntity findIcebergSource(Session session, long tableId, String tableName) {
    Query<IcebergSourceEntity> query = session.createQuery(
      "FROM IcebergSourceEntity WHERE tableId = :tableId AND tableName = :tableName",
      IcebergSourceEntity.class);
    query.setParameter("tableId", tableId);
    query.setParameter("tableName", tableName);

    return query.uniqueResult();
  }

  private YugabyteSourceEntity findYugabyteSource(Session session, long tableId, String tableName) {
    Query<YugabyteSourceEntity> query = session.createQuery(
      "FROM YugabyteSourceEntity WHERE tableId = :tableId AND tableName = :tableName",
      YugabyteSourceEntity.class);
    query.setParameter("tableId", tableId);
    query.setParameter("tableName", tableName);

    return query.uniqueResult();
  }

  private List<KafkaSourceEntity> findAllKafkaSources(Session session, long tableId) {
    Query<KafkaSourceEntity> query = session.createQuery("FROM KafkaSourceEntity WHERE tableId = :tableId",
      KafkaSourceEntity.class);
    query.setParameter("tableId", tableId);

    return query.getResultList();
  }

  private List<IcebergSourceEntity> findAllIcebergSources(Session session, long tableId) {
    Query<IcebergSourceEntity> query = session.createQuery("FROM IcebergSourceEntity WHERE tableId = :tableId",
      IcebergSourceEntity.class);
    query.setParameter("tableId", tableId);

    return query.getResultList();
  }

  private List<YugabyteSourceEntity> findAllYugabyteSources(Session session, long tableId) {
    Query<YugabyteSourceEntity> query = session.createQuery("FROM YugabyteSourceEntity WHERE tableId = :tableId",
      YugabyteSourceEntity.class);
    query.setParameter("tableId", tableId);

    return query.getResultList();
  }

  private List<DataHarnessTable> findAllTables(Session session) {
    Query<DataHarnessTable> query = session.createQuery("FROM DataHarnessTable", DataHarnessTable.class);
    return query.getResultList();
  }
}
