// Copyright (c) 2025
package org.dataharness.service;

import io.grpc.stub.StreamObserver;
import org.dataharness.db.HibernateSessionManager;
import org.dataharness.entity.DataHarnessTable;
import org.dataharness.entity.IcebergSourceEntity;
import org.dataharness.entity.KafkaSourceEntity;
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

    Transaction transaction = null;
    try (Session session = HibernateSessionManager.getSession()) {
      transaction = session.beginTransaction();

      String tableName = request.getName();

      if (tableName.isEmpty()) {
        transaction.rollback();
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      DataHarnessTable table = new DataHarnessTable(tableName);
      session.persist(table);
      transaction.commit();

      CreateTableResponse response = CreateTableResponse.newBuilder().setSuccess(true)
        .setMessage("Table created successfully").setTableId(table.getId()).build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      if (transaction != null) {
        transaction.rollback();
      }
      logger.error("Error creating table", e);
      responseObserver.onError(e);
    }
  }

  /**
   * Upserts a Kafka or Iceberg source into the database. For Kafka sources: uses
   * (table_id, topic_name, partition_number) as the unique key For Iceberg
   * sources: uses (table_id, table_name) as the unique key
   *
   * @param request          The upsert source request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void upsertSource(UpsertSourceRequest request, StreamObserver<UpsertSourceResponse> responseObserver) {

    Transaction transaction = null;
    try (Session session = HibernateSessionManager.getSession()) {
      transaction = session.beginTransaction();

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

      long tableId = table.getId();

      if (request.hasKafkaSource()) {
        KafkaSourceMessage kafkaMsg = request.getKafkaSource();
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
      } else if (request.hasIcebergSource()) {
        IcebergSourceMessage icebergMsg = request.getIcebergSource();
        IcebergSourceEntity existing = findIcebergSource(session, tableId, icebergMsg.getTableName());

        if (existing != null) {
          existing.setTrinoCatalogName(icebergMsg.getTrinoCatalogName());
          existing.setTrinoSchemaName(icebergMsg.getTrinoSchemaName());
          existing.setReadTimestamp(icebergMsg.getReadTimestamp());
          session.merge(existing);
        } else {
          IcebergSourceEntity entity = new IcebergSourceEntity(tableId, icebergMsg.getTrinoCatalogName(),
            icebergMsg.getTrinoSchemaName(), icebergMsg.getTableName(), icebergMsg.getReadTimestamp());
          session.persist(entity);
        }
      }

      transaction.commit();

      UpsertSourceResponse response = UpsertSourceResponse.newBuilder().setSuccess(true)
        .setMessage("Source upserted successfully").build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      if (transaction != null) {
        transaction.rollback();
      }
      responseObserver.onError(e);
    }
  }

  /**
   * Fetches all Kafka and Iceberg sources for a given data harness table name.
   *
   * @param request          The fetch sources request
   * @param responseObserver The observer to send the response to
   */
  @Override
  public void fetchSources(FetchSourcesRequest request, StreamObserver<FetchSourcesResponse> responseObserver) {

    try (Session session = HibernateSessionManager.getSession()) {

      String tableName = request.getTableName();

      if (tableName.isEmpty()) {
        responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
        return;
      }

      DataHarnessTable table = findTableByName(session, tableName);
      if (table == null) {
        responseObserver.onError(new IllegalArgumentException("Table not found: " + tableName));
        return;
      }

      long tableId = table.getId();

      List<KafkaSourceEntity> kafkaSources = findAllKafkaSources(session, tableId);
      List<IcebergSourceEntity> icebergSources = findAllIcebergSources(session, tableId);

      FetchSourcesResponse.Builder responseBuilder = FetchSourcesResponse.newBuilder();

      for (KafkaSourceEntity kafka : kafkaSources) {
        KafkaSourceMessage kafkaMsg = KafkaSourceMessage.newBuilder()
          .setTrinoCatalogName(kafka.getTrinoCatalogName()).setTrinoSchemaName(kafka.getTrinoSchemaName())
          .setStartOffset(kafka.getStartOffset()).setEndOffset(kafka.getEndOffset())
          .setPartitionNumber(kafka.getPartitionNumber()).setTopicName(kafka.getTopicName()).build();

        TableSourceMessage sourceMsg = TableSourceMessage.newBuilder().setTableName(tableName)
          .setKafkaSource(kafkaMsg).build();

        responseBuilder.addSources(sourceMsg);
      }

      for (IcebergSourceEntity iceberg : icebergSources) {
        IcebergSourceMessage icebergMsg = IcebergSourceMessage.newBuilder()
          .setTrinoCatalogName(iceberg.getTrinoCatalogName())
          .setTrinoSchemaName(iceberg.getTrinoSchemaName()).setTableName(iceberg.getTableName())
          .setReadTimestamp(iceberg.getReadTimestamp()).build();

        TableSourceMessage sourceMsg = TableSourceMessage.newBuilder().setTableName(tableName)
          .setIcebergSource(icebergMsg).build();

        responseBuilder.addSources(sourceMsg);
      }

      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    } catch (Exception e) {
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
}
