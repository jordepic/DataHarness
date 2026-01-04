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
// Copyright (c) 2025
package org.dataharness.service;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.dataharness.db.HibernateSessionManager;
import org.dataharness.entity.*;
import org.dataharness.proto.*;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the CatalogService gRPC service. Provides operations for managing data source
 * catalogs.
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
            String tableName = request.getName();

            if (tableName.isEmpty()) {
                responseObserver.onError(new IllegalArgumentException("Table name cannot be null or empty"));
                return;
            }

            Transaction transaction = session.beginTransaction();
            DataHarnessTable table = new DataHarnessTable(tableName);
            session.persist(table);
            transaction.commit();

            CreateTableResponse response = CreateTableResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Table created successfully")
                    .setTableId(table.getId())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error creating table", e);
            responseObserver.onError(e);
        }
    }

    /**
     * Upserts multiple Kafka and/or Iceberg sources into the database atomically. For Kafka sources:
     * uses (table_id, topic_name, partition_number) as the unique key For Iceberg sources: uses
     * (table_id, table_name) as the unique key. All modifier fields must match the source's current
     * modifier. After successful update, the modifier is reset to empty string.
     *
     * @param request          The upsert sources request
     * @param responseObserver The observer to send the response to
     */
    @Override
    public void upsertSources(UpsertSourcesRequest request, StreamObserver<UpsertSourcesResponse> responseObserver) {

        try (Session session = HibernateSessionManager.getSession()) {
            if (request.getSourcesList().isEmpty()) {
                responseObserver.onError(new IllegalArgumentException("At least one source must be provided"));
                return;
            }

            Transaction transaction = null;
            try {
                transaction = session.beginTransaction();

                List<SourceEntity> sourcesToUpdate = new ArrayList<>();

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
                        validateSourceModifier(
                                session, KafkaSourceEntity.class, tableId, kafkaMsg.getName(), kafkaMsg.getModifier());
                        SourceEntity source = upsertKafkaSourceAndReturn(session, tableId, kafkaMsg);
                        sourcesToUpdate.add(source);
                    } else if (sourceUpdate.hasIcebergSource()) {
                        IcebergSourceMessage icebergMsg = sourceUpdate.getIcebergSource();
                        validateSourceModifier(
                                session,
                                IcebergSourceEntity.class,
                                tableId,
                                icebergMsg.getName(),
                                icebergMsg.getModifier());
                        SourceEntity source = upsertIcebergSourceAndReturn(session, tableId, icebergMsg);
                        sourcesToUpdate.add(source);
                    } else if (sourceUpdate.hasYugabytedbSource()) {
                        YugabyteDBSourceMessage yugabyteMsg = sourceUpdate.getYugabytedbSource();
                        validateSourceModifier(
                                session,
                                YugabyteSourceEntity.class,
                                tableId,
                                yugabyteMsg.getName(),
                                yugabyteMsg.getModifier());
                        SourceEntity source = upsertYugabyteSourceAndReturn(session, tableId, yugabyteMsg);
                        sourcesToUpdate.add(source);
                    } else if (sourceUpdate.hasPostgresdbSource()) {
                        PostgresDBSourceMessage postgresMsg = sourceUpdate.getPostgresdbSource();
                        validateSourceModifier(
                                session,
                                PostgresSourceEntity.class,
                                tableId,
                                postgresMsg.getName(),
                                postgresMsg.getModifier());
                        SourceEntity source = upsertPostgresSourceAndReturn(session, tableId, postgresMsg);
                        sourcesToUpdate.add(source);
                    }
                }

                for (SourceEntity source : sourcesToUpdate) {
                    source.setModifier("");
                    session.merge(source);
                }

                transaction.commit();

                UpsertSourcesResponse response = UpsertSourcesResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Sources upserted successfully")
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                if (transaction != null && transaction.isActive()) {
                    transaction.rollback();
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
                List<PostgresSourceEntity> postgresSources = findAllPostgresSources(session, tableId);

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

                addKafkaSourcestoResponse(kafkaSources, tableName, responseBuilder);
                addIcebergSourcesToResponse(icebergSources, tableName, responseBuilder);
                addYugabyteSourcesToResponse(yugabyteSources, tableName, responseBuilder);
                addPostgresSourcesToResponse(postgresSources, tableName, responseBuilder);

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

                SetSchemaResponse response = SetSchemaResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Schema set successfully")
                        .build();

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

            TableExistsResponse response =
                    TableExistsResponse.newBuilder().setExists(exists).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error checking if table exists", e);
            responseObserver.onError(e);
        }
    }

    /**
     * Claims (locks) multiple sources by setting their modifier field. Only succeeds if all sources
     * currently have an empty modifier field. The operation is atomic - either all sources are
     * claimed or none are.
     *
     * @param request          The claim sources request
     * @param responseObserver The observer to send the response to
     */
    @Override
    public void claimSources(ClaimSourcesRequest request, StreamObserver<ClaimSourcesResponse> responseObserver) {
        try (Session session = HibernateSessionManager.getSession()) {
            if (request.getSourcesList().isEmpty()) {
                responseObserver.onError(new IllegalArgumentException("At least one source must be provided"));
                return;
            }

            Transaction transaction = null;
            try {
                transaction = session.beginTransaction();

                for (SourceToClaim sourceInfo : request.getSourcesList()) {
                    String sourceName = sourceInfo.getName();
                    String modifier = sourceInfo.getModifier();

                    if (sourceName.isEmpty()) {
                        transaction.rollback();
                        responseObserver.onError(new IllegalArgumentException("Source name cannot be empty"));
                        return;
                    }

                    if (modifier.isEmpty()) {
                        transaction.rollback();
                        responseObserver.onError(new IllegalArgumentException("Modifier cannot be empty"));
                        return;
                    }

                    SourceEntity source = findSourceByName(session, sourceName);
                    if (source == null) {
                        transaction.rollback();
                        ClaimSourcesResponse response = ClaimSourcesResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Source not found: " + sourceName)
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }

                    if (!source.getModifier().isEmpty()) {
                        transaction.rollback();
                        ClaimSourcesResponse response = ClaimSourcesResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Source is already locked: " + sourceName)
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                }

                for (SourceToClaim sourceInfo : request.getSourcesList()) {
                    String sourceName = sourceInfo.getName();
                    String modifier = sourceInfo.getModifier();

                    SourceEntity source = findSourceByName(session, sourceName);
                    source.setModifier(modifier);
                    session.merge(source);
                }

                transaction.commit();

                ClaimSourcesResponse response = ClaimSourcesResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Sources claimed successfully")
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } catch (Exception e) {
                if (transaction != null && transaction.isActive()) {
                    transaction.rollback();
                }
                logger.error("Error claiming sources", e);
                responseObserver.onError(e);
            }
        } catch (Exception e) {
            logger.error("Error claiming sources - session failure", e);
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

                deleteSourcesByTableId(session, KafkaSourceEntity.class, tableId);
                deleteSourcesByTableId(session, IcebergSourceEntity.class, tableId);
                deleteSourcesByTableId(session, YugabyteSourceEntity.class, tableId);
                deleteSourcesByTableId(session, PostgresSourceEntity.class, tableId);

                session.remove(table);

                transaction.commit();

                DropTableResponse response = DropTableResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Table dropped successfully")
                        .build();

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
        Query<DataHarnessTable> query =
                session.createQuery("FROM DataHarnessTable WHERE name = :name", DataHarnessTable.class);
        query.setParameter("name", tableName);

        return query.uniqueResult();
    }

    private SourceEntity findSourceByName(Session session, String name) {
        SourceEntity source = findSourceByNameAndType(session, KafkaSourceEntity.class, name);
        if (source != null) return source;

        source = findSourceByNameAndType(session, IcebergSourceEntity.class, name);
        if (source != null) return source;

        source = findSourceByNameAndType(session, YugabyteSourceEntity.class, name);
        if (source != null) return source;

        source = findSourceByNameAndType(session, PostgresSourceEntity.class, name);
        return source;
    }

    private <T extends SourceEntity> T findSourceByNameAndType(Session session, Class<T> sourceClass, String name) {
        String query = "FROM " + sourceClass.getSimpleName() + " WHERE name = :name";
        Query<T> hibernateQuery = session.createQuery(query, sourceClass);
        hibernateQuery.setParameter("name", name);

        return hibernateQuery.uniqueResult();
    }

    private <T extends SourceEntity> T findSource(Session session, Class<T> sourceClass, long tableId, String name) {
        String query = "FROM " + sourceClass.getSimpleName() + " WHERE tableId = :tableId AND name = :name";
        Query<T> hibernateQuery = session.createQuery(query, sourceClass);
        hibernateQuery.setParameter("tableId", tableId);
        hibernateQuery.setParameter("name", name);

        return hibernateQuery.uniqueResult();
    }

    private <T extends SourceEntity> void validateSourceModifier(
            Session session, Class<T> sourceClass, long tableId, String sourceName, String expectedModifier) {
        T existing = findSource(session, sourceClass, tableId, sourceName);
        if (existing != null && !existing.getModifier().equals(expectedModifier)) {
            throw new IllegalArgumentException("Modifier mismatch for source: " + sourceName);
        }
    }

    private <T extends SourceEntity> List<T> findAllSources(Session session, Class<T> entityClass, long tableId) {
        Query<T> query =
                session.createQuery("FROM " + entityClass.getSimpleName() + " WHERE tableId = :tableId", entityClass);
        query.setParameter("tableId", tableId);

        return query.getResultList();
    }

    private List<KafkaSourceEntity> findAllKafkaSources(Session session, long tableId) {
        return findAllSources(session, KafkaSourceEntity.class, tableId);
    }

    private List<IcebergSourceEntity> findAllIcebergSources(Session session, long tableId) {
        return findAllSources(session, IcebergSourceEntity.class, tableId);
    }

    private List<YugabyteSourceEntity> findAllYugabyteSources(Session session, long tableId) {
        return findAllSources(session, YugabyteSourceEntity.class, tableId);
    }

    private List<PostgresSourceEntity> findAllPostgresSources(Session session, long tableId) {
        return findAllSources(session, PostgresSourceEntity.class, tableId);
    }

    private List<DataHarnessTable> findAllTables(Session session) {
        Query<DataHarnessTable> query = session.createQuery("FROM DataHarnessTable", DataHarnessTable.class);
        return query.getResultList();
    }

    private void upsertKafkaSource(Session session, long tableId, KafkaSourceMessage kafkaMsg) {
        upsertKafkaSourceAndReturn(session, tableId, kafkaMsg);
    }

    private KafkaSourceEntity upsertKafkaSourceAndReturn(Session session, long tableId, KafkaSourceMessage kafkaMsg) {
        KafkaSourceEntity existing = findSource(session, KafkaSourceEntity.class, tableId, kafkaMsg.getName());

        if (existing != null) {
            existing.setTrinoCatalogName(kafkaMsg.getTrinoCatalogName());
            existing.setTrinoSchemaName(kafkaMsg.getTrinoSchemaName());
            existing.setStartOffset(kafkaMsg.getStartOffset());
            existing.setEndOffset(kafkaMsg.getEndOffset());
            existing.setBrokerUrls(kafkaMsg.getBrokerUrls());
            existing.setSchemaType(kafkaMsg.getSchemaType().getNumber());
            existing.setSchema(kafkaMsg.getSchema());
            session.merge(existing);
            return existing;
        } else {
            KafkaSourceEntity entity = new KafkaSourceEntity(
                    tableId,
                    kafkaMsg.getName(),
                    kafkaMsg.getTrinoCatalogName(),
                    kafkaMsg.getTrinoSchemaName(),
                    kafkaMsg.getStartOffset(),
                    kafkaMsg.getEndOffset(),
                    kafkaMsg.getPartitionNumber(),
                    kafkaMsg.getTopicName(),
                    kafkaMsg.getBrokerUrls(),
                    kafkaMsg.getSchemaType().getNumber(),
                    kafkaMsg.getSchema());
            session.persist(entity);
            return entity;
        }
    }

    private void upsertIcebergSource(Session session, long tableId, IcebergSourceMessage icebergMsg) {
        upsertIcebergSourceAndReturn(session, tableId, icebergMsg);
    }

    private IcebergSourceEntity upsertIcebergSourceAndReturn(
            Session session, long tableId, IcebergSourceMessage icebergMsg) {
        IcebergSourceEntity existing = findSource(session, IcebergSourceEntity.class, tableId, icebergMsg.getName());

        if (existing != null) {
            existing.setTrinoCatalogName(icebergMsg.getTrinoCatalogName());
            existing.setTrinoSchemaName(icebergMsg.getTrinoSchemaName());
            existing.setReadTimestamp(icebergMsg.getReadTimestamp());
            existing.setSparkCatalogName(icebergMsg.getSparkCatalogName());
            existing.setSparkSchemaName(icebergMsg.getSparkSchemaName());
            session.merge(existing);
            return existing;
        } else {
            IcebergSourceEntity entity = new IcebergSourceEntity(
                    tableId,
                    icebergMsg.getName(),
                    icebergMsg.getTrinoCatalogName(),
                    icebergMsg.getTrinoSchemaName(),
                    icebergMsg.getTableName(),
                    icebergMsg.getReadTimestamp(),
                    icebergMsg.getSparkCatalogName(),
                    icebergMsg.getSparkSchemaName());
            session.persist(entity);
            return entity;
        }
    }

    private void upsertYugabyteSource(Session session, long tableId, YugabyteDBSourceMessage yugabyteMsg) {
        upsertYugabyteSourceAndReturn(session, tableId, yugabyteMsg);
    }

    private YugabyteSourceEntity upsertYugabyteSourceAndReturn(
            Session session, long tableId, YugabyteDBSourceMessage yugabyteMsg) {
        YugabyteSourceEntity existing = findSource(session, YugabyteSourceEntity.class, tableId, yugabyteMsg.getName());

        if (existing != null) {
            existing.setTrinoCatalogName(yugabyteMsg.getTrinoCatalogName());
            existing.setTrinoSchemaName(yugabyteMsg.getTrinoSchemaName());
            existing.setJdbcUrl(yugabyteMsg.getJdbcUrl());
            existing.setUsername(yugabyteMsg.getUsername());
            existing.setPassword(yugabyteMsg.getPassword());
            existing.setReadTimestamp(yugabyteMsg.getReadTimestamp());
            session.merge(existing);
            return existing;
        } else {
            YugabyteSourceEntity entity = new YugabyteSourceEntity(
                    tableId,
                    yugabyteMsg.getName(),
                    yugabyteMsg.getTrinoCatalogName(),
                    yugabyteMsg.getTrinoSchemaName(),
                    yugabyteMsg.getTableName(),
                    yugabyteMsg.getJdbcUrl(),
                    yugabyteMsg.getUsername(),
                    yugabyteMsg.getPassword(),
                    yugabyteMsg.getReadTimestamp());
            session.persist(entity);
            return entity;
        }
    }

    private void upsertPostgresSource(Session session, long tableId, PostgresDBSourceMessage postgresMsg) {
        upsertPostgresSourceAndReturn(session, tableId, postgresMsg);
    }

    private PostgresSourceEntity upsertPostgresSourceAndReturn(
            Session session, long tableId, PostgresDBSourceMessage postgresMsg) {
        PostgresSourceEntity existing = findSource(session, PostgresSourceEntity.class, tableId, postgresMsg.getName());

        if (existing != null) {
            existing.setTrinoCatalogName(postgresMsg.getTrinoCatalogName());
            existing.setTrinoSchemaName(postgresMsg.getTrinoSchemaName());
            existing.setJdbcUrl(postgresMsg.getJdbcUrl());
            existing.setUsername(postgresMsg.getUsername());
            existing.setPassword(postgresMsg.getPassword());
            existing.setReadTimestamp(postgresMsg.getReadTimestamp());
            existing.setHistoryTableName(postgresMsg.getHistoryTableName());
            existing.setTableNameNoTstzrange(postgresMsg.getTableNameNoTstzrange());
            existing.setHistoryTableNameNoTstzrange(postgresMsg.getHistoryTableNameNoTstzrange());
            session.merge(existing);
            return existing;
        } else {
            PostgresSourceEntity entity = new PostgresSourceEntity(
                    tableId,
                    postgresMsg.getName(),
                    postgresMsg.getTrinoCatalogName(),
                    postgresMsg.getTrinoSchemaName(),
                    postgresMsg.getTableName(),
                    postgresMsg.getJdbcUrl(),
                    postgresMsg.getUsername(),
                    postgresMsg.getPassword(),
                    postgresMsg.getReadTimestamp(),
                    postgresMsg.getHistoryTableName(),
                    postgresMsg.getTableNameNoTstzrange(),
                    postgresMsg.getHistoryTableNameNoTstzrange());
            session.persist(entity);
            return entity;
        }
    }

    private <T extends SourceEntity> void deleteSourcesByTableId(Session session, Class<T> entityClass, long tableId) {
        Query<?> query =
                session.createQuery("DELETE FROM " + entityClass.getSimpleName() + " WHERE tableId = :tableId");
        query.setParameter("tableId", tableId);
        query.executeUpdate();
    }

    private void addKafkaSourcestoResponse(
            List<KafkaSourceEntity> kafkaSources, String tableName, LoadTableResponse.Builder responseBuilder) {
        for (KafkaSourceEntity kafka : kafkaSources) {
            KafkaSourceMessage.Builder kafkaMsgBuilder = KafkaSourceMessage.newBuilder()
                    .setName(kafka.getName())
                    .setTrinoCatalogName(kafka.getTrinoCatalogName())
                    .setTrinoSchemaName(kafka.getTrinoSchemaName())
                    .setStartOffset(kafka.getStartOffset())
                    .setEndOffset(kafka.getEndOffset())
                    .setPartitionNumber(kafka.getPartitionNumber())
                    .setTopicName(kafka.getTopicName())
                    .setModifier(kafka.getModifier());

            if (kafka.getBrokerUrls() != null && !kafka.getBrokerUrls().isEmpty()) {
                kafkaMsgBuilder.setBrokerUrls(kafka.getBrokerUrls());
            }

            if (kafka.getSchemaType() != 0) {
                kafkaMsgBuilder.setSchemaType(SchemaType.forNumber(kafka.getSchemaType()));
            }

            if (kafka.getSchema() != null && !kafka.getSchema().isEmpty()) {
                kafkaMsgBuilder.setSchema(kafka.getSchema());
            }

            KafkaSourceMessage kafkaMsg = kafkaMsgBuilder.build();

            TableSourceMessage sourceMessage = TableSourceMessage.newBuilder()
                    .setTableName(tableName)
                    .setKafkaSource(kafkaMsg)
                    .build();

            responseBuilder.addSources(sourceMessage);
        }
    }

    private void addIcebergSourcesToResponse(
            List<IcebergSourceEntity> icebergSources, String tableName, LoadTableResponse.Builder responseBuilder) {
        for (IcebergSourceEntity iceberg : icebergSources) {
            IcebergSourceMessage icebergMsg = IcebergSourceMessage.newBuilder()
                    .setName(iceberg.getName())
                    .setTrinoCatalogName(iceberg.getTrinoCatalogName())
                    .setTrinoSchemaName(iceberg.getTrinoSchemaName())
                    .setTableName(iceberg.getTableName())
                    .setReadTimestamp(iceberg.getReadTimestamp())
                    .setSparkCatalogName(iceberg.getSparkCatalogName() != null ? iceberg.getSparkCatalogName() : "")
                    .setSparkSchemaName(iceberg.getSparkSchemaName() != null ? iceberg.getSparkSchemaName() : "")
                    .setModifier(iceberg.getModifier())
                    .build();

            TableSourceMessage sourceMessage = TableSourceMessage.newBuilder()
                    .setTableName(tableName)
                    .setIcebergSource(icebergMsg)
                    .build();

            responseBuilder.addSources(sourceMessage);
        }
    }

    private void addYugabyteSourcesToResponse(
            List<YugabyteSourceEntity> yugabyteSources, String tableName, LoadTableResponse.Builder responseBuilder) {
        for (YugabyteSourceEntity yugabyte : yugabyteSources) {
            YugabyteDBSourceMessage yugabyteMsg = YugabyteDBSourceMessage.newBuilder()
                    .setName(yugabyte.getName())
                    .setTrinoCatalogName(yugabyte.getTrinoCatalogName())
                    .setTrinoSchemaName(yugabyte.getTrinoSchemaName())
                    .setTableName(yugabyte.getTableName())
                    .setJdbcUrl(yugabyte.getJdbcUrl())
                    .setUsername(yugabyte.getUsername())
                    .setPassword(yugabyte.getPassword())
                    .setReadTimestamp(yugabyte.getReadTimestamp())
                    .setModifier(yugabyte.getModifier())
                    .build();

            TableSourceMessage sourceMessage = TableSourceMessage.newBuilder()
                    .setTableName(tableName)
                    .setYugabytedbSource(yugabyteMsg)
                    .build();

            responseBuilder.addSources(sourceMessage);
        }
    }

    private void addPostgresSourcesToResponse(
            List<PostgresSourceEntity> postgresSources, String tableName, LoadTableResponse.Builder responseBuilder) {
        for (PostgresSourceEntity postgres : postgresSources) {
            PostgresDBSourceMessage postgresMsg = PostgresDBSourceMessage.newBuilder()
                    .setName(postgres.getName())
                    .setTrinoCatalogName(postgres.getTrinoCatalogName())
                    .setTrinoSchemaName(postgres.getTrinoSchemaName())
                    .setTableName(postgres.getTableName())
                    .setJdbcUrl(postgres.getJdbcUrl())
                    .setUsername(postgres.getUsername())
                    .setPassword(postgres.getPassword())
                    .setReadTimestamp(postgres.getReadTimestamp())
                    .setHistoryTableName(postgres.getHistoryTableName() != null ? postgres.getHistoryTableName() : "")
                    .setTableNameNoTstzrange(
                            postgres.getTableNameNoTstzrange() != null ? postgres.getTableNameNoTstzrange() : "")
                    .setHistoryTableNameNoTstzrange(
                            postgres.getHistoryTableNameNoTstzrange() != null
                                    ? postgres.getHistoryTableNameNoTstzrange()
                                    : "")
                    .setModifier(postgres.getModifier())
                    .build();

            TableSourceMessage sourceMessage = TableSourceMessage.newBuilder()
                    .setTableName(tableName)
                    .setPostgresdbSource(postgresMsg)
                    .build();

            responseBuilder.addSources(sourceMessage);
        }
    }
}
