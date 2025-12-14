// Copyright (c) 2025
package org.dataharness.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "kafka_sources", indexes = {
  @Index(name = "idx_kafka_sources_table_id_topic_partition", columnList = "table_id, topic_name, partition_number", unique = true)}, uniqueConstraints = {
  @UniqueConstraint(name = "uk_kafka_sources_table_id_topic_partition", columnNames = {"table_id",
    "topic_name", "partition_number"})})
public class KafkaSourceEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "table_id", nullable = false)
  private long tableId;

  @Column(name = "trino_catalog_name", nullable = false)
  private String trinoCatalogName;

  @Column(name = "trino_schema_name", nullable = false)
  private String trinoSchemaName;

  @Column(name = "start_offset", nullable = false)
  private long startOffset;

  @Column(name = "end_offset", nullable = false)
  private long endOffset;

  @Column(name = "partition_number", nullable = false)
  private int partitionNumber;

  @Column(name = "topic_name", nullable = false)
  private String topicName;

  public KafkaSourceEntity() {
  }

  public KafkaSourceEntity(long tableId, String trinoCatalogName, String trinoSchemaName, long startOffset,
                           long endOffset, int partitionNumber, String topicName) {
    this.tableId = tableId;
    this.trinoCatalogName = trinoCatalogName;
    this.trinoSchemaName = trinoSchemaName;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.partitionNumber = partitionNumber;
    this.topicName = topicName;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getTableId() {
    return tableId;
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
  }

  public String getTrinoCatalogName() {
    return trinoCatalogName;
  }

  public void setTrinoCatalogName(String trinoCatalogName) {
    this.trinoCatalogName = trinoCatalogName;
  }

  public String getTrinoSchemaName() {
    return trinoSchemaName;
  }

  public void setTrinoSchemaName(String trinoSchemaName) {
    this.trinoSchemaName = trinoSchemaName;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(long startOffset) {
    this.startOffset = startOffset;
  }

  public long getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(long endOffset) {
    this.endOffset = endOffset;
  }

  public int getPartitionNumber() {
    return partitionNumber;
  }

  public void setPartitionNumber(int partitionNumber) {
    this.partitionNumber = partitionNumber;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }
}
