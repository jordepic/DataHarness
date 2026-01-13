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
package com.jordepic.dataharness.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(
        name = "kafka_sources",
        indexes = {
            @Index(
                    name = "idx_kafka_sources_table_id_topic_partition",
                    columnList = "table_id, topic_name, partition_number",
                    unique = true)
        },
        uniqueConstraints = {
            @UniqueConstraint(
                    name = "uk_kafka_sources_table_id_topic_partition",
                    columnNames = {"table_id", "topic_name", "partition_number"})
        })
public class KafkaSourceEntity implements SourceEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "table_id", nullable = false)
    private long tableId;

    @Column(name = "name", nullable = false)
    private String name;

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

    @Column(name = "broker_urls")
    private String brokerUrls;

    @Column(name = "schema_type")
    private int schemaType;

    @Column(name = "schema")
    private String schema;

    @Column(name = "modifier", nullable = false)
    private String modifier = "";

    public KafkaSourceEntity() {}

    public KafkaSourceEntity(
            long tableId,
            String name,
            String trinoCatalogName,
            String trinoSchemaName,
            long startOffset,
            long endOffset,
            int partitionNumber,
            String topicName) {
        this.tableId = tableId;
        this.name = name;
        this.trinoCatalogName = trinoCatalogName;
        this.trinoSchemaName = trinoSchemaName;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.partitionNumber = partitionNumber;
        this.topicName = topicName;
    }

    public KafkaSourceEntity(
            long tableId,
            String name,
            String trinoCatalogName,
            String trinoSchemaName,
            long startOffset,
            long endOffset,
            int partitionNumber,
            String topicName,
            String brokerUrls,
            int schemaType,
            String schema) {
        this.tableId = tableId;
        this.name = name;
        this.trinoCatalogName = trinoCatalogName;
        this.trinoSchemaName = trinoSchemaName;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.partitionNumber = partitionNumber;
        this.topicName = topicName;
        this.brokerUrls = brokerUrls;
        this.schemaType = schemaType;
        this.schema = schema;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getBrokerUrls() {
        return brokerUrls;
    }

    public void setBrokerUrls(String brokerUrls) {
        this.brokerUrls = brokerUrls;
    }

    public int getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(int schemaType) {
        this.schemaType = schemaType;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getModifier() {
        return modifier;
    }

    public void setModifier(String modifier) {
        this.modifier = modifier;
    }
}
