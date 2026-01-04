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
@Table(
        name = "iceberg_sources",
        indexes = {
            @Index(name = "idx_iceberg_sources_table_id_table_name", columnList = "table_id, table_name", unique = true)
        },
        uniqueConstraints = {
            @UniqueConstraint(
                    name = "uk_iceberg_sources_table_id_table_name",
                    columnNames = {"table_id", "table_name"})
        })
public class IcebergSourceEntity implements SourceEntity {
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

    @Column(name = "table_name", nullable = false)
    private String tableName;

    @Column(name = "read_timestamp", nullable = false)
    private long readTimestamp;

    @Column(name = "spark_catalog_name")
    private String sparkCatalogName;

    @Column(name = "spark_schema_name")
    private String sparkSchemaName;

    public IcebergSourceEntity() {}

    public IcebergSourceEntity(
            long tableId,
            String name,
            String trinoCatalogName,
            String trinoSchemaName,
            String tableName,
            long readTimestamp) {
        this.tableId = tableId;
        this.name = name;
        this.trinoCatalogName = trinoCatalogName;
        this.trinoSchemaName = trinoSchemaName;
        this.tableName = tableName;
        this.readTimestamp = readTimestamp;
    }

    public IcebergSourceEntity(
            long tableId,
            String name,
            String trinoCatalogName,
            String trinoSchemaName,
            String tableName,
            long readTimestamp,
            String sparkCatalogName,
            String sparkSchemaName) {
        this.tableId = tableId;
        this.name = name;
        this.trinoCatalogName = trinoCatalogName;
        this.trinoSchemaName = trinoSchemaName;
        this.tableName = tableName;
        this.readTimestamp = readTimestamp;
        this.sparkCatalogName = sparkCatalogName;
        this.sparkSchemaName = sparkSchemaName;
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getReadTimestamp() {
        return readTimestamp;
    }

    public void setReadTimestamp(long readTimestamp) {
        this.readTimestamp = readTimestamp;
    }

    public String getSparkCatalogName() {
        return sparkCatalogName;
    }

    public void setSparkCatalogName(String sparkCatalogName) {
        this.sparkCatalogName = sparkCatalogName;
    }

    public String getSparkSchemaName() {
        return sparkSchemaName;
    }

    public void setSparkSchemaName(String sparkSchemaName) {
        this.sparkSchemaName = sparkSchemaName;
    }
}
