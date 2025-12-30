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
@Table(name = "postgres_sources", indexes = {
  @Index(name = "idx_postgres_sources_table_id_table_name", columnList = "table_id, table_name", unique = true)}, uniqueConstraints = {
  @UniqueConstraint(name = "uk_postgres_sources_table_id_table_name", columnNames = {"table_id",
    "table_name"})})
public class PostgresSourceEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(name = "table_id", nullable = false)
  private long tableId;

  @Column(name = "trino_catalog_name", nullable = false)
  private String trinoCatalogName;

  @Column(name = "trino_schema_name", nullable = false)
  private String trinoSchemaName;

  @Column(name = "table_name", nullable = false)
  private String tableName;

  @Column(name = "jdbc_url", nullable = false)
  private String jdbcUrl;

  @Column(name = "username", nullable = false)
  private String username;

  @Column(name = "password", nullable = false)
  private String password;

  @Column(name = "read_timestamp", nullable = false)
  private long readTimestamp;

  public PostgresSourceEntity() {
  }

  public PostgresSourceEntity(long tableId, String trinoCatalogName, String trinoSchemaName, String tableName,
                              String jdbcUrl, String username, String password, long readTimestamp) {
    this.tableId = tableId;
    this.trinoCatalogName = trinoCatalogName;
    this.trinoSchemaName = trinoSchemaName;
    this.tableName = tableName;
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.readTimestamp = readTimestamp;
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

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public long getReadTimestamp() {
    return readTimestamp;
  }

  public void setReadTimestamp(long readTimestamp) {
    this.readTimestamp = readTimestamp;
  }
}
