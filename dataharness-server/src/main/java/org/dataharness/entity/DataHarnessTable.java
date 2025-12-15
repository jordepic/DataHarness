// Copyright (c) 2025
package org.dataharness.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

@Entity
@Table(name = "data_harness_table", indexes = {
  @Index(name = "idx_data_harness_table_name", columnList = "name", unique = true)})
public class DataHarnessTable {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Column(nullable = false, unique = true)
  private String name;

  @Column(nullable = true, columnDefinition = "TEXT")
  private String avroSchema;

  @Column(nullable = true, columnDefinition = "TEXT")
  private String icebergSchema;

  public DataHarnessTable() {
  }

  public DataHarnessTable(String name) {
    this.name = name;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAvroSchema() {
    return avroSchema;
  }

  public void setAvroSchema(String avroSchema) {
    this.avroSchema = avroSchema;
  }

  public String getIcebergSchema() {
    return icebergSchema;
  }

  public void setIcebergSchema(String icebergSchema) {
    this.icebergSchema = icebergSchema;
  }
}
