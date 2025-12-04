// Copyright (c) 2025
package org.example.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "iceberg_sources", indexes = {
		@Index(name = "idx_iceberg_sources_table_id_table_name", columnList = "table_id, table_name", unique = true)}, uniqueConstraints = {
				@UniqueConstraint(name = "uk_iceberg_sources_table_id_table_name", columnNames = {"table_id",
						"table_name"})})
public class IcebergSourceEntity {
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

	@Column(name = "read_timestamp", nullable = false)
	private long readTimestamp;

	public IcebergSourceEntity() {
	}

	public IcebergSourceEntity(long tableId, String trinoCatalogName, String trinoSchemaName, String tableName,
			long readTimestamp) {
		this.tableId = tableId;
		this.trinoCatalogName = trinoCatalogName;
		this.trinoSchemaName = trinoSchemaName;
		this.tableName = tableName;
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

	public long getReadTimestamp() {
		return readTimestamp;
	}

	public void setReadTimestamp(long readTimestamp) {
		this.readTimestamp = readTimestamp;
	}
}
