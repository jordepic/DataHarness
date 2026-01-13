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

@Entity
@Table(
        name = "data_harness_table",
        indexes = {@Index(name = "idx_data_harness_table_name", columnList = "name", unique = true)})
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

    @Column(nullable = true, columnDefinition = "TEXT")
    private String protobufSchema;

    public DataHarnessTable() {}

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

    public String getProtobufSchema() {
        return protobufSchema;
    }

    public void setProtobufSchema(String protobufSchema) {
        this.protobufSchema = protobufSchema;
    }
}
