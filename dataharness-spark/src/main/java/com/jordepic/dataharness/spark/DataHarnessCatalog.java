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
package com.jordepic.dataharness.spark;

import com.jordepic.dataharness.proto.ListTablesRequest;
import com.jordepic.dataharness.proto.LoadTableRequest;
import com.jordepic.dataharness.proto.LoadTableResponse;
import com.jordepic.dataharness.proto.TableExistsRequest;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHarnessCatalog implements TableCatalog {
    private static final Logger logger = LoggerFactory.getLogger(DataHarnessCatalog.class);
    private static final String[] DATA_HARNESS_NAMESPACE = {"data_harness"};

    private String catalogName;
    private DataHarnessGrpcClientFactory grpcClientFactory;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        catalogName = name;
        grpcClientFactory = DataHarnessGrpcClientFactory.create(options);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        if (Arrays.equals(DATA_HARNESS_NAMESPACE, ident.namespace())) {
            LoadTableResponse response = grpcClientFactory
                    .getStub()
                    .loadTable(LoadTableRequest.newBuilder()
                            .setTableName(ident.name())
                            .build());
            return new DataHarnessTable(ident.name(), response);
        }
        throw new NoSuchTableException(ident.namespace()[0], String.format("Table %s does not exist!", ident.name()));
    }

    @Override
    public Identifier[] listTables(String... namespace) {
        if (!Arrays.equals(DATA_HARNESS_NAMESPACE, namespace)) {
            return new Identifier[] {};
        }

        Identifier[] dataHarnessIdentifiers = new Identifier[0];
        try {
            var response = grpcClientFactory
                    .getStub()
                    .listTables(ListTablesRequest.newBuilder().build());
            dataHarnessIdentifiers = response.getTableNamesList().stream()
                    .map(tableName -> Identifier.of(DATA_HARNESS_NAMESPACE, tableName))
                    .toArray(Identifier[]::new);
        } catch (Exception e) {
            logger.debug("Failed to list views from DataHarness: {}", e.getMessage());
        }

        return dataHarnessIdentifiers;
    }

    @Override
    public boolean tableExists(Identifier ident) {
        if (!Arrays.equals(DATA_HARNESS_NAMESPACE, ident.namespace())) {
            return false;
        }

        return grpcClientFactory
                .getStub()
                .tableExists(TableExistsRequest.newBuilder()
                        .setTableName(ident.name())
                        .build())
                .getExists();
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
        return null;
    }

    @Override
    public boolean dropTable(Identifier ident) {
        return false;
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {}

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String[] defaultNamespace() {
        return DATA_HARNESS_NAMESPACE;
    }
}
