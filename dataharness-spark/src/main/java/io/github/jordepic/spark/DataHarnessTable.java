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
package io.github.jordepic.spark;

import io.github.jordepic.proto.LoadTableResponse;
import java.util.Collections;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;

public class DataHarnessTable implements Table {

    private final String tableName;
    private final LoadTableResponse response;

    public DataHarnessTable(String tableName, LoadTableResponse response) {
        this.tableName = tableName;
        this.response = response;
    }

    public LoadTableResponse getResponse() {
        return response;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public StructType schema() {
        return DataHarnessSchemaExtractor.extractSchema(tableName, response);
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.emptySet();
    }
}
