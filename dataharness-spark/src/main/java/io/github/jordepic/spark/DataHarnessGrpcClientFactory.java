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

import io.github.jordepic.proto.CatalogServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;

public class DataHarnessGrpcClientFactory {

    public static final String DATA_HARNESS_HOST = "data-harness-host";
    public static final String DATA_HARNESS_PORT = "data-harness-port";
    private final CatalogServiceGrpc.CatalogServiceBlockingStub stub;

    public DataHarnessGrpcClientFactory(String host, int port) {
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = CatalogServiceGrpc.newBlockingStub(channel);
    }

    public static DataHarnessGrpcClientFactory create(Map<String, String> options) {
        String host = options.getOrDefault(DATA_HARNESS_HOST, "localhost");
        int port = Integer.parseInt(options.getOrDefault(DATA_HARNESS_PORT, "50051"));
        return new DataHarnessGrpcClientFactory(host, port);
    }

    public CatalogServiceGrpc.CatalogServiceBlockingStub getStub() {
        return stub;
    }
}
