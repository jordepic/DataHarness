package org.dataharness.spark;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.dataharness.proto.CatalogServiceGrpc;

import java.util.Map;

public class DataHarnessGrpcClientFactory {

  public static final String DATA_HARNESS_HOST = "data-harness-host";
  public static final String DATA_HARNESS_PORT = "data-harness-port";
  private final CatalogServiceGrpc.CatalogServiceBlockingStub stub;

  public DataHarnessGrpcClientFactory(String host, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
      .usePlaintext()
      .build();
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
