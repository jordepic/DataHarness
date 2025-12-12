// Copyright (c) 2025
package org.dataharness.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

import org.dataharness.service.CatalogServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);
  private static final int PORT = 50051;
  private Server server;

  public void start() throws IOException {
    server = ServerBuilder.forPort(PORT).addService(new CatalogServiceImpl()).build().start();

    LOGGER.info("gRPC server started on port {}", PORT);

    Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
      LOGGER.info("gRPC server stopped");
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
