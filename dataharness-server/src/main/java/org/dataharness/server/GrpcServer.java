// Copyright (c) 2025
package org.dataharness.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import org.dataharness.service.CatalogServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);
    private static final int DEFAULT_PORT = 50051;
    private static final String PORT_PROPERTY = "grpc.server.port";
    private Server server;

    public void start() throws IOException {
        int port = Integer.parseInt(System.getProperty(PORT_PROPERTY, String.valueOf(DEFAULT_PORT)));
        server = ServerBuilder.forPort(port)
                .addService(new CatalogServiceImpl())
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        LOGGER.info("gRPC server started on port {}", port);

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
