// Copyright (c) 2025
package org.dataharness;

import org.dataharness.server.GrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            GrpcServer server = new GrpcServer();
            server.start();
            server.blockUntilShutdown();
        } catch (Exception e) {
            logger.error("Failed to start gRPC server", e);
        }
    }
}
