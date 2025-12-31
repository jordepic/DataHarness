package io.trino.plugin.dataharness;

import com.google.inject.Inject;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.dataharness.proto.CatalogServiceGrpc;
import org.dataharness.proto.CatalogServiceGrpc.CatalogServiceBlockingStub;

public class DataHarnessGrpcClientFactory {
    private final ManagedChannel channel;
    private final CatalogServiceBlockingStub stub;

    @Inject
    public DataHarnessGrpcClientFactory(DataHarnessConfig config) {
        this.channel = ManagedChannelBuilder.forAddress(config.getHost(), config.getPort())
                .usePlaintext()
                .build();
        this.stub = CatalogServiceGrpc.newBlockingStub(channel);
    }

    public CatalogServiceBlockingStub getStub() {
        return stub;
    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void shutdown() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }
}
