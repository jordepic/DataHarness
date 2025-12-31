package io.trino.plugin.dataharness;

import static io.trino.plugin.dataharness.DataHarnessTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

public class DataHarnessConnector implements Connector {
    private final LifeCycleManager lifeCycleManager;
    private final DataHarnessMetadata metadata;

    @Inject
    public DataHarnessConnector(LifeCycleManager lifeCycleManager, DataHarnessMetadata metadata) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }
}
