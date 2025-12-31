package io.trino.plugin.dataharness;

import io.trino.spi.connector.ConnectorTransactionHandle;

public final class DataHarnessTransactionHandle implements ConnectorTransactionHandle {
    public static final DataHarnessTransactionHandle INSTANCE = new DataHarnessTransactionHandle();

    private DataHarnessTransactionHandle() {}
}
