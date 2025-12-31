package io.trino.plugin.dataharness;

import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class DataHarnessModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(DataHarnessConnector.class).in(Scopes.SINGLETON);
        binder.bind(DataHarnessMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DataHarnessGrpcClientFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DataHarnessConfig.class);
    }
}
