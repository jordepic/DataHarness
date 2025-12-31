package io.trino.plugin.dataharness;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class DataHarnessConnectorFactory implements ConnectorFactory {
  @Override
  public String getName() {
    return "data_harness";
  }

  @Override
  public Connector create(
      String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
    requireNonNull(requiredConfig, "requiredConfig is null");
    checkStrictSpiVersionMatch(context, this);

    Bootstrap app =
        new Bootstrap(
            new JsonModule(),
            new TypeDeserializerModule(context.getTypeManager()),
            new DataHarnessModule());

    Injector injector =
        app.doNotInitializeLogging()
            .setRequiredConfigurationProperties(requiredConfig)
            .initialize();

    return injector.getInstance(DataHarnessConnector.class);
  }
}
