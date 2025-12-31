package io.trino.plugin.dataharness;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class DataHarnessConfig {
  private String host;
  private int port;

  @NotNull
  public String getHost() {
    return host;
  }

  @Config("data-harness.host")
  @ConfigDescription("Data Harness service host")
  public DataHarnessConfig setHost(String host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  @Config("data-harness.port")
  @ConfigDescription("Data Harness service port")
  public DataHarnessConfig setPort(int port) {
    this.port = port;
    return this;
  }
}
