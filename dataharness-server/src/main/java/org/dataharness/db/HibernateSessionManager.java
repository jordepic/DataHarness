// Copyright (c) 2025
package org.dataharness.db;

import org.dataharness.entity.*;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HibernateSessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HibernateSessionManager.class);
  private static SessionFactory sessionFactory = null;

  private static synchronized void initializeSessionFactory() {
    if (sessionFactory != null) {
      return;
    }

    try {
      StandardServiceRegistryBuilder registryBuilder =
          new StandardServiceRegistryBuilder().configure("hibernate.cfg.xml");

      String url = System.getProperty("hibernate.connection.url");
      String username = System.getProperty("hibernate.connection.username");
      String password = System.getProperty("hibernate.connection.password");

      if (url != null) {
        registryBuilder.applySetting("hibernate.connection.url", url);
      }
      if (username != null) {
        registryBuilder.applySetting("hibernate.connection.username", username);
      }
      if (password != null) {
        registryBuilder.applySetting("hibernate.connection.password", password);
      }

      StandardServiceRegistry registry = registryBuilder.build();

      MetadataSources metadata = new MetadataSources(registry);
      metadata.addAnnotatedClass(DataHarnessTable.class);
      metadata.addAnnotatedClass(KafkaSourceEntity.class);
      metadata.addAnnotatedClass(IcebergSourceEntity.class);
      metadata.addAnnotatedClass(YugabyteSourceEntity.class);
      metadata.addAnnotatedClass(PostgresSourceEntity.class);

      sessionFactory = metadata.getMetadataBuilder().build().buildSessionFactory();
    } catch (Exception e) {
      LOGGER.error("", e);
    }
  }

  public static Session getSession() throws HibernateException {
    if (sessionFactory == null) {
      initializeSessionFactory();
    }
    return sessionFactory.openSession();
  }
}
