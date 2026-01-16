/*
 * The MIT License
 * Copyright © 2026 Jordan Epstein
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
// Copyright (c) 2025
package io.github.jordepic.db;

import io.github.jordepic.entity.*;
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
