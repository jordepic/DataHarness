---
layout: post
title: Introducing DataHarness
date: 2026-01-01
excerpt: "DataHarness brings unified access to multiple table sources, simplifying data integration across your infrastructure."
---

## What is DataHarness?

DataHarness is a unified abstraction layer designed to solve a critical problem in modern data engineering: seamlessly integrating data from multiple heterogeneous sources.

Traditional data architectures often require engineers to maintain separate connection logic for each data source—whether it's Kafka, CockroachDB, Iceberg, or other systems. DataHarness eliminates this complexity by providing a single, consistent interface.

## Key Features

### Multi-Source Unification
Connect data from Kafka, relational databases, data lakes, and lakehouses through a single abstraction layer. Your applications interact with a unified table interface, regardless of the underlying source.

### Schema Management
DataHarness maintains centralized schema tracking, ensuring consistency across your data ecosystem. No more schema mismatches or silent data quality issues.

### Time-Based Queries
Need to read data as it existed at a specific point in time? DataHarness supports offset and timestamp-based queries, enabling reproducible analytics and debugging.

### gRPC-Powered
Built on gRPC for efficient, low-latency client-server communication. Perfect for performance-critical applications.

## Architecture Overview

DataHarness consists of:

1. **PostgreSQL Backend** - Stores table metadata, schemas, and source configurations
2. **gRPC Service** - Provides the primary client interface
3. **Plugin System** - Extensible architecture for adding new data sources
4. **Query Engine Plugins** - (Coming soon) Integrations with Trino, Presto, and other query engines

## Getting Started

Check out our [Documentation](/DataHarness/docs/) to learn how to:
- Set up DataHarness
- Configure your data sources
- Query unified tables
- Integrate with your applications

## What's Next?

We're actively working on:
- Trino plugin for SQL access to unified tables
- Additional data source connectors
- Enhanced caching and performance optimization
- Client libraries in multiple languages

Stay tuned!
