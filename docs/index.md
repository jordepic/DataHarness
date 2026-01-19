---
layout: page
title: Documentation
permalink: /docs/
---

# DataHarness Documentation

DataHarness is a unified abstraction layer that brings together multiple table sources into a single abstract table
interface.

## Overview

DataHarness aims to unify multiple different data sources into a single tabular interface. Clients of the
DataHarness can create tables, atomically update a "schema" across many sources, atomically update and query the
displayed
data from each source. It offers protections against multiple concurrent writers by "claiming" data sources.

For a complete list of which query engines and sources DataHarness supports, see
the [readme](https://github.com/jordepic/DataHarness/blob/main/readme.md#currently-supported).

For a list of protobuf RPC calls to interact with DataHarness, see
the [proto spec](https://github.com/jordepic/DataHarness/blob/main/dataharness-rpc/src/main/proto/catalog.proto).

Docker images and helm charts can be
found [on the docker hub](https://hub.docker.com/repository/docker/jordepic/data-harness).

## Architecture

DataHarness is a stateless GRPC server that interfaces with a database via Hibernate.

For now, the helm chart in this repository uses a single server replica and a single database instance, though this will
be scaled out in the future for improved throughput and availability. The only theoretical requirement of our database
is that it supports ACID transactions.

## Spark

DataHarness requires importing two dependencies when initializing spark:

1) The DataHarness catalog (communicates with the DataHarness server)
2) The DataHarness extension (rewrites scan plans to federate between individual table sources)

To run DataHarness with Spark, see the [readme](https://hub.docker.com/repository/docker/jordepic/data-harness).

In addition to the notes there, keep in mind that any data sources that you are reading from (e.g. kafka, iceberg) also
still need to be added as dependencies to your spark job.

## Trino

Trino setup is fairly simple, with a small amount of extra work to do if using a PostgreSQL database as a table source.
See the [readme](https://github.com/jordepic/DataHarness/blob/main/readme.md#trino-setup).

## Known Issues

- Complex type evolution is not supported (we're unioning these tables with SQL statements for now, thus limiting our
  flexibility).
