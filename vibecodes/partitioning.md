I'd like to add a new feature to the project that assigns a "partition filter"
to table sources.

This will allow us to have multiple "sources" from the same actual data source,
albeit represents different underlying partitions of it. The partition filter should
be represented as a simple string: "id = 1 AND name = 'jordan'", which implies that
the underlying data source can easily filter down its results were I to pass that condition
as my filter. The end goal here is for clients to specify different "read timestamps" per
partition of a data source, thereby enabling them to be updated and queried independently.

While certain technologies may have hash based partitioning under the hood, the Data Harness
will initially operate under the assumption that its clients will convert the partitioning
schemas of all of its data sources to be "identity-based". For example:

- Bucket(name, 16) -> Actually partition the table by Identity(Hash(name) % 16)
- This does imply that users would need to pass in the actual hashes to their queries, which
  is a less than ideal user experience, but we'll live with it for now

Since this "partition filter" can belong on any data source, let's add it to the SourceEntity
class of dataharness-server. We'll also need it accessible from our protobuf source objects.

Next, we need to implement the functionality in Spark/Trino.

Trino:

- SQL Based
- On line 211 (DataHarnessMetadata), we union together many queries from different sources
- For each source, if a "partition filter" is present, we add it to the query
- e.g. partitionFilter.isEmpty() ? SELECT * FROM source : SELECT * FROM source WHERE ${partitionFilter}

Spark:

- DataFrame Based
- In lines 78-86 of UnionTableResolutionRule, we return all dataframes from our sources
- Now, if partitionFilter is not the empty string, do a df.where(partitionFilter)

The end goal here is that the user can read from multiple different partitions of one data source
from different read timestamps, and that any filters/projections will be automatically pushed down
by the optimizer.

Let's make sure to add some tests in CatalogServiceIntegrationTest as well as DataHarness test
for this purpose. You may need to create a partitioned iceberg table for the latter.
