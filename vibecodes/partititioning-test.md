/Users/jordanepstein/data/DataHarness/vibecodes/partitioning.md

Previously, you implemented the "partitioning" feature according to the directions that I supplied in the above file.

I'd now like this feature tested in a more end-to-end way in the DataHarnessTest class.

The goal of this test is twofold:

- We can establish claims on different "partitions" of the same data source. These are represented as different table
  sources in the DataHarness for one table, albeit just with different names and partition filters.
    - In this simple example, let's create and update an iceberg table which is partitioned on the field "id"
    - We'll add in the same data that we might normally in DataSourcePopulator, with the exception that this iceberg
      table
      is partitioned, so I expect the three iceberg rows to go to three different partitions
    - Each one of these iceberg partitions should be added as sources with the partition filter id = x, where x
      corresponds to their particular id. After we add data into those partitions, grab a claim on all three of them
      using
      different modifier ids, and then update the sources to reflect the individual snapshot at which each of these were
      added.
    - Generally try to reuse as much code as possible with DataSourcePopulator, extracting helper methods where possible
- We can query the data from Trino and Spark with partition filters applied
    - We can SELECT * FROM our table where id = 1 and ensure that we only get one row back
    - I also want you to print out the query plan that we get back from trino and spark (EXPLAIN ANALYZE {query} in
      trino
      and EXPLAIN FORMATTED {query} in spark in addition to running the query so that I can see that we properly push
      down filters where expected
