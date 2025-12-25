
1. Under **Snapshot settings** → **Parallel snapshot settings**, configure the transfer performance:

   1. **Processes count** sets the number of parallel instances of a container with a transfer. Increasing this number will speed up your transfer execution. You can specify up to 8 instances.

   1. **Threads count** specifies the number of processes within each container. You can run up to 10 processes.

   The average download speed of data transfers is between 1 and 10 mb/s.

1. Add one or more **Incremental tables**. With incremental tables, you can transfer only the data that has changed. Each of these tables has the following fields:

   * **Schema** corresponds to the database schema (as in {{ PG }}), name, or dataset.

   * **Table** in the source to compare with your target table.

   * **Key column** is the name of a column that contains some value (a cursor) that indicates whether to increment the table. A common example of a cursor is a column with timestamps. Refer to the [Airbyte - Incremental Sync - Append ![external link](../_assets/external-link.svg)](https://docs.airbyte.com/understanding-airbyte/connections/incremental-append/) to learn more about cursor definitions and usage examples.

   * **Start value** (Optional) defines a value in the **Key column** based on which to track the changes. For example, you can specify a date as the **Start value**. In this case, the service will transfer only the rows with a date value greater than the start value.
