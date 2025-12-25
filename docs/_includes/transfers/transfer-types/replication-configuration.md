* Replication

   This feature allows you to continuously copy changes on the source and apply them to the target.

   Under **Replication settings** → **Parallel replication settings**, configure the number of workers.

   In most test cases, the default configuration is enough to provide adequate transfer performance.

   {% note tip "High-load transfer configuration" %}

   In high-load production cases, consider adding workers if you need to continuously transfer more than 100 GB. In our experience, adding a single worker doubles the transfer's performance.

   {% endnote %}