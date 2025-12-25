* Snapshot

   This type allows you to perform a transfer at set periods of time.

   1. Specify the snapshot time interval. You can do it using two methods - **Period** or **Cron Expression**:

      {% cut "Period" %}

      From the dropdown menu, select the period at which {{ data-transfer-name }} will automatically perform a snapshot:

      ![Screenshot that shows a list of for the periodic snapshot interval from every 5 minutes to every 24 hours](../../_assets/transfer/transfer-instructions/periodic-snapshot-period.png "Select the periodic snapshot interval" =512x)

      If you start the transfer manually, the next periodic snapshot will happen after the specified interval.

      {% endcut %}

      {% cut "Cron expression" %}

      Cron expressions allow you to specify the snapshot interval with extended flexibility and precision.

      Here are some examples for cron expressions:

      #|
      ||

      ```cron
      0 12 * * ?
      ```

      | At 12:00 p.m. (noon) every day ||
      ||

      ```cron
      30 9 15 * ?
      ```

      | At 9:30 a.m. on the 15th day of every month ||
      ||

      ```cron
      0 10 ? * 2#3
      ```

      | At 10 a.m. on the third Monday of every month ||
      |#

      For a complete syntax reference, see [the Guide to Cron Expressions ![external link](../../_assets/external-link.svg)](https://www.baeldung.com/cron-expressions).

      {% endcut %}

   1. Under **Incremental tables**, click **+ Table**.

      {{ data-transfer-name }} builds an incremental table from scratch during the first launch. When running subsequent snapshots, {{ data-transfer-name }} adds only new rows to such tables, reducing the overall time and resource load.

      To configure the table:

      1. Provide a **schema**.

      1. Specify the name of the **table**.

      1. Enter the above table's **key column**.

      1. Provide the **initial value** from which to snapshot the data on the source.

   1. Under **Snapshot settings** → **Parallel snapshot settings**, configure the number of workers and threads per worker.

      In most test cases, the default configuration is enough to provide adequate transfer performance.

      {% note tip "High-load transfer configuration" %}

      In high-load production cases, consider adding workers if you need to transfer more than 100 GB of data to snapshot. In our experience, adding a single worker doubles the transfer's performance.

      {% endnote %}
