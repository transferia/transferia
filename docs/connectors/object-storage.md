---
title: "S3-compatible Object Storage connector"
description: "View configuration options for the S3-compatible Object Storage connector"
---

# S3-compatible Object Storage connector

You can use this connector for **source** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

    1. Specify the **S3: Amazon Web Services** settings:

        * The name of your **Bucket**.

        * Your **AWS Access Key ID**. This field isn't necessary if you are accessing a public AWS bucket.

        * Your **AWS Secret Access Key**. This field isn't necessary if you are accessing a public AWS bucket.

            {% note tip %}

            You can find your credentials on the **Identity and Access Management (IAM)** page in the AWS console. Look for the **Access keys for CLI, SDK, & API access** section and click **Create access key** or use an existing one.

            {% endnote %}

        * **Path Prefix** as a file location in a folder to speed up the file search in a bucket.

        * **Endpoint** name if you use an S3-compatible service. Leave blank to use AWS itself.

            Certain S3-compatible services like [Wasabi ![external link](../_assets/external-link.svg)](https://wasabi.com/), require integrating the AWS region into the endpoint URL as follows:

            ```url
            s3.<storage-region>.wasabisys.com
            ```

            For more information, consult the [official Wasabi documentation ![external link](../_assets/external-link.svg)](https://docs.wasabi.com/docs/what-are-the-service-urls-for-wasabis-different-storage-regions).

        * Check the **Use SSL** box to use SSL/TLS encryption.

        * Check **Verify SSL Cert** to allow self-signed certificates.

        * Specify a **Path Pattern** to identify the files to select for transfer. Enter `**` to match all files in a bucket or specify the exact path to the files with extensions. Use [wcmatch.glob ![external link](../_assets/external-link.svg)](https://facelessuser.github.io/wcmatch/glob/) syntax and separate patterns with `|`. For example:

          ```sh
          myFolder/myTableFiles/*.csv|myFolder/myOtherTableFiles/*.csv
          ```

    1. Set up the **Event queue configuration**.

        This feature allows you to optimize your replication querying process and improve its performance. Instead of consistently reading the entire list of objects on the source for updates, the connector will receive [s3:ObjectCreated ![external link](../_assets/external-link.svg)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html) events from an [AWS SQS queue ![external link](../_assets/external-link.svg)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html).

        * Click **+ Event queue configuration** → **+ SQS**.

        * Specify the **Queue name** configured in your S3-compatible Object Storage bucket to receive [s3:ObjectCreated ![external link](../_assets/external-link.svg)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html) events.

        * Provide the **AWS owner account ID**. This account must belong to the AWS user who created the queue specified above. Leave this field empty if the {{ S3 }} bucket and the queue were created in the same account.

        * Enter the **AWS Access Key ID** used as part of the credentials to read from the [SQS queue ![external link](../_assets/external-link.svg)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html). Leave empty if the credentials for the {{ S3 }} bucket can be used.

        * Provide the **AWS Secret Access Key** used as part of the credentials to read from the SQS queue. Leave empty if the credentials for the {{ S3 }} bucket can be used.

        * Specify the **Endpoint** to an S3-compatible service. Leave empty when connecting to AWS.

        * Enter the **Region** to which you want to send requests. Leave empty if the desired region matches the one for the bucket.

        * Check the **Use SSL** box if the remote server uses a secure SSL/TLS connection.

        * Check the **Verify SSL certificate** box to allow self-signed certificates.

    1. Configure the **Dataset**:

        * Provide a **Schema** as a string in the following format:

            ```sh
            database_name / schema_name
            ```

       * Name the table you want to create for data from {{ S3 }} in the **Table** field.

    1. From the dropdown menu, select the file type you want this endpoint to transfer:

        * **CSV**
        * **Parquet**
        * **JSON Lines**.

    1. Configure properties specific to a **format**:

        {% cut "CSV" %}

        * **Delimiter** is a one-character string. This is a required field.

        * **Quote char** is used to quote values.

        * **Escape char** is used for escape special characters. Leave this field blank to ignore.

        * **Encoding** as shown in the list of [Python encodings ![external link](../_assets/external-link.svg)](https://docs.python.org/3/library/codecs.html#standard-encodings). Leave this field blank to use the default UTF-8 encoding.

        * Check the **Double quote** box if two quotes in CSV files correspond to a single quote.

        * Check the **Newlines in values** if the CSV files in your bucket contain newline characters. If enabled, this setting might lower performance.

        * **Block size** is the number of bytes to process in memory in parallel while reading files. We recommend you to keep this field with a default value: `10000`.

        * Under **Advanced options**:

            * Specify the number of rows to skip before the header line in the **Skip rows** field.
    
            * Enter the number of rows to skip after the header line in the **Skip rows after the header line** field.

            * Keep the **Automatically generate column names** box checked if the CSV filed in your data source have no header line. This feature will automatically generate column names in the following format: `f0, f1, ... fN`.

        * If you want to transfer exact columns from your CSV files on the source, click **+** under **Column names** to add them one by one.

            Note that the order of the names matters - the sequence of column names must match the one in the actual CSV file.

        * Under **Additional reader options**, you can:

            * Under **Null values**, add a list of strings that denote the `NULL` values in the data.

            * Under **True values**, provide a list of strings that denote the `true` booleans in the data.

            * Under **False values**, add a list of strings that denote the `false` booleans in the data.

            For more information on the above list sections, consult the [PyArrow documentation ![external link](../_assets/external-link.svg)](https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html).

            * In the **Decimal point** field, provide the character used as decimal point in floating-point and decimal data.

            * Check the **Strings can be NULL** box if you want to allow string columns to have `NULL` values.

            * Under **Include columns**, list the names of columns whose data will be transferred. If you specify at least one column name here, only the specified column(s) are transferred. Leave empty to transfer all columns.

            * Check the **Include missing columns** box if you want to automatically fill the missing column values with `NULL`. For more information, consult the [PyArrow documentation ![external link](../_assets/external-link.svg)](https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html#pyarrow.csv.ConvertOptions.include_missing_columns).

            * Under **Time parsers**, you can specify a [golang-compatible time format ![external link](../_assets/external-link.svg)](https://go.dev/src/time/format.go) strings to apply to the inferred `date` or `timestamp` values. Not that the connector will apply the first applicable string to the data.

        {% endcut %}

        {% cut "Parquet" %}

        This format requires no additional settings.

        {% endcut %}

        {% cut "JSON Lines" %}

        * The **Allow newlines in values** checkbox enables newline characters in JSON values. Enabling this parameter may affect transfer performance.

        * The **Unexpected field behavior** drop-down menu allows you to select how to process the JSON fields outside the provided **schema**:

            * `Ignore` - ignores unexpected JSON fields.
            * `Error` - return an error when encountering unexpected JSON fields.
            * `Infer` - type-infer unexpected JSON fields and include them in the output. We recommend using this option by default

        * **Block Size** is the number of bytes to process in memory in parallel while reading files. We recommend you to keep this field with a default value: `10000`.

        {% endcut %}

    1. Toggle the **Result table schema** type:

        * The **Automatic** doesn't require further configuration.

          This feature attempts to deduce a schema from sample data in the bucket, leading to potentially incorrect schema. We recommend providing a detailed **Manual** schema for complex table structures.

        * The **Manual** type gives you two options to specify the schema:

            {% cut "Field list" %}

            * Click **Add Field** and specify the field properties:

            * The **name** of the field.

            * Select the field **type**.

            * (optional) Check **Key** to make the field a primary key. You can select more than one key.

                {% note warning %}

                Selecting more than one primary key for this table schema makes the whole table incompatible with {{ CH }}.

                {% endnote %}

            * Provide the CSV pattern identifying the column numbers starting with `0` in the **Path** field.

            {% endcut %}

            {% cut "JSON specification" %}

            Write a schema description in JSON format. For example, a schema could look as follows:

            ```json
            [
               {
                  "name": "remote_addr",
                  "type": "string"
               },
               {
                  "name": "remote_user",
                  "type": "string"
               },
               {
                  "name": "time_local",
                  "type": "string"
               },
               {
                  "name": "request",
                  "type": "string"
               },
               {
                  "name": "status",
                  "type": "int32"
               },
               {
                  "name": "bytes_sent",
                  "type": "int32"
               },
               {
                  "name": "http_referer",
                  "type": "string"
               },
               {
                  "name": "http_user_agent",
                  "type": "string"
               }
            ]
            ```

            {% endcut %}

    1. Click **Submit**.

* Model

    ## Overview
    
    The **S3 Source Connector** aggregates data from files stored in an S3-compatible storage bucket into a single table. It supports various file formats such as CSV, JSONL, and Parquet, and allows schema definition for the output data. The connector provides two modes of file replication: **polling** for new files or using an event-driven approach with **SQS** (Simple Queue Service).
    
    This document describes the configuration options and behavior of the S3 Source Connector. The connector is controlled via JSON or YAML configurations based on the `S3Source` Go structure.
    
    ---
    
    ## Configuration
    
    The S3 Source Connector is configured using the `S3Source` structure. Below is a breakdown of each configuration field.
    
    ### Example Configuration
    
    ```yaml
    Bucket: "my-data-bucket"
    ConnectionConfig:
    AccessKey: "your-access-key"
    SecretKey: "your-secret-key"
    Endpoint: "s3.amazonaws.com"
    UseSSL: true
    VerifySSL: true
    Region: "us-west-2"
    PathPrefix: "data/2023/"
    TableName: "s3_data_table"
    TableNamespace: "my_namespace"
    HideSystemCols: false
    ReadBatchSize: 1000
    InflightLimit: 5000000
    InputFormat: "CSV"
    OutputSchema:
    - ColumnName: "id"
      DataType: "string"
    - ColumnName: "value"
      DataType: "integer"
    PathPattern: "*.csv"
    Concurrency: 5
    Format:
    CSVSetting:
      Delimiter: ","
      QuoteChar: "\""
      EscapeChar: "\\"
      Encoding: "UTF-8"
      DoubleQuote: true
      BlockSize: 1048576
    EventSource:
    SQS:
      QueueName: "my-sqs-queue"
      OwnerAccountID: "123456789012"
      ConnectionConfig:
        AccessKey: "your-access-key"
        SecretKey: "your-secret-key"
        Endpoint: "sqs.us-west-2.amazonaws.com"
        UseSSL: true
        VerifySSL: true
        Region: "us-west-2"
    UnparsedPolicy: "fail"
    ```
    
    ### Fields Breakdown
    
    #### **Bucket** (`string`)
    - Specifies the S3 bucket name from which the files will be retrieved.
      - Example: `"my-data-bucket"`
    
    #### **ConnectionConfig** (`ConnectionConfig`)
    - Contains the configuration for connecting to the S3 bucket. It includes credentials, endpoint, region, and SSL settings.
    
      **Fields:**
        - `AccessKey`: The access key for the S3 bucket.
        - `SecretKey`: The secret key for the S3 bucket.
        - `Endpoint`: The S3-compatible endpoint (e.g., `"s3.amazonaws.com"`).
        - `UseSSL`: If set to `true`, the connection uses SSL.
        - `VerifySSL`: If set to `true`, the SSL certificate is verified.
        - `Region`: The AWS region where the bucket is hosted (e.g., `"us-west-2"`).
    
    #### **PathPrefix** (`string`)
    - Specifies the prefix of the file paths to filter the files in the S3 bucket.
      - Example: `"data/2023/"`
    
    #### **TableName** (`string`)
    - The name of the output table where aggregated data from the files will be stored.
      - Example: `"s3_data_table"`
    
    #### **TableNamespace** (`string`)
    - Defines the namespace for the table in which the data will be stored.
      - Example: `"my_namespace"`
    
    #### **HideSystemCols** (`bool`)
    - If set to `true`, system columns (`__file_name` and `__row_index`) are excluded from the output schema.
      - Example: `false`
    
    #### **ReadBatchSize** (`int`)
    - Specifies the number of rows read in each batch during ingestion.
      - Example: `1000`
    
    #### **InflightLimit** (`int64`)
    - Limits the number of bytes that can be processed in-flight during replication.
      - Example: `5000000`
    
    #### **InputFormat** (`server.ParsingFormat`)
    - The format of the input files. Supported formats include `CSV`, `JSONL`, and `Parquet`.
      - Example: `"CSV"`
    
    #### **OutputSchema** (`[]abstract.ColSchema`)
    - Defines the schema for the aggregated table. This includes column names and data types.
      - Example:
        ```yaml
        OutputSchema:
          - ColumnName: "id"
            DataType: "string"
          - ColumnName: "value"
            DataType: "integer"
        ```
    
    #### **AirbyteFormat** (`string`)
    - Used for backward compatibility with Airbyte. Specifies the raw format for later parsing.
    
    #### **PathPattern** (`string`)
    - A pattern that filters files to ingest, matching based on the file name (e.g., `"*.csv"`).
    
    #### **Concurrency** (`int64`)
    - Defines the number of concurrent processes for reading files.
      - Example: `5`
    
    #### **Format** (`Format`)
    - Specifies the settings for the file format (CSV, JSONL, Parquet, etc.).
    
      **CSVSetting Fields:**
        - `Delimiter`: The delimiter for CSV files (e.g., `","`).
        - `QuoteChar`: The character used to quote fields (e.g., `"\""`).
        - `EscapeChar`: The character used to escape fields (e.g., `"\""`).
        - `Encoding`: The encoding of the file (e.g., `"UTF-8"`).
        - `DoubleQuote`: Whether double quotes are used in CSV fields.
        - `BlockSize`: The block size for reading the file in bytes (e.g., `1048576`).
    
    #### **EventSource** (`EventSource`)
    - Defines how new files are detected for replication. The connector can either poll for new files or listen for events from **SQS** (Simple Queue Service).
    
      **SQS Fields:**
        - `QueueName`: The name of the SQS queue.
        - `OwnerAccountID`: The AWS account ID of the queue owner.
        - `ConnectionConfig`: Configuration for connecting to SQS (similar to `ConnectionConfig` for S3).
    
    #### **UnparsedPolicy** (`UnparsedPolicy`)
    - Specifies the policy to follow when encountering unparsed or malformed files. Options are:
        - `"fail"`: Stop processing and throw an error.
        - `"continue"`: Skip the unparsed file and continue.
        - `"retry"`: Retry processing the file.
    
    ---
    
    ## Ingestion Modes
    
    ### Snapshot Mode
    
    In **Snapshot Mode**, the S3 Source Connector collects all files from the specified bucket path and aggregates them into a single table. It reads the files based on the `PathPattern` and formats them according to the `InputFormat`.
    
    ### Event-Driven Mode with SQS
    
    In this mode, the connector listens for file creation events using **Amazon SQS**. When new files are added to the S3 bucket, an event is triggered via SQS, and the connector ingests these files in near real-time.
    
    ---
    
    ## Supported File Formats
    
    The connector supports the following file formats:
    - **CSV**: Customizable with delimiters, quote characters, and encoding options.
    - **JSONL**: Supports newline-separated JSON records.
    - **Parquet**: Columnar storage format.
    
    For each file format, the connector provides settings that can be configured to match the file's structure.
    
    ---
    
    ## Schema Definition
    
    The S3 Source Connector requires the user to define the schema for the output table. The schema is specified in the `OutputSchema` field, which includes column names and data types. The connector then maps the input data from the files into this schema during ingestion.
    
    ---
    
    ## Example
    
    TODO

{% endlist %}
