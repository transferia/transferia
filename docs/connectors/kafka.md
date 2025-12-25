---
title: "{{ KF }} connector"
description: "Configure the {{ KF }} connector to transfer data from {{ KF }} with {{ DC }} {{ data-transfer-name }}"

---

# {{ KF }} connector

You can use this connector both for **source** and **target** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings**, select the **Connection type**:

      {% cut "On-premise" %}

      1. Under **Broker URLs**, click **+ URL** to add brokers.

         Enter IP address or domain names (FQDN) of broker hosts.

         {% note tip %}

         If the {{ KF }} port number isn't standard, separate it with a colon as follows:

         `<Broker host IP of FQDN>:<port number>`

         {% endnote %}

      1. Click **SSL** if you want to encrypt your connection.

      1. Add the **PEM Certificate**. Click **Choose a file** to upload a certificate file (public key) in PEM format or provide it as text.

      1. Select the **Authentication** method:

         1. To verify your identity with **SASL**, specify the following options:

            * Your **User Name**,

            * **Password** for this user name,

            * Encryption **Mechanism**.

         1. Set this property to **No Authentication** if you don't need authentication.

      {% endcut %}

   1. Specify the **Topic full name**.

   1. Configure **Advanced settings** → **Conversion rules**:

      1. Click **+ Conversion rules**.

      1. Select the **Data format**. Currently, we support the `JSON` format.

      1. Choose the **Data scheme**:

         {% cut "Field list" %}

         1. Under **Field list** → **Field** click **Add Field** and specify the field properties:

         1. The **name** of the field.

         1. Select the field **type**.

         1. (optional) Check **Key** to make the field a table sorting key.

         1. (optional) Check the **Required** box to make the field obligatory.

         1. Provide the **Path** to redefine the names of columns in the table following

            ```sh
            library.shelf[a].book[b].title`
            ```

         {% endcut %}

         {% cut "JSON Spec" %}

         Click **Choose a file** to provide a file with schema description in JSON format. The schema should look as follows:

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

      1. Check the **Add a column for missing keys** box if you need to collect keys missing from the scheme.

      1. Check **Enable null values in keys** if needed.

* Model

    ## Overview
    
    The **Kafka Source Connector** enables replication from **Apache Kafka** topics. Kafka is a distributed messaging system, which allows the ingestion of real-time, high-throughput data from Kafka topics into the system. This connector is tailored for replication, consuming data in real-time from Kafka topics and parsing messages using a variety of formats.
    
    Kafka is a queue-like source, meaning it is generally unstructured, so the use of a **parser** is necessary to convert Kafka messages into structured data. This document also references the parser configuration, which is discussed in detail in separate documentation.
    
    ---
    
    ## Configuration
    
    The **Kafka Source Connector** is configured using the `KafkaSource` structure. Below is a detailed breakdown of the configuration fields.
    
    ### Example Configuration
    
    ```yaml
    KafkaSource:
      Connection:
        TLS: "Enabled"
        Brokers:
          - "broker1.kafka.local:9092"
          - "broker2.kafka.local:9092"
        TLSFile: "/path/to/tls/certificate.pem"
      Auth:
        Enabled: true
        Mechanism: "SCRAM-SHA-256"
        User: "kafka_user"
        Password: "kafka_password"
      Topic: "my-kafka-topic"
      GroupTopics:
        - "group1"
        - "group2"
      Transformer:
        TransformConfig: { ... }
      BufferSize: 1048576
      ParserConfig:
        "json.lb":
          AddRest: true
          AddSystemCols: false
          DropUnparsed: false
          Fields:
            - name: "event_id"
              type: "string"
            - name: "event_type"
              type: "string"
            - name: "timestamp"
              type: "timestamp"
      IsHomo: false
      SynchronizeIsNeeded: true
    ```
    
    ### Fields Breakdown
    
    #### **Connection** (`KafkaConnectionOptions`)
    - Contains the configuration for connecting to the Kafka cluster.
        - **TLS** (`TLSMode`): Configures whether TLS is used for communication. Options include:
            - `Enabled`: Enables TLS for secure communication.
            - `Disabled`: Disables TLS, using an unencrypted connection.
            - `Default`: Uses the default TLS configuration based on the Kafka setup.
        - **Brokers** (`[]string`): A list of Kafka brokers in the format `host:port`.
        - **TLSFile** (`string`): The path to the PEM file for secure TLS communication. Optional but necessary when `TLS` is set to `Enabled`.
    
    #### **Auth** (`KafkaAuth`)
    - Optional authentication settings for Kafka, enabled only when `Enabled` is set to `true`.
        - **Enabled** (`bool`): Indicates if authentication is required.
        - **Mechanism** (`string`): Authentication mechanism (e.g., `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`).
        - **User** (`string`): The username for authentication.
        - **Password** (`string`): The password for authentication.
    
    #### **Topic** (`string`)
    - The name of the Kafka topic from which data is being consumed.
      - Example: `"my-kafka-topic"`
    
    #### **GroupTopics** (`[]string`)
    - An optional list of group topics for multi-topic ingestion. When configured, the connector consumes from multiple topics.
      - Example: `["group1", "group2"]`
    
    #### **Transformer** (`server.DataTransformOptions`)
    - Optional field for applying data transformations as the data is ingested from Kafka.
        - **TransformConfig**: Allows defining specific transformations, filters, and data cleaning operations. Refer to the **Data Transformation Documentation** for more details.
    
    #### **BufferSize** (`server.BytesSize`)
    - The size of the buffer used for handling Kafka messages, specified in bytes. This is not a strict memory limit but affects the handling of batch operations.
      - Example: `1048576` (1 MB)
    
    #### **ParserConfig** (`map[string]interface{}`)
    - Specifies the configuration for the parser responsible for processing and structuring the raw Kafka messages. Kafka messages can be parsed into structured data using different formats like **JSON**, **TSKV**, **Protobuf**, or **Raw Table**.
    
        - Example of a JSON parser configuration:
          ```yaml
          "json.lb":
            AddRest: true
            AddSystemCols: false
            DropUnparsed: false
            Fields:
              - name: "event_id"
                type: "string"
              - name: "event_type"
                type: "string"
              - name: "timestamp"
                type: "timestamp"
          ```
    
      For more details on parsers, refer to the [Parser Documentation](#).
    
    #### **IsHomo** (`bool`)
    - If set to `true`, this enables the Kafka mirror protocol, which allows mirroring between Kafka clusters. It only works when a **Kafka target** is configured.
    
    #### **SynchronizeIsNeeded** (`bool`)
    - When `true`, the connector sends synchronization events when releasing Kafka partitions, which can be important for certain data consistency requirements.
    
    ---
    
    ## Parser Types
    
    The Kafka Source Connector supports the following parsers:
    
    1. **JSON Parser**: Parses JSON messages and extracts fields based on the configuration.
        - Example: `"json.lb"`
    
       2. **TSKV Parser**: Parses tab-separated key-value message formats, typically used in logging systems.
    
       3. **Protobuf Parser**: For messages serialized using Protocol Buffers, a compact, language-neutral data format.
    
       4. **Schema Registry Parser**: Uses an external schema registry for parsing messages (e.g., Avro).
    
       5. **Raw Table Parser**: Treats each Kafka message as a logical row, storing metadata like the topic, partition, offset, timestamp, key, and value.
    
    ---
    
    ## Supported Ingestion Mode
    
    ### Replication Mode
    
    The **Kafka Source Connector** supports **replication mode**. This mode continuously ingests data from the specified Kafka topic(s) in real time, ensuring that all messages are captured and processed as they are published to the stream.
    
    ---
    
    ## Example JSON Parser Configuration
    
    Below is an example configuration for a **JSON Parser**, which extracts fields from JSON messages published to Kafka topics:
    
    ```yaml
    "json.lb":
      AddRest: true
      AddSystemCols: false
      DropUnparsed: false
      Fields:
        - name: "event_id"
          type: "string"
        - name: "event_type"
          type: "string"
        - name: "timestamp"
          type: "timestamp"
    ```
    
    - `AddRest`: If set to `true`, any unparsed fields are added to the output as an additional column.
      - `AddSystemCols`: When `false`, the Kafka system columns (e.g., topic, partition, offset) are not added to the output.
      - `DropUnparsed`: When `false`, fields that cannot be parsed are kept in the record.
      - `Fields`: Defines the specific fields extracted from the JSON messages, with each field having a name and a data type.
    
    ---
    
    ## Conclusion
    
    The **Kafka Source Connector** offers a flexible solution for ingesting real-time data from Kafka topics into a structured environment. With support for secure connections, advanced parser configurations, and multi-topic ingestion, this connector can be tailored to various Kafka setups. Additionally, the use of **parsers** allows unstructured Kafka messages to be converted into structured formats, ensuring seamless data processing and replication.
    
    For more information on configuring parsers, refer to the [Parser Documentation](#).


{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings**, select the **Connection type**:

      {% cut "On-premise cluster" %}

      1. Provide **Broker URLs**:

      1. Click **Broker URLs** → **+ URL** to add brokers.

         Enter the IP addresses or domain names (FQDN) of broker hosts.

         {% note tip %}

         If the {{ KF }} port number isn't standard, separate it with a colon as follows:

         ```sh
         <Broker host IP of FQDN>:<port number>
         ```

         {% endnote %}

      1. Click **SSL** if you want to encrypt your connection.

      1. Add the **PEM Certificate**. Click **Choose a file** to upload a certificate file (public key) in PEM format or paste it as text.

      1. Select the **Authentication** method:

         1. To verify your identity with **SASL**, specify the following options:

            * Your **User Name**,

            * **Password** for this user name,

            * Encryption **Mechanism**.

         1. Set this property to **No Authentication** if you don't need to provide credentials.

      {% endcut %}

   1. Specify the **Apache Kafka topic settings**:

      {% cut "Topic full name" %}

      1. Specify the **Topic full name** in the target cluster as a full path.

      {% endcut %}

      {% cut "Topic prefix" %}

      1. Specify the **Topic prefix** in the target cluster. The format is the following: `topic_prefix.schema.table_name`, it's similar to the [Debezium ![external link](../_assets/external-link.svg)](https://debezium.io/) settings.

      {% endcut %}

      To parse multiple topic names in the above sections, you can use regular expressions:

      {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

   1. Configure **Advanced settings**:

      1. Check the **Save tx order** box if you want to write database transactions in the same order.

         When you enable this setting, the service writes all tables from the source database into a single [partition ![external link](../_assets/external-link.svg)](https://kafka.apache.org/documentation/#intro_concepts_and_terms). The tables order is preserved.

         In the default mode, when this setting is disabled, the service splits the data you transfer by table names. Each table goes to a separate partition.

         {% note warning %}

         This setting applies only if a transfer meets both conditions below:

         * The source endpoint is [{{ PG }}](postgresql.md#source-endpoint-configuration) or [{{ MY }}](mysql.md#source-endpoint-configuration).

         * You set `Topic prefix` in **Apache Kafka topic settings**.

         {% endnote %}

{% endlist %}
