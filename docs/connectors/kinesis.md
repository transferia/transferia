---
title: "Kinesis Data Streams connector"
description: "Configure the Kinesis Data Streams connector to transfer data from AWS Kinesis Data Streams with {{ DC }} {{ data-transfer-name }}"
---

# Kinesis Data Streams connector

[Amazon Kinesis Data Streams ![external link](../_assets/external-link.svg)](https://aws.amazon.com/kinesis/data-streams/)
is a serverless service for capturing, processing, and storing data streams of various scales.
The Kinesis Data Streams connector enables you
to transfer data from AWS Kinesis Data Streams with {{ DC }} {{ data-transfer-name }}.

You can use this connector in **source** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

    To configure a Kinesis Data Streams source endpoint, provide the following settings:

    1. In **AWS region**, specify the region where your Kinesis Data Streams is deployed.

    1. In **Kinesis stream**, enter the stream name.

    1. In **Access key ID** and **Secret access key**,
        enter the credentials to access the stream.

    1. In **Conversion rules**, select how you want to transform the streamed data.

* Model

  ## Overview
  
  The **Kinesis Source Connector** is designed to enable replication from **Amazon Kinesis** streams. Since Kinesis is a queue-like data source, it supports only **replication mode**, meaning it continuously ingests data in real time as messages arrive in the Kinesis stream. Due to the unstructured nature of queue-based sources like Kinesis, a **parser** must be specified to convert incoming records into a structured format suitable for data ingestion.
  
  This document outlines the configuration options for the Kinesis Source Connector and provides an example of a simple JSON parser configuration.
  
  ---
  
  ## Configuration
  
  The Kinesis Source Connector is configured using the `KinesisSource` structure. Below is a breakdown of each configuration field.
  
  ### Example Configuration
  
  ```yaml
  Endpoint: "kinesis.us-west-2.amazonaws.com"
  Region: "us-west-2"
  Stream: "my-kinesis-stream"
  BufferSize: 1024
  AccessKey: "your-access-key"
  SecretKey: "your-secret-key"
  ParserConfig:
  "json.lb":
    AddRest: true
    AddSystemCols: false
    DropUnparsed: false
    Fields:
      - name: "cluster_id"
        type: "string"
      - name: "cluster_name"
        type: "string"
      - name: "host"
        type: "string"
      - name: "database"
        type: "string"
      - name: "pid"
        type: "uint32"
      - name: "version"
        type: "uint64"
  ```
  
  ### Fields Breakdown
  
  #### **Endpoint** (`string`)
  - The Amazon Kinesis endpoint URL.
    - Example: `"kinesis.us-west-2.amazonaws.com"`
  
  #### **Region** (`string`)
  - The AWS region where the Kinesis stream is hosted.
    - Example: `"us-west-2"`
  
  #### **Stream** (`string`)
  - The name of the Kinesis stream from which the data is being consumed.
    - Example: `"my-kinesis-stream"`
  
  #### **BufferSize** (`int`)
  - The size of the buffer used to process the incoming records from the Kinesis stream, specified in bytes.
    - Example: `1024`
  
  #### **AccessKey** (`string`)
  - The AWS access key for authenticating with the Kinesis service.
  
  #### **SecretKey** (`model.SecretString`)
  - The AWS secret key for authenticating with the Kinesis service. It should be handled securely.
  
  #### **ParserConfig** (`map[string]interface{}`)
  - The configuration for the parser that processes and structures the unstructured data from Kinesis.
    - You must define a parser for converting the raw queue messages into a structured format. The `ParserConfig` is a flexible field that allows specifying different parser types such as **JSON**, **TSKV**, **Protobuf**, **Schema Registry**, or **Raw Table**.
  
      - Example of a simple JSON parser configuration:
        ```yaml
        "json.lb":
          AddRest: true
          AddSystemCols: false
          DropUnparsed: false
          Fields:
            - name: "cluster_id"
              type: "string"
            - name: "cluster_name"
              type: "string"
            - name: "host"
              type: "string"
            - name: "database"
              type: "string"
            - name: "pid"
              type: "uint32"
            - name: "version"
              type: "uint64"
        ```
  
  ---
  
  ## Parser Types
  
  The Kinesis Source Connector supports a variety of parser types to handle different data formats coming from the stream:
  
  1. **JSON Parser**: Parses the message as JSON, extracting the specified fields into structured columns.
     - Example: `"json.lb"`
  
     2. **TSKV Parser**: Handles tab-separated key-value formats, common in logging systems.
  
     3. **Protobuf Parser**: Used for parsing messages serialized using Protocol Buffers, a language-neutral, platform-neutral extensible mechanism for serializing structured data.
  
     4. **Schema Registry Parser**: Useful when dealing with Avro or Protobuf data formats registered in a schema registry.
  
     5. **Raw Table Parser**: Emits each queue message as a logical row with a predefined schema, where the message is treated as raw text. This parser emits system columns like `topic`, `partition`, `offset`, `timestamp`, `key`, and `value`.
  
  ---
  
  ## JSON Parser Example
  
  Here is a breakdown of the example **JSON Parser** configuration:
  
  ```yaml
  "json.lb":
    AddRest: true
    AddSystemCols: false
    DropUnparsed: false
    Fields:
      - name: "cluster_id"
        type: "string"
      - name: "cluster_name"
        type: "string"
      - name: "host"
        type: "string"
      - name: "database"
        type: "string"
      - name: "pid"
        type: "uint32"
      - name: "version"
        type: "uint64"
  ```
  
  - `AddRest`: If set to `true`, any unparsed data is added to the output as an additional column.
    - `AddSystemCols`: If set to `false`, system columns (e.g., message metadata) are not added to the output schema.
    - `DropUnparsed`: If set to `false`, unparsed fields will be retained instead of being dropped.
    - `Fields`: Defines the specific fields extracted from the JSON messages, with the name and data type for each.
  
  For more detailed information about parsers and their specific configurations, please refer to the [Parser Documentation](#) (linked to the external parser docs).
  
  ---
  
  ## Supported Ingestion Mode
  
  ### Replication Mode
  
  The **Kinesis Source Connector** supports **only replication mode** since it is a queue-like source. This mode allows real-time continuous ingestion of messages from the Kinesis stream into the system, where each message is treated as a record.
  
  ---
  
  ## Conclusion
  
  The Kinesis Source Connector enables real-time replication of unstructured data from Kinesis streams into the system by leveraging different parsers to convert raw queue messages into structured formats. With flexible parser configurations and support for various formats like JSON, Protobuf, and raw tables, this connector ensures that you can handle any message structure or format that your Kinesis stream provides.


{% endlist %}
