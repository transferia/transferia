# NATS Connector

You can use this connector as a **source** endpoint.


## Overview

The **NATS Source Connector** enables replication from **NATS** streams. [NATS](https://docs.nats.io/) is a high-performance messaging system that supports real-time data streaming and event-driven architectures. This connector is designed for real-time data ingestion from NATS subjects, parsing messages using configurable formats.

---

  ## Configuration

  The **NATS Source Connector** is configured using the `NatsSource` structure. Below is a detailed breakdown of the configuration fields.

  ### Example Configuration

```yaml
    Config:
      Connection:
        NatsConnectionOptions:
          URL: "nats://localhost:4222"
          MaxReconnect: 10
      StreamIngestionConfigs:
        - Stream: "events_stream"
          SubjectIngestionConfigs:
            - TableName: "events_table"
              ConsumerConfig:
                Durable_Name: "events_consumer"
                Name: "events_consumer"
                Deliver_Policy: 0
                Ack_Policy: 1
                Filter_Subject: "events.*"
                Max_Batch: 100
              ParserConfig:
                "json.lb":
                  AddRest: false
                  AddSystemCols: false
                  DropUnparsed: false
                  Fields:
                    - Name: "cluster_id"
                      Type: "string"
                    - Name: "cluster_name"
                      Type: "string"
                    - Name: "host"
                      Type: "string"
                    - Name: "database"
                      Type: "string"
                    - Name: "pid"
                      Type: "uint32"
                    - Name: "version"
                      Type: "uint64"
   ```

## Fields Breakdown

### **Connection** (`Connection`)
Contains the configuration for connecting to the NATS server. It wraps nats connections options.
### **NatsConnectionOptions** (`NatsConnectionOptions`)
- **URL** (`string`): The NATS server address.
  - Example: `"nats://localhost:4222"`
- **MaxReconnect** (`int`): The maximum number of reconnection attempts to a nats server
  - Example: `10`

### **StreamIngestionConfigs** (`[]StreamIngestionConfig`)
Defines the list of NATS streams and its configurations from which data will be ingested.
- **Stream** (`string`) The name of the NATS Stream from which messages are consumed.
  - Example: `"events_stream"`

### **SubjectIngestionConfigs** (`[]SubjectIngestionConfig`) 
Defines the list of subjects within a Stream that will be consumed.
- **TableName** (`string`)
  - The logical table name associated with the subject, used for storing structured data.
    - Example: `"events_table"`

### **ConsumerConfig** (`NatsConsumerOptions`)
Configuration settings for the NATS consumer.
- **Durable** (`string`): The durable name for the consumer, ensuring state persistence across restarts.
  - Example: `"event_consumer"`
- **AckPolicy** (`string`): The acknowledgment policy for message processing.
  - Options: `"explicit"`, `"all"`, `"none"`
  - Example: `"explicit"`
- **MaxDeliver** (`int`): The maximum number of times a message will be redelivered if not acknowledged.
  - Example: `5`
- **FilterSubject** (`string`): A subject filter to narrow down message consumption.
  - Example: `"events.*"`

### **ParserConfig** (`map[string]interface{}`)
Defines how incoming NATS messages are parsed into structured data.
- **Parser Type** (`string`)
  - Specifies the Parser format used for extracting fields.
    - Example: `"json.lb"`
- **AddRest** (`bool`)
  - If `true`, any unparsed fields are added as additional columns.
    - Example: `false`
- **AddSystemCols** (`bool`)
  - If `true`, system-related metadata columns (such as timestamps) are included in the output.
    - Example: `false`
- **DropUnparsed** (`bool`)
  - If `true`, messages that cannot be fully parsed will be dropped.
    - Example: `false`
- **Fields** (`[]FieldConfig`)
  - A list of fields extracted from incoming NATS messages, defining their names and data types.
  - Each field consists of:
    - **Name** (`string`): The field name.
    - **Type** (`string`): The data type of the field.