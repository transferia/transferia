# Getting Started Guide

This guide will walk you through building the Transfer from the source repository and using it to manage data transfers. The process involves building the tool, validating configurations, checking the health of transfers, activating them, and finally verifying the data in your target database.

## Step 1: Build Transfer

To build the ingestion engine, clone the repository and run the build command using `make`. The final executable will be located in the `binaries/trcli` directory.

### Build the Project

1. Clone the repository to your local machine:
   ```bash
   git clone git@github.com:transferia/transferia.git
   cd transfer
   ```

2. Build the project:
   ```bash
   make build
   ```

   After the build completes, the compiled binary will be located at `binaries/trcli`.

## Step 2: Prepare the Transfer Configuration

You will need a configuration file, typically named `transfer.yaml`, that defines the source, target, and transfer details for your data pipeline.

Below is a sample `transfer.yaml` configuration file for transferring data from a source like **Postgres** to a target like **Clickhouse**:

```yaml
source:
  type: "pg"
  params:
    host: "source-db-host"
    port: 5432
    database: "source_database"
    user: "source_user"
    password: "source_password"
    tables:
      - name: "personas"

target:
  type: "ch"
  params:
    host: "clickhouse-db-host"
    port: 9000
    database: "target_database"
    user: "clickhouse_user"
    password: "clickhouse_password"

transfer:
  mode: "snapshot"
  batch_size: 10000
  concurrency: 4
```

This is an example configuration, which can be adapted based on your source (Postgres, MySQL, Kafka, etc.) and target (Clickhouse, S3, etc.).

## Step 3: Validate the Configuration

Use the `trcli` tool to validate the `transfer.yaml` configuration. This step ensures that the configuration is correctly formatted and free from errors.

### Command to Validate the Transfer Configuration:

```bash
./binaries/trcli validate --transfer transfer.yaml --log-config=minimal
```

- The `--transfer transfer.yaml` argument specifies the configuration file.
- The `--log-config=minimal` argument sets minimal logging for the output.

If the validation succeeds, you'll see a confirmation message. If there are any issues with the configuration, the validation process will report them so you can correct them.

## Step 4: Check Transfer Health

Before activating the data transfer, it's important to run a health check. This checks whether the source, target, and any intermediary components are accessible and ready.

### Command to Run a Health Check:

```bash
./binaries/trcli check --transfer transfer.yaml --log-config=minimal
```

- This will validate connectivity to both the source and target databases, check schema compatibility, and ensure that all resources are prepared for data transfer.

## Step 5: Activate the Transfer

Once the configuration is validated and the health check passes, you can activate the transfer to begin moving data.

### Command to Activate the Data Transfer:

```bash
./binaries/trcli activate --transfer transfer.yaml --log-config=minimal
```

- This command will initiate the data migration process based on the configuration specified in `transfer.yaml`. The logs will show progress as the data is transferred from the source to the target.

![Made with VHS](https://vhs.charm.sh/vhs-3ETIytnxDtBmrgkcOX3ZBf.gif)

