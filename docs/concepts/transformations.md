---
title: "Transformations in {{ data-transfer-name }}"
description: "Transform the data you replicate with Transfer on the fly with built-in transformers. Convert values, apply filters, perform SQL-like transformations, and more"
---

# Transformations in {{ data-transfer-name }}

{{ DC }} {{ data-transfer-name }} can apply transformations to the transferred data on the fly.

You can configure transformations when creating or editing a transfer.
To do that, click **+ Transformation** on the transfer configuration page and provide the desired settings.

The list of available transformations in each transfer depends on the source and target types.

## What is transformer

We can apply stateless transformation on our `inflight` data.
This is based on our data model

![data-model](../transformers/assets/data_model_transformer.png)

Each batch of changes can be transformed into new batch of changes:

![transformation](../transformers/assets/transformer_data_flow.png)

### How to add new transformer

1. Create new package
2. Implemenet `abstract.Transformer` interface
3. Register implementation

Example:

#### Implementation of `abstract.Transformer`

```go
type DummyTransformer struct {
}

func (r *DummyTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (r *DummyTransformer) Suitable(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

func (r *DummyTransformer) ResultSchema(original abstract.TableColumns) abstract.TableColumns {
	return original
}

func (r *DummyTransformer) Description() string {
	return "this transformer do nothing"
}
```

# Configuration Model

### Full Example Configuration

Here is how transfer config with included transformers looks like.
Transformers applied in same order as specified in transformers section.

```yaml
id: test
type: SNAPSHOT_AND_INCREMENT
src:
  type: mysql
  params:
    Host: mysql
    User: myuser
    Password: mypassword
    Database: mydb
    Port: 3306
dst:
  type: ch
  params:
    ShardsList:
      - Hosts:
          - clickhouse
    HTTPPort: 8123
    NativePort: 9000
    Database: default
    User: default
    Password: "ch_password"
transformation:
  debugmode: false
  transformers:
    - sql:
        query: SELECT * FROM table
        tables:
          includeTables:
            - public.test
          excludeTables: null
      transformerId: ""
    - mask_field:
        columns:
          - address
        maskFunctionHash:
          userDefinedSalt: random_secret_string
        tables:
          includeTables:
            - public.test
          excludeTables: null
      transformerId: ""
    - dbt:
        ProfileName: 'my_dbt_profile'
        GitBranch: 'main'
        GitRepositoryLink: 'https://github.com/myorg/mydbtproject'
        Operation: 'run'
      transformerId: ""
errorsoutput: null
data_objects:
  include_objects:
    - public.test
type_system_version: 9
```

---

## Additional Configuration Options
- `debugmode`: Enables or disables debug mode for transformation execution.
- `errorsoutput`: Defines how transformation errors should be handled.
- `transformerId`: Assign for each transformer, useful for logging

This YAML-based approach to configuring transformations in **DoubleCloud Transfer** provides a structured and flexible way to modify and optimize data flows, ensuring smooth and efficient data pipeline operations.
