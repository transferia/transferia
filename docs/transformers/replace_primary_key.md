# Replace Primary Key Transformer

- **Purpose**: Reassigns the primary key columns on the target table.
- **Configuration**:
    - `keys`: List of columns to be used as primary keys.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - replace_primary_key:
      keys:
        - id
        - first_name
        - last_name
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```
