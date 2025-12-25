# Convert to String Transformer

- **Purpose**: Converts specified data columns in a table to string format.
- **Configuration**:
    - `columns`:
        - `includeColumns`: List of columns to include for conversion.
        - `excludeColumns`: List of columns to exclude from conversion.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - convert_to_string:
      columns:
        includeColumns:
          - salary
        excludeColumns: null
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```
