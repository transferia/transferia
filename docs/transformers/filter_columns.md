# Filter Columns Transformer

- **Purpose**: Filters the list of columns transferred from the data source.
- **Configuration**:
    - `columns`:
        - `includeColumns`: List of columns to include (supports regular expressions).
        - `excludeColumns`: List of columns to exclude.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - filter_columns:
      columns:
        includeColumns:
          - ^.*
        excludeColumns: null
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```
