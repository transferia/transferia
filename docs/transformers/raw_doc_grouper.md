# Raw Document Grouper Transformer

- **Purpose**: Converts data into raw JSON format, grouping specified fields.
- **Configuration**:
    - `keys`: List of primary key columns for grouping.
    - `fields`: List of fields to be grouped into a raw document.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - raw_doc_grouper:
      keys:
        - id
      fields:
        - salary
        - address
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```
