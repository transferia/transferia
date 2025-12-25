# Raw CDC Document Grouper Transformer

- **Purpose**: Similar to the Raw Document Grouper but optimized for Change Data Capture (CDC) processing, grouping fields based on a specified key.
- **Configuration**:
    - `keys`: List of primary key columns for grouping.
    - `fields`: List of fields to be grouped into a raw document.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - raw_cdc_doc_grouper:
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
