# Mask Field Transformer

- **Purpose**: Applies a hash function to specified columns to protect sensitive data during transfer.
- **Configuration**:
    - `columns`: List of columns to mask.
    - `maskFunctionHash`: Defines the hash function and includes a user-defined salt for hashing.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - mask_field:
      columns:
        - address
      maskFunctionHash:
        userDefinedSalt: random_secret_string
      tables:
        includeTables:
          - public.foo
        excludeTables: null
    transformerId: ""
  ```
