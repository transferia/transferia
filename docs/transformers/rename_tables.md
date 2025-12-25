# Rename Tables Transformer

- **Purpose**: Renames tables during the transfer process.
- **Configuration**:
    - `renameTables`: List of renaming rules, each specifying the original and new table names and namespaces.
- **Example**:
  ```yaml
  - rename_tables:
      renameTables:
        - originalName:
            name: foo
            nameSpace: public
          newName:
            name: schmancy
            nameSpace: fancy
    transformerId: ""
  ```
