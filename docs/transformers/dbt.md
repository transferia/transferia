# DBT Transformer

- **Purpose**: Executes a dbt model to transform data within the target database.
- **Configuration**:
    - `ProfileName`: Name of the dbt profile matching the `profile` property in `dbt_project.yml`.
    - `GitBranch`: Branch or tag of the Git repository containing the dbt project.
    - `GitRepositoryLink`: URL to the Git repository with the dbt project (must start with `https://`).
    - `Operation`: The operation to execute within the dbt project.
- **Example**:
  ```yaml
  - dbt:
      ProfileName: ''
      GitBranch: ''
      GitRepositoryLink: ''
      Operation: ''
  ```
