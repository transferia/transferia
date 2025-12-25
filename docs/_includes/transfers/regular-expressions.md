{% cut "Collection of regular expression patterns to parse table names" %}

**Pattern** | **Description** | **Example**
|---|---|---|
`abc` | An explicit series of characters | `test` returns the table names containing `test`.
`.` | A **character wildcard**. Use it to match an expression with defined character positions. | `t..t` returns `test`, `tent`, `tart` etc.
`\` | An **escape character**. Use it to match special characters. | `\_` returns the table names containing an underscore.
`?` | Use it to express that a character (or a group of characters) is optional. | `c?.n` returns `can`, `con`, `in`, `on`, `en`, etc.
`+` | Use it to express that a character (or a group of characters) can appear one and more times. | `-+` returns the table names containing `-`, `--`, `---` etc.
`{n}` | Use it to express that a character (or a group of characters) must appear explicitly `n` times | `-{2}` returns the table names containing `--`.
`{n,m}` | Use it to express that a character (or a group of characters) must appear between `n` and `m` times. | `_{1,3}` returns the table names containing `_`, `__` and `___`.
`\w` | An **alphanumeric wildcard**. Use it to match any alphanumeric characters. The match pattern is case-sensitive. | `\w+` returns the table names containing letters and/or digits.
`\W` | A **non-alphanumeric wildcard**. Use it to match any non-alphanumeric character. The match pattern is case-sensitive. | `\W+` returns the table names containing characters other than letters or digits.
`\d` | A **digit wildcard**. Use it to match any digit characters. The match pattern is case-sensitive. | `\d+` returns the table names containing digits.
`\D` | A **non-digit wildcard**. Use it to match any non-digit characters. The match pattern is case-sensitive. | `\D+` returns the table names containing any characters other than digits.
`$` | Use it to match the position after the table name's last character. | `metrics$` returns the table names ending with `metrics`.
`^` | Use it to match the position before the table name's first character. | This position is useful to define database names. For example, `^monthly_sales` returns all the tables from the `monthly_sales` database.

{% endcut %}
