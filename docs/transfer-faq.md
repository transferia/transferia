# Frequently asked question about {{ DC }} {{ data-transfer-name }}

Questions:

* [Does {{ data-transfer-name }} support encryption?](#encryption-support)
* [Can I transfer empty tables in {{ data-transfer-name }}?](#empty-tables)
* [Do you support changing the schema after activating a transfer?](#schema-change-after-activation)

## Does {{ data-transfer-name }} support encryption? {#encryption-support}

If a {{ DC }} cluster (source or target) has encryption enabled, {{ data-transfer-name }} will support this encryption.

## Can I transfer empty tables in {{ data-transfer-name }}? {#empty-tables}

{{ data-transfer-name }} doesn't support transfer of empty tables or other empty entities.

There's an exception — in homogeneous transfers, you can copy metadata separately. 
In this case, empty tables and other entities (views, triggers, etc.) will be transferred.

## Do you support changing the schema after activating a transfer? {#schema-change-after-activation}

Some transfer target support on-flight schema changes (such as Clickhouse)
