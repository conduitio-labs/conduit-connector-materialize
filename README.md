# Conduit Connector Materialize

### General

This connector allows you to move data from any [Conduit Source](https://www.conduit.io/docs/connectors/overview) to a [Materialize Table](https://materialize.com/docs/sql/create-table/). This connector is a destination connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- [Materialize](https://materialize.com/docs/install/) v0.26.0

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back to use the table configured in the connector. This way the Destination can support multiple tables in the same connector, provided the user has proper access to those tables.

### Known limitations

Materialize doesn't yet support the following features:
- Primary keys, unique constraints, and `UPSERT`. This means there is no guarantee that Materialize will not have duplicates. Delete and update will affect all records found by the specified key and insert only appends data.
- Placeholders (`?`) within prepared statements.

These limitations are the reason why the [PostgreSQL connector](https://github.com/ConduitIO/conduit-connector-postgres) cannot be used with Materialize.

### Configuration Options

| name                      | description                                                                                                                         | required | default                |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------- |
| `url`                     | The connection URL for Materialize instance.                                                                                        | true     |                        |
| `table`                   | The table name of the table in Materialize that the connector should write to, by default.                                                                   | true     |                        |
| `key`                     | The column name used when updating and deleting records.                                                                         | true    |  |

### Testing 

Run `make test` in order to run all the unit and integration tests. This requires [Docker](https://docs.docker.com/engine/install/ubuntu/) to be installed.