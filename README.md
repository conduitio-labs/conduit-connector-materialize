# Conduit Connector Materialize

### General

This connector allows you to move data from any [Conduit Source](https://www.conduit.io/docs/connectors/overview) to a [Materialize Table](https://materialize.com/docs/sql/create-table/). This connector is a destination connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- [Materialize](https://materialize.com/docs/install/) v0.26.0

### Configuration Options

| name                      | description                                                                                                                         | required | default                |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------- |
| `url`                     | The connection URL for Materialize instance.                                                                                        | true     |                        |
| `table`                   | The table name of the table in Materialize that the connector should write to, by default.                                                                   | true     |                        |
| `key`                     | The column name used when updating and deleting records.                                                                         | true    |  |
