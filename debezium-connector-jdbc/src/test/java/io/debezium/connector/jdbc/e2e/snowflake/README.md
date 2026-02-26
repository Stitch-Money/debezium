# NOTE

This package exists to allow end to end testing of Snowflake Dialect on
debezium events. These classes mimick the classes in the parent directory
but accomodate testing for snowflake like custom type assertions and inclusion
of the custom hibernate dialect.

# File Summary

## `AbstractJdbcSinkIT.java`

The Snowflake-specific base class for all JDBC sink connector integration tests. It mirrors the parent package's `AbstractJdbcSinkIT` but injects the custom Snowflake Hibernate dialect (`io.debezium.connector.jdbc.hibernate.dialect.snowflake.SnowflakeDialect`) when starting the sink task. It manages the full test lifecycle: initialising JSON key/value converters, starting a `JdbcSinkConnectorTask`, spinning up a Kafka consumer on a background thread to poll for records matching the source topic, and feeding the consumed `SinkRecord`s into the sink task. It also builds `ConnectorConfiguration` for each supported source type (MySQL, Postgres, SQL Server, Oracle).

## `AbstractJdbcSinkPipelineIT.java`

The main body of Snowflake e2e pipeline tests (~3800 lines). It corresponds to the parent package's `AbstractJdbcSinkPipelineIT` and contains `@TestTemplate` methods that are invoked across a matrix of source databases, flatten/unflatten modes, and temporal precision modes (via `SourcePipelineInvocationContextProvider`). Each test method creates a source table, starts the source connector, consumes CDC events through Kafka, writes them to Snowflake via the sink task, and then asserts column types and row values in Snowflake. Tests cover all major data types (boolean, integer, float, decimal, string, binary, JSON, XML, UUID, enum, temporal types, intervals, etc.) as well as connector features like schema evolution, primary key modes, insert/upsert modes, and column type propagation. Several tests are adapted for Snowflake-specific behaviour (e.g. uppercased identifiers, `VARIANT` for JSON/XML, `TIMESTAMPNTZ`/`TIMESTAMPTZ` for timestamps). It declares abstract methods for type name resolution that concrete subclasses must implement.

## `JdbcSinkPipelineToSnowflakeIT.java`

The concrete, runnable test class — equivalent to `JdbcSinkPipelineToMySqlIT`, `JdbcSinkPipelineToPostgresIT`, etc. in the parent package. It extends `AbstractJdbcSinkPipelineIT`, registers the `SnowflakeSinkDatabaseContextProvider` JUnit extension via `@ExtendWith`, and is tagged with `e2e` and `e2e-snowflake` for selective test execution. Its sole responsibility is implementing the abstract type-mapping methods to return the Snowflake-specific SQL type names (e.g. `NUMBER` for integers, `FLOAT` for decimals, `VARIANT` for JSON/XML, `TIMESTAMPNTZ`/`TIMESTAMPTZ` for timestamps, `BINARY` for byte arrays, etc.).
