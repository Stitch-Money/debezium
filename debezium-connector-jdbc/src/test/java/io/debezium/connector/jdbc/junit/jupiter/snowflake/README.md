# NOTE

This `snowflake` package was added to compensate for the fact that the
test configuration (jupyter extension) classes by default want to inject
a container (using testcontainers). See [JdbcConnectionProvider](../JdbcConnectionProvider.java) class for an example.

This is not possible for Snowflake unless [localstack](https://www.localstack.cloud/), integrates their Snowflake product with [testcontainers](https://testcontainers.com/).

Consequently, we re-implement several of the key classes needed to configure e2e and integration tests.

# File Summary

## `JdbcConnectionProvider.java`

Replaces the parent package's `JdbcConnectionProvider`, which depends on a `JdbcDatabaseContainer` from testcontainers. This version instead accepts a raw JDBC URL and `Properties` object to establish a connection to a live Snowflake instance via `DriverManager`. It provides the same core API surface — `execute()`, `getConnection()`, `close()` — so that higher-level test classes can interact with Snowflake identically to how they interact with containerised databases.

## `Sink.java`

The Snowflake-specific counterpart of the parent package's `Sink`. Extends the local `JdbcConnectionProvider` (rather than the testcontainers-based one) and hard-codes `SinkType.SNOWFLAKE`. Provides table/column name formatting (uppercasing, matching Snowflake's identifier behaviour), column type and value assertions via `assertj-db`, and row-level result set assertions. The `assertColumn` methods query Snowflake's JDBC `DatabaseMetaData` using the `SNOWFLAKE_DB_NAME` and `SNOWFLAKE_SCHEMA_NAME` environment variables to locate the correct catalog/schema.

## `AbstractSinkDatabaseContextProvider.java`

A JUnit 5 extension that mirrors the parent package's `AbstractSinkDatabaseContextProvider`, but replaces container lifecycle management with Snowflake schema lifecycle management. On `beforeAll` it drops and recreates the test schema (via a separate `setupSink` connected to the `PUBLIC` schema), and on `afterAll` it closes the connection. It resolves `Sink` parameters for test methods and honours `@SkipWhenSink` / `@SkipWhenSinks` annotations. Configuration is driven entirely by environment variables: `SNOWFLAKE_ACCOUNT_IDENTIFIER`, `SNOWFLAKE_DB_NAME`, `SNOWFLAKE_SCHEMA_NAME`, `SNOWFLAKE_USERNAME`, and `SNOWFLAKE_PASSWORD` (a Programmatic Access Token).

## `SnowflakeSinkDatabaseContextProvider.java`

A concrete, no-op subclass of `AbstractSinkDatabaseContextProvider`. Exists so that test classes can reference it via `@ExtendWith(SnowflakeSinkDatabaseContextProvider.class)`, following the same pattern used by `MySqlSinkDatabaseContextProvider`, `PostgresSinkDatabaseContextProvider`, etc. in the parent package.