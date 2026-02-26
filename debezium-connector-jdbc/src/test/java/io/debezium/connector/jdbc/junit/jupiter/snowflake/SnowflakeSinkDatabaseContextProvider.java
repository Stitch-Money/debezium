/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.snowflake;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for Snowflake.
 *
 * @author Marinus Krommenhoek
 */
public class SnowflakeSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    @SuppressWarnings("resource")
    public SnowflakeSinkDatabaseContextProvider() {
        super();
    }
}
