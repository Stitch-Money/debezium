/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.hibernate.dialect.snowflake;

/** Definition of supported table types. */
public enum TableType {
    /** Standard table type optimized for OLAP processing. */
    STANDARD(null),
    /**
     * Hybrid table type that is optimized for hybrid transactional and operational workloads that
     * require low latency and high throughput on small random point reads and writes.
     *
     * @see <a href="https://docs.snowflake.com/en/user-guide/tables-hybrid">Snowflake
     *     documentation</a>
     */
    HYBRID("HYBRID");

    private final String tableModifier;

    TableType(String tableModifier) {
        this.tableModifier = tableModifier;
    }

    String createTableStatement() {
        if (tableModifier == null) {
            return "CREATE TABLE";
        }
        else {
            return "CREATE " + tableModifier + " TABLE";
        }
    }
}
