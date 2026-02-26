/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.hibernate.dialect.snowflake;

class JdbcDriverVersionException extends RuntimeException {
    JdbcDriverVersionException(Version actual, Version minimalRecommended) {
        super(
                String.format(
                        "Using driver in version %s must be forced - recommended driver version should be at least %s",
                        actual, minimalRecommended));
    }
}
