/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.hibernate.dialect.snowflake;

class VersionParsingException extends RuntimeException {
    VersionParsingException(String rawVersion, Exception cause) {
        super("Version " + rawVersion + "does not follow {major}.{minor}.{patch} format", cause);
    }

    VersionParsingException(String rawVersion) {
        this(rawVersion, null);
    }
}
