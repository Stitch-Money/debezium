/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.debezium;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.time.Timestamp;

/**
 * An implementation of {@link JdbcType} for {@link Timestamp} values.
 *
 * @author Marinus Krommenhoek
 */
public class TimestampType extends AbstractDebeziumTimestampType {

    public static final TimestampType INSTANCE = new TimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Timestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        String formattedTimestamp = getDialect().getFormattedDateTime(Instant.ofEpochMilli((long) value).atZone(ZoneOffset.UTC));
        return "TO_TIMESTAMP_TZ(" + formattedTimestamp + ")";
    }

    @Override
    protected LocalDateTime getLocalDateTime(long value) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
    }

}
