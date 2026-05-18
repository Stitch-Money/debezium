/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.debezium;

import java.time.LocalTime;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.Time;

public class TimeType extends AbstractDebeziumTimeType {

    public static final TimeType INSTANCE = new TimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.SCHEMA_NAME };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int precision = getTimePrecision(schema);
        DatabaseDialect dialect = getDialect();
        if (precision > 0) {
            return String.format("TIME(%d)", Math.min(precision, dialect.getMaxTimePrecision()));
        }

        return String.format("TIME(%d)", dialect.getMaxTimePrecision());
    }

    @Override
    protected LocalTime getLocalTime(Number value) {
        return DateTimeUtils.toLocalTimeFromDurationMilliseconds(value.longValue());
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        String formattedTime = super.getDefaultValueBinding(schema, value);
        return "TO_TIME(" + formattedTime + ")";
    }

}
