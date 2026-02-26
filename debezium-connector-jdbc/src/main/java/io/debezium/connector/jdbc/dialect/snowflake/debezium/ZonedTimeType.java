/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.debezium;

import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.ZonedTime;

/**
 * An implementation of {@link JdbcType} for {@link ZonedTime} values.
 *
 * @author Marinus Krommenhoek
 */
public class ZonedTimeType extends AbstractTimeType {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZonedTimeType.class);
    public static final ZonedTimeType INSTANCE = new ZonedTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ ZonedTime.SCHEMA_NAME };
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
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDialect().getFormattedTimeWithTimeZone((String) value);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (value instanceof String) {
            final ZonedDateTime zdt = OffsetTime.parse((String) value, ZonedTime.FORMATTER).atDate(LocalDate.now()).toZonedDateTime();
            /* snowflake only supports timestamps at utc, no timezones so we need to modify them */

            return List.of(new ValueBindDescriptor(
                    index,
                    zdt.toOffsetDateTime().atZoneSameInstant(ZoneId.of("UTC")).toLocalTime().toString()// ,
            // getJdbcType()
            ));

        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

    protected int getJdbcType() {
        return Types.TIME_WITH_TIMEZONE;
    }
}
