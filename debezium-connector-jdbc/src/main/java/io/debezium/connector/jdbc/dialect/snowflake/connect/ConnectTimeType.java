/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.connect;

import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.jdbc.type.AbstractTimeType;
import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

public class ConnectTimeType extends AbstractTimeType {

    public static final ConnectTimeType INSTANCE = new ConnectTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Time.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return getDialect().getTimeQueryBinding();
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        String formattedTime = getDialect().getFormattedTime(DateTimeUtils.toZonedDateTimeFromDate((Date) value, getDatabaseTimeZone()));
        return "TO_TIME(" + formattedTime + ")";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        if (value instanceof Date) {

            /* it seems date will be interpreted as at UTC corrected system interpreted timestamp */
            /* this implies we need to get the time into the system reference zone */
            final java.util.Date utcDate = (java.util.Date) value;
            final java.time.ZonedDateTime utcInstant = utcDate.toInstant().atZone(java.time.ZoneId.of("UTC"));
            final java.time.LocalDateTime localDateTime = utcInstant.toLocalDateTime();
            final java.time.LocalTime localTime = localDateTime.toLocalTime();

            return List.of(new ValueBindDescriptor(
                    index,
                    localTime.toString()));
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }

}
