/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.debezium;

import java.time.LocalDateTime;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.util.DateTimeUtils;
import io.debezium.time.MicroTimestamp;

public class MicroTimestampType extends AbstractDebeziumTimestampType {

    public static final MicroTimestampType INSTANCE = new MicroTimestampType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ MicroTimestamp.SCHEMA_NAME };
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        String formattedTimestamp = getDialect().getFormattedDateTime(DateTimeUtils.toZonedDateTimeFromInstantEpochMicros((long) value));
        String typeName = getTypeName(schema, false);
        // NOTE: isKey is not referenced in the getTypeName method, so we can safely pass false here.
        // NOTE: Snowflake requires the string formatted timestamp to be cast to the type name.
        return formattedTimestamp + "::" + typeName;
    }

    @Override
    protected LocalDateTime getLocalDateTime(long value) {
        return DateTimeUtils.toLocalDateTimeFromInstantEpochMicros(value);
    }

}
