/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.connect;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.type.connect.AbstractConnectSchemaType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

public class ConnectBooleanType extends AbstractConnectSchemaType {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectBooleanType.class);

    public static final ConnectBooleanType INSTANCE = new ConnectBooleanType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "BOOLEAN" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.BOOLEAN);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Byte) {
            /* For primitives debezium tends to wrap them in a Byte */
            LOGGER.info("Boolean Type passed in as Byte.");
            return List.of(new ValueBindDescriptor(index, Boolean.valueOf(value.toString())));
        }
        if (value instanceof Boolean) {
            LOGGER.info("Boolean Type passed in as Boolean.");
            return List.of(new ValueBindDescriptor(index, value));
        }
        return List.of(new ValueBindDescriptor(index, value));
    }
}
